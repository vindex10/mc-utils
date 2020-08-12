import gzip
import logging
import re
import zlib

from mc_utils.fastlhe import parse_batch

try:
    from XRootD import client
    from XRootD.client.flags import OpenFlags
    XROOTD_AVAILABLE = True
except ModuleNotFoundError:
    XROOTD_AVAILABLE = False

logger = logging.getLogger("mc_utils.lhe_io")

NUM_PROCESSES = 6

EVENT_START_MARKER = b"<event"
EVENT_END_MARKER = b"</event>\n"
EVENT_END_MARKER_LEN = len(EVENT_END_MARKER)

RE_multispace = re.compile(br"\s+")


def _iter_file(path, chunksize, gzipped):
    opener = open if not gzipped else gzip.open
    with opener(path, "rb") as raw_e:
        while True:
            chunk = raw_e.read(chunksize)
            if chunk == b"":
                break
            yield chunk


def _iter_xrootd(filepath, chunksize, gzipped):
    with client.File() as f:
        f.open(filepath, OpenFlags.READ)
        if gzipped:
            dec = zlib.decompressobj(32 + zlib.MAX_WBITS)
            for chunk in f.readchunks(offset=0, chunksize=chunksize):
                yield dec.decompress(chunk)
        else:
            for chunk in f.readchunks(offset=0, chunksize=chunksize):
                yield chunk


def _skip_preamble(chunks_iter):
    events_started = False
    logger.debug("Start reading file")
    for chunk in chunks_iter:
        if not events_started:
            logger.debug("Skiping preamble")
            events_loc = chunk.find(EVENT_START_MARKER)
            if events_loc == -1:
                continue
            events_started = True
            chunk = chunk[events_loc:]
        logger.debug("Yield raw chunk")
        yield chunk


def _parse_event_batch(buf, event_batch, event_postprocess=None):
    logger.debug("Start parsing event batch")
    arr, buf = parse_batch(5, buf, event_batch)
    if event_postprocess is not None:
        arr = event_postprocess(arr)
    logger.debug("Built array. Return.")
    return arr, buf


def lhe_iter_file(path, chunksize=100000000, gzipped=None, event_postprocess=None):
    is_root = path.startswith("root://")
    is_gzip = (path.endswith(".gz") and gzipped is None) or gzipped

    f_iterator = _iter_xrootd if is_root else _iter_file
    raw_event_chunks = _skip_preamble(f_iterator(path, chunksize, is_gzip))
    buf = ""
    for raw_event_chunk in raw_event_chunks:
        logger.debug("Got chunk. Start parsing batch")
        arr, buf = _parse_event_batch(buf, raw_event_chunk, event_postprocess)
        yield arr
        logger.debug("Yielded array. Fetch next raw chunk")
