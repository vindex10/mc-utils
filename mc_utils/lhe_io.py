import gzip
import io
import logging
import re
import zlib

from multiprocessing.dummy import Pool

try:
    from XRootD import client
    from XRootD.client.flags import OpenFlags
    XROOTD_AVAILABLE = True
except ModuleNotFoundError:
    XROOTD_AVAILABLE = False

import numpy as np

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


def _complete_event_chunks(raw_event_chunks):
    buf = b""
    for chunk in raw_event_chunks:
        logger.debug("Searching for END marker")
        last_event = chunk.rfind(EVENT_END_MARKER)
        if last_event != -1:
            logger.debug("Has END marker")
            newbuf, chunk = chunk[last_event+EVENT_END_MARKER_LEN:], chunk[:last_event+EVENT_END_MARKER_LEN]
            if buf:
                logger.debug("Prepend buf end yield")
                yield buf + chunk
            else:
                logger.debug("No-Prepend buf end yield")
                yield chunk
            buf = newbuf
            continue
        logger.debug("No END marker. Yield buf")
        yield buf
        buf = chunk


def _read_event(stream):
    event = []
    while True:
        line = stream.readline()
        if not line:
            break
        event.append(line)
        if EVENT_END_MARKER not in line:
            continue
        yield event
        event = []


def _parse_event(event):
    parsed = []

    events_summary_fields = RE_multispace.split(event[1].strip())
    num_particles = int(events_summary_fields[0])
    event_weight = float(events_summary_fields[2])
    
    for line in event[2:2+num_particles]:
        event_line = re.sub(RE_multispace, b"\t", line.strip()).split(b"\t")
        event_line.append(event_weight)
        parsed.append(event_line)
    return np.array(parsed, dtype=np.float64)


def _parse_event_batch(event_batch, pool, event_postprocess=None):
    logger.debug("Start parsing event batch")
    eventstream = io.BytesIO(event_batch)
    logger.debug("Define bytestream")
    event_parser = _parse_event
    if event_postprocess is not None:
        event_parser = lambda eb: event_postprocess(_parse_event(eb))
    res = pool.map(event_parser, _read_event(eventstream))
    logger.debug("Got list of parsed event batch. Build array and return.")
    return np.array(res, dtype=np.float64)


def lhe_iter_file(path, chunksize=100000000, gzipped=None, event_postprocess=None):
    is_root = path.startswith("root://")
    is_gzip = (path.endswith(".gz") and gzipped is None) or gzipped

    f_iterator = _iter_xrootd if is_root else _iter_file
    raw_event_chunks = _skip_preamble(f_iterator(path, chunksize, is_gzip))
    complete_event_chunks = _complete_event_chunks(raw_event_chunks)
    with Pool(NUM_PROCESSES) as p:
        logger.debug("Start iterating complete chunks")
        for complete_event_chunk in complete_event_chunks:
            logger.debug("Got complete chunk. Start parsing batch")
            yield _parse_event_batch(complete_event_chunk, p, event_postprocess)
            logger.debug("Yielded array. Fetch next raw chunk")
