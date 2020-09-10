import logging
import re

from itertools import chain

from mc_utils.fastlhe import parse_batch
from mc_utils.io.reader import iter_file_chunked

logger = logging.getLogger("mc_utils.io.lhe_io")

EVENT_START_MARKER = b"<event"
EVENT_END_MARKER = b"</event>\n"
EVENT_END_MARKER_LEN = len(EVENT_END_MARKER)

RE_multispace = re.compile(br"\s+")


def _parse_event_batch(buf, event_batch, event_postprocess=None):
    logger.debug("Start parsing event batch")
    arr, buf = parse_batch(5, buf, event_batch)
    if event_postprocess is not None:
        arr = event_postprocess(arr)
    logger.debug("Built array. Return.")
    return arr, buf


def lhe_iter_files(paths, chunksize=None, gzipped=None, event_postprocess=None):
    if not isinstance(paths, list):
        yield from lhe_iter_files([paths], chunksize, gzipped, event_postprocess)
        return
    raw_event_chunks = (iter_file_chunked(path, chunksize, gzipped) for path in paths)
    buf = ""
    for raw_event_chunk in chain.from_iterable(raw_event_chunks):
        logger.debug("Got chunk. Start parsing batch")
        arr, buf = _parse_event_batch(buf, raw_event_chunk, event_postprocess)
        yield arr
        logger.debug("Yielded array. Fetch next raw chunk")
