import logging
from io import StringIO
from itertools import chain

import numpy as np

from mc_utils.io.reader import iter_file_chunked

logger = logging.getLogger("mc_utils.io.tsv_io")

HEADER_MARKER = b"#"
NEWLINE_MARKER = b"\n"


def _skip_preamble(chunks_iter):
    events_started = False
    logger.debug("Start reading file")
    for chunk in chunks_iter:
        if not events_started:
            logger.debug("Skiping preamble")
            events_loc = chunk.rfind(HEADER_MARKER)
            if events_loc == -1:
                continue
            events_loc = chunk.find(NEWLINE_MARKER, events_loc)
            events_started = True
            chunk = chunk[events_loc+1:]
        logger.debug("Yield raw chunk")
        yield chunk


def _parse_batch(buf, batch):
    full_batch_idx = batch.rfind(NEWLINE_MARKER)
    if buf:
        batch = buf + batch
        full_batch_idx = full_batch_idx + len(buf) if full_batch_idx != -1 else -1
    if full_batch_idx == -1:
        return None, batch
    newbuf = batch[full_batch_idx+1:]
    batch = StringIO(batch[:full_batch_idx+1].decode("ascii"))
    return np.genfromtxt(batch, dtype=np.complex), newbuf


def _parse_event_batch(buf, event_batch, event_postprocess=None):
    logger.debug("Start parsing event batch")
    arr, buf = _parse_batch(buf, event_batch)
    if arr is None:
        return None, buf
    if event_postprocess is not None:
        arr = event_postprocess(arr)
    logger.debug("Built array. Return.")
    return arr, buf


def tsv_iter_files(paths, chunksize=None, gzipped=None, event_postprocess=None):
    if not isinstance(paths, list):
        yield from tsv_iter_files([paths], chunksize, gzipped, event_postprocess)
        return
    raw_event_chunks = (_skip_preamble(iter_file_chunked(path, chunksize, gzipped)) for path in paths)
    buf = b""
    for raw_event_chunk in chain.from_iterable(raw_event_chunks):
        logger.debug("Got chunk. Start parsing batch")
        arr, buf = _parse_event_batch(buf, raw_event_chunk, event_postprocess)
        if arr is None:
            continue
        yield arr
        logger.debug("Yielded array. Fetch next raw chunk")
