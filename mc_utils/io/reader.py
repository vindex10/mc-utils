import gzip
import zlib

from io import BufferedIOBase

try:
    from XRootD import client
    from XRootD.client.flags import OpenFlags
    XROOTD_AVAILABLE = True
except ModuleNotFoundError:
    XROOTD_AVAILABLE = False


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


def iter_file_chunked(path, chunksize=None, gzipped=None):
    chunksize = 1000 if chunksize is None else chunksize

    is_root = path.startswith("root://")
    is_gzip = (path.endswith(".gz") and gzipped is None) or gzipped

    f_iterator = _iter_xrootd if is_root else _iter_file
    raw_event_chunks = f_iterator(path, chunksize, is_gzip)
    yield from raw_event_chunks
