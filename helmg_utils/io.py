import gzip
import logging
import re

import numpy as np

logger = logging.getLogger("helmg_utils.io")


RE_multispace = RE_multispace = re.compile(r"\s+")


def parse_event(fe):
    parsed = []
    events_summary = fe.readline()
    num_particles = int(events_summary.strip().split(" ")[0])
    for _ in range(num_particles):
        line = fe.readline()
        event_line = re.sub(RE_multispace, "\t", line.strip()).split("\t")
        parsed.append(np.array(event_line, dtype=np.float64))
    while True:
        line = fe.readline()
        if "</event>" in line:
            break
    joint = np.vstack(parsed)
    return joint


def lhe_iter(path):
    with gzip.open(path, "rt") as raw_e:
        i = 1
        while True:
            line = raw_e.readline()
            if line == "":
                break
            if "<event" in line:
                if i % 10000 == 0:
                    logger.debug("lhe_iter read %s", i)
                i += 1
                event_parsed = parse_event(raw_e)
                yield event_parsed
