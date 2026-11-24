"""Logging, the standard way.

The library logs to the logger named "streaming_parquet" (the checkpoint
layer to "parquet_checkpoint", the ring to "arrow_ring"). It attaches no
handler and sets no level: the program decides where the lines go, with
Python's `logging`, as for any library.

    logging.basicConfig(level=logging.INFO)          # the program's own setup
    logging.getLogger("streaming_parquet").setLevel(logging.DEBUG)

With nothing set up, Python shows warnings and errors on standard error
and makes no file.

    sp.log_to_file("telemetry-diag.log")

is for a program that wants the library's lines in a file of its own:
every line at INFO and above from the three loggers, one line each, with
the time. $STREAMING_PARQUET_DIAG_LOG, when set, does the same at import
with the path it names.

Levels: an ERROR is a fault that loses or delays data, a WARNING is a
condition someone should look at, INFO is a milestone (a stream starts,
a file is finished, the catalog is published), DEBUG is detail for a
person who is looking for a fault (with DEBUG on "parquet_checkpoint",
every S3 call is logged with its duration).
"""

import logging
import os
import threading

LOGGER = "streaming_parquet"
#: The loggers log_to_file() writes: the library's three.
LOGGERS = ("streaming_parquet", "parquet_checkpoint", "arrow_ring")

_diag = logging.getLogger(LOGGER)

_lock = threading.Lock()
_files = {}             # absolute path -> the FileHandler writing it

#: The format of a line in a log_to_file() file: the one the library's
#: file has had (the time of day, then the line).
FILE_FORMAT = "%(asctime)s.%(msecs)03d %(message)s"
FILE_DATE_FORMAT = "%H:%M:%S"


def log_to_file(path):
    """Write the library's log lines to the file `path` as well (lines
    are added to a file that exists). Every line at INFO and above; a
    logger the program has given a level of its own keeps it. A second
    call with the same path changes nothing; another path is another
    file."""
    path = os.path.abspath(path)
    with _lock:
        if path in _files:
            return
        handler = logging.FileHandler(path, mode="a")
        handler.setFormatter(logging.Formatter(FILE_FORMAT,
                                               datefmt=FILE_DATE_FORMAT))
        _files[path] = handler
        for name in LOGGERS:
            logger = logging.getLogger(name)
            logger.addHandler(handler)
            if logger.level == logging.NOTSET:
                logger.setLevel(logging.INFO)


if os.environ.get("STREAMING_PARQUET_DIAG_LOG"):
    log_to_file(os.environ["STREAMING_PARQUET_DIAG_LOG"])
