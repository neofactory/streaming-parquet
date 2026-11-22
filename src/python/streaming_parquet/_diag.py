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

suppress_console() and set_verbosity() are old names (docs/deprecations.md).
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
_suppressed = None      # the NullHandler of suppress_console(True)

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


def suppress_console(suppress: bool = True):
    """An old name. While suppressed, the library's lines go to its own
    handlers only (a log_to_file() file): not to the program's handlers
    and not to standard error. The standard way: put a handler of your
    own on the logger "streaming_parquet" and set its `propagate` to
    False."""
    from ._deprecations import old_name
    old_name("suppress_console")
    global _suppressed
    with _lock:
        if suppress and _suppressed is None:
            _suppressed = logging.NullHandler()
            for name in LOGGERS:
                logger = logging.getLogger(name)
                logger.addHandler(_suppressed)
                logger.propagate = False
        elif not suppress and _suppressed is not None:
            for name in LOGGERS:
                logger = logging.getLogger(name)
                logger.removeHandler(_suppressed)
                logger.propagate = True
            _suppressed = None


def set_verbosity(level: int):
    """An old name. 0: INFO and above; 1: DEBUG too; 2: DEBUG also for
    the checkpoint layer (every S3 call with its duration). The
    standard way: logging.getLogger("streaming_parquet").setLevel(...)."""
    from ._deprecations import old_name
    old_name("set_verbosity")
    logging.getLogger(LOGGER).setLevel(
        logging.DEBUG if level >= 1 else logging.INFO)
    logging.getLogger("parquet_checkpoint").setLevel(
        logging.DEBUG if level >= 2 else logging.INFO)


if os.environ.get("STREAMING_PARQUET_DIAG_LOG"):
    log_to_file(os.environ["STREAMING_PARQUET_DIAG_LOG"])
