"""Centralized diagnostic logging for streaming_parquet.

All streaming_parquet console and file logging goes through this module.
The file logger always writes to $STREAMING_PARQUET_DIAG_LOG, default
telemetry-diag.log in the working directory (read once, at import).
Console output can be suppressed (e.g. during a Rich Live display) via
suppress_console(True/False).

Verbosity levels:
    0 — normal: milestones only (FIRST COMMIT, WORKER STARTED/STOPPED, errors)
    1 — verbose: + periodic DRAIN stats, subscriber connect/disconnect
    2 — very verbose: + every batch, detailed diagnostics
"""

import logging
import os
import sys
import threading

# --- Shared state (controlled from streaming_parquet/__init__.py) ---
_lock = threading.Lock()
_console_suppressed = False
_verbosity = 0  # 0=normal, 1=verbose, 2=very verbose

# --- File logger (always on) ---
_diag = logging.getLogger("telemetry.diag")
_diag.setLevel(logging.DEBUG)
_diag.propagate = False  # Never bubble up to root logger

_file_handler = logging.FileHandler(
    os.environ.get("STREAMING_PARQUET_DIAG_LOG")
    or os.path.join(os.getcwd(), "telemetry-diag.log"), mode="a"
)
_file_handler.setFormatter(logging.Formatter(
    "%(asctime)s.%(msecs)03d %(message)s", datefmt="%H:%M:%S"
))
_diag.addHandler(_file_handler)

# The checkpoint/mirror layer's timing lines (slow S3 calls, slow
# finalizes) land in the same file. File only: never the console, which
# a curses display may own.
_pc = logging.getLogger("parquet_checkpoint")
_pc.setLevel(logging.INFO)
_pc.propagate = False
_pc.addHandler(_file_handler)

# --- Console handler (toggleable) ---
_console_handler = logging.StreamHandler(sys.stderr)
_console_handler.setFormatter(logging.Formatter(
    "telemetry.diag %(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
))
_diag.addHandler(_console_handler)


def _update_console_handler():
    """Add or remove the console handler based on current state."""
    if _console_suppressed:
        _diag.removeHandler(_console_handler)
    elif _console_handler not in _diag.handlers:
        _diag.addHandler(_console_handler)


def suppress_console(suppress: bool = True):
    """Suppress or resume all streaming_parquet console output.

    When suppressed, file logging continues but nothing prints to the console.
    Use this when a Rich Live display or similar TUI is active.
    """
    global _console_suppressed
    with _lock:
        _console_suppressed = suppress
        _update_console_handler()


def set_verbosity(level: int):
    """Set streaming_parquet diagnostic verbosity.

    0 — normal: milestones only
    1 — verbose: + periodic stats
    2 — very verbose: + every detail, and every S3 call is logged
        with its duration (parquet_checkpoint _s3.trace)
    """
    global _verbosity
    with _lock:
        _verbosity = max(0, min(2, level))
    try:
        from parquet_checkpoint import _s3
        _s3.trace = _verbosity >= 2
    except ImportError:
        pass


def get_verbosity() -> int:
    return _verbosity


def is_console_suppressed() -> bool:
    return _console_suppressed


def console_print(msg: str):
    """Print a streaming_parquet message to the console (unless suppressed).

    For [TELEMETRY] status messages that aren't part of the diag logger.
    Always flushes.
    """
    with _lock:
        if not _console_suppressed:
            print(msg, file=sys.stderr, flush=True)
