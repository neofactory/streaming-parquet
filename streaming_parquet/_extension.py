"""The parquet_checkpoint extension: DuckDB's own parquet scan over an
unfinished file, through the pqc:// paths the ticket lists.

    con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    con.sql("SELECT ... FROM read_parquet('pqc://<store>/streams/<name>/<file>.parquet.ckpt')")

DuckDB loads an extension it did not sign only on a connection opened
with allow_unsigned_extensions (the command line: duckdb -unsigned); the
setting cannot be turned on for a connection already open. Until DuckDB
signs the extension (a community extension), every connection a view is
given opens that way. load() says so when it does not.

The extension is the build in this checkout's duckdb-checkpoint/
(see its README); a build serves one exact DuckDB version.
"""

import os

import duckdb

SOURCE = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "duckdb-checkpoint")
PATH = os.path.join(SOURCE, "build", "release", "extension",
                    "parquet_checkpoint", "parquet_checkpoint.duckdb_extension")


def _one(con, sql):
    """The single row of `sql`, read to the end: DuckDB 2.0 refuses
    con.sql() and register() while an execute() result is unread."""
    rows = con.execute(sql).fetchall()
    return rows[0] if rows else None


def load(con, who):
    """Load the parquet_checkpoint extension into `con`, once. `who`
    names the caller for the error: e.g. "duckdb_all('probe/voltages')"."""
    got = _one(con, "SELECT loaded FROM duckdb_extensions() "
                    "WHERE extension_name = 'parquet_checkpoint'")
    if got is not None and got[0]:
        return
    if not _one(con, "SELECT current_setting('allow_unsigned_extensions')")[0]:
        raise RuntimeError(
            f"{who}: the DuckDB connection was opened without "
            f"allow_unsigned_extensions, so DuckDB refuses the "
            f"parquet_checkpoint extension that reads unfinished files "
            f"(DuckDB has not signed it). Open the connection with "
            f"duckdb.connect(config={{\"allow_unsigned_extensions\": \"true\"}}) "
            f"(the command line: duckdb -unsigned); the setting cannot be "
            f"turned on for a connection already open.")
    if not os.path.exists(PATH):
        raise RuntimeError(
            f"{who}: the parquet_checkpoint extension that reads unfinished "
            f"files is not built: no {PATH}. Build it as {SOURCE}/README.md "
            f"says, for DuckDB {duckdb.__version__}.")
    try:
        con.execute(f"LOAD '{PATH}'")
    except duckdb.Error as e:
        raise RuntimeError(
            f"{who}: DuckDB {duckdb.__version__} could not load the "
            f"parquet_checkpoint extension {PATH}: {e}") from e
