"""arrow_ring -- a recycling builder over ring storage; frozen regions
are zero-copy Arrow record batches.

Public API (the README is the contract):

    Ring(schema, capacity, rate_hint=None)      # schema: pyarrow.Schema
    ring.get_mutable_row(block_s=0.0) -> MutableRow
    ring.commit() -> int
    ring.read_row(seq) -> Row (MutableRow is its writable subclass)
    ring.get_static_relation(duration_s) -> RecordSet
    ring.get_dynamic_relation(ttl_s=..., lease_s=...) -> DynamicRelation
    ring.oldest_seq / ring.newest_seq
    RecordSet.table / .lo / .hi / .release() / context manager
    StaleReadError, PinStalledError
"""

from ._binding import (Ring, RecordSet, DynamicRelation,
                       StaleReadError, PinStalledError)

__all__ = ['Ring', 'RecordSet', 'DynamicRelation',
           'StaleReadError', 'PinStalledError']
