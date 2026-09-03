"""Package-owned compact market-data contracts and transformations."""

from .audit import compact_partition_audit
from .normalize import normalized_bbo_from_depth_columns
from .schema import (
    BBO_SCHEMA,
    COMPACT_SCHEMA_VERSION,
    PROJECTED_COLUMNS,
    SLIM_ROW_DTYPE,
)

__all__ = (
    "BBO_SCHEMA",
    "COMPACT_SCHEMA_VERSION",
    "PROJECTED_COLUMNS",
    "SLIM_ROW_DTYPE",
    "compact_partition_audit",
    "normalized_bbo_from_depth_columns",
)
