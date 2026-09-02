"""Reserved home for implemented neutral runtime models.

Phase 1 deliberately defines no engine/order models beyond the public enums
and :class:`hftbacktest_slim.AssetConfig`; exporting speculative models here
would create an accidental contract before the runtime is migrated.
"""

__all__: tuple[str, ...] = ()
