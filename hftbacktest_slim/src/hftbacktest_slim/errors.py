"""Foundational exception hierarchy for the slim package boundary."""


class SlimError(Exception):
    """Base class for errors raised by the shared slim runtime."""


class SlimConfigurationError(SlimError, ValueError):
    """A neutral slim configuration is invalid."""


class UnsupportedCapabilityError(SlimError):
    """The requested behavior is outside the supported slim profile."""


class ArrowDataError(SlimError, ValueError):
    """A compact Arrow partition cannot satisfy the native row contract."""


class CompactCacheError(SlimError, RuntimeError):
    """A compact cache cannot be built, validated, or reused safely."""


class CompactCacheBudgetError(CompactCacheError):
    """A configured compact-cache size or free-space limit was crossed."""


class EngineClosedError(SlimError):
    """An operation was attempted after the engine handle was closed."""


class NativeCallError(SlimError, RuntimeError):
    """A native operation returned an unexpected failure code."""


class OrderSubmissionError(NativeCallError):
    """An immediate order could not be submitted."""


class NativeLibraryError(SlimError):
    """Base class for native-library loading or compatibility failures."""


class NativeLibraryNotFoundError(NativeLibraryError, FileNotFoundError):
    """The slim native library could not be found."""


class AbiMismatchError(NativeLibraryError):
    """The loaded native library does not expose the required ABI version."""


__all__ = (
    "AbiMismatchError",
    "ArrowDataError",
    "CompactCacheBudgetError",
    "CompactCacheError",
    "EngineClosedError",
    "NativeLibraryError",
    "NativeLibraryNotFoundError",
    "NativeCallError",
    "OrderSubmissionError",
    "SlimConfigurationError",
    "SlimError",
    "UnsupportedCapabilityError",
)
