"""Foundational exception hierarchy for the slim package boundary."""


class SlimError(Exception):
    """Base class for errors raised by the shared slim runtime."""


class SlimConfigurationError(SlimError, ValueError):
    """A neutral slim configuration is invalid."""


class UnsupportedCapabilityError(SlimError):
    """The requested behavior is outside the supported slim profile."""


class NativeLibraryError(SlimError):
    """Base class for native-library loading or compatibility failures."""


class NativeLibraryNotFoundError(NativeLibraryError, FileNotFoundError):
    """The slim native library could not be found."""


class AbiMismatchError(NativeLibraryError):
    """The loaded native library does not expose the required ABI version."""


__all__ = (
    "AbiMismatchError",
    "NativeLibraryError",
    "NativeLibraryNotFoundError",
    "SlimConfigurationError",
    "SlimError",
    "UnsupportedCapabilityError",
)
