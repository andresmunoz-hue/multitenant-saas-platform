"""Domain exceptions."""


class CriticalQualityError(RuntimeError):
    """Raised when a critical quality check fails and fail-on-critical is enabled."""
