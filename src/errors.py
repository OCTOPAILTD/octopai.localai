class ParserServiceError(Exception):
    """Base class for parser service failures."""


class UpstreamModelError(ParserServiceError):
    """Raised when model endpoint calls fail."""


class ValidationFailure(ParserServiceError):
    """Raised when SQL validation fails in strict mode."""

