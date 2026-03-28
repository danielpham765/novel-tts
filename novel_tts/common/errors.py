from __future__ import annotations


class RateLimitExceededError(RuntimeError):
    """Raised when a provider keeps returning 429 and we want to release the job back to the queue."""


class InputTranslationError(RuntimeError):
    """Raised when a chapter fails due to input/content issues after translation and repair are exhausted."""
