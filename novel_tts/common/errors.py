from __future__ import annotations


class RateLimitExceededError(RuntimeError):
    """Raised when a provider keeps returning 429 and we want to release the job back to the queue."""

