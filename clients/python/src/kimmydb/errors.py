"""What can go wrong, and what a caller may do about it."""

from __future__ import annotations

from enum import Enum
from typing import Any, Mapping, Optional


class Retry(str, Enum):
    """What a client may do about a failure.

    Three-valued rather than a boolean because KimmyDB is leaderless: every
    node accepts writes, so "ask a different node" is a real answer and the
    right one for a failure local to the node that answered.

    A `str` enum so that ``event["retry"] == Retry.NO`` works and printing one
    shows the wire value rather than ``Retry.NO``.
    """

    NO = "no"
    WAIT = "wait"
    ELSEWHERE = "elsewhere"

    @classmethod
    def parse(cls, value: Optional[str]) -> "Retry":
        try:
            return cls(value)
        except ValueError:
            # An unknown class is read as `no`, which is the safe direction: a
            # client that does not understand the advice does not act on it.
            return cls.NO


class KimmyError(Exception):
    """The server refused, and said why in the envelope every route uses.

    ``code`` is a plain string rather than an enum on purpose. Codes are
    additive — a server newer than this client will use ones it has never heard
    of — and an enum would turn "a code I do not recognize" into an error in
    itself. Branch on :attr:`retry` when the code is unfamiliar; that is what
    it is there for.
    """

    def __init__(
        self,
        status: int,
        code: str,
        message: str,
        retry: Retry = Retry.NO,
        retry_after: Optional[int] = None,
    ) -> None:
        super().__init__(f"{status} {code}: {message}")
        self.status = status
        self.code = code
        self.message = message
        self.retry = retry
        self.retry_after = retry_after

    @property
    def is_unauthorized(self) -> bool:
        return self.code == "unauthorized"

    @property
    def is_not_found(self) -> bool:
        return self.code == "not_found"

    @classmethod
    def from_response(
        cls,
        status: int,
        body: Any,
        retry_after: Optional[int] = None,
    ) -> "KimmyError":
        """Build one from a refusal.

        A body that is *not* the envelope — a proxy's HTML error page, say —
        still produces an error carrying the status, because the status is the
        part that came from HTTP and is worth keeping.
        """
        envelope: Mapping[str, Any] = body if isinstance(body, Mapping) else {}
        retry = envelope.get("retry")
        if retry is None:
            # A server older than the retry class. Guessing from the status is
            # worse advice than the server's own and better than none.
            retry = "elsewhere" if status >= 500 else "wait" if status == 429 else "no"
        return cls(
            status=status,
            code=str(envelope.get("error", "unknown")),
            message=str(envelope.get("message", "(no message)")),
            retry=Retry.parse(retry),
            retry_after=retry_after,
        )


class TransportError(Exception):
    """The request never got an answer: refused, timed out, TLS.

    Carries the endpoint, because with failover the interesting question is
    *which* node failed rather than that one did.
    """

    def __init__(self, endpoint: str, cause: BaseException) -> None:
        super().__init__(f"could not reach {endpoint}: {cause}")
        self.endpoint = endpoint
        self.cause = cause
        # A node that did not answer is exactly the case "ask another one"
        # exists for.
        self.retry = Retry.ELSEWHERE


class NoNodeAvailable(Exception):
    """Every endpoint was tried and none answered."""

    def __init__(self, tried: list) -> None:
        super().__init__("no node answered; tried " + ", ".join(tried))
        self.tried = tried


class ProtocolError(Exception):
    """The server answered with something that is not the shape promised."""
