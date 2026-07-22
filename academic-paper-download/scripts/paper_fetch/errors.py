from __future__ import annotations

from typing import Any

from .sanitize import sanitize_data, sanitize_text


class PaperFetchError(Exception):
    def __init__(
        self,
        code: str,
        message: str,
        *,
        retryable: bool = False,
        **details: Any,
    ) -> None:
        message = sanitize_text(message)
        super().__init__(message)
        self.code = code
        self.message = message
        self.retryable = retryable
        self.details = sanitize_data(details)

    def as_dict(self) -> dict[str, Any]:
        return sanitize_data({
            "code": self.code,
            "message": self.message,
            "retryable": self.retryable,
            **self.details,
        })
