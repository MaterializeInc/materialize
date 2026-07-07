# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License in the LICENSE file at the
# root of this repository, or online at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""The opaque checkpoint that makes resuming gap-free."""

from __future__ import annotations

import base64
import json
from dataclasses import dataclass

from .errors import InvalidToken

# Current on-the-wire token format. Bumped only if the encoded shape changes, so
# a stored token from an older SDK is rejected with a clear error rather than
# misinterpreted. Matches the Rust SDK so tokens are cross-compatible.
_TOKEN_FORMAT = 1


@dataclass(frozen=True)
class ResumeToken:
    """An opaque, serializable checkpoint marking how far a subscription has
    been durably consumed.

    A token records a *closed* frontier: every update with a timestamp strictly
    below :attr:`frontier` has been delivered. Resuming re-subscribes with
    ``SNAPSHOT = false AS OF frontier - 1``, which the server reads as "emit
    updates with timestamp strictly greater than ``frontier - 1``", i.e.
    timestamp ``>= frontier``. That is exactly the set not yet delivered, so the
    resume is gap-free and overlap-free.

    The ``- 1`` is the single most error-prone part of the subscribe protocol.
    It lives here, in :meth:`as_of`, so callers never compute it.
    """

    frontier: int
    """The closed frontier: every update below this timestamp is delivered."""

    fingerprint: str
    """The query fingerprint this token was taken against."""

    def as_of(self) -> int:
        """The ``AS OF`` value for a gap-free ``SNAPSHOT = false`` resume.

        Saturates at zero so a token for the very first frontier resumes from
        the beginning rather than going negative.
        """
        return max(0, self.frontier - 1)

    def encode(self) -> str:
        """Encodes the token to a compact, URL-safe string for durable storage.

        The encoding is opaque: callers persist and return the string unchanged.
        """
        payload = {
            "format": _TOKEN_FORMAT,
            "frontier": self.frontier,
            "fingerprint": self.fingerprint,
        }
        raw = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")

    @classmethod
    def decode(cls, encoded: str) -> ResumeToken:
        """Decodes a token previously produced by :meth:`encode`."""
        try:
            padded = encoded + "=" * (-len(encoded) % 4)
            raw = base64.urlsafe_b64decode(padded.encode("ascii"))
            payload = json.loads(raw)
        except (ValueError, TypeError) as exc:
            raise InvalidToken(f"malformed token: {exc}") from exc

        if not isinstance(payload, dict) or payload.get("format") != _TOKEN_FORMAT:
            raise InvalidToken(
                f"unsupported token format {payload.get('format') if isinstance(payload, dict) else '?'} "
                f"(this SDK understands {_TOKEN_FORMAT})"
            )
        try:
            return cls(
                frontier=int(payload["frontier"]),
                fingerprint=str(payload["fingerprint"]),
            )
        except (KeyError, ValueError, TypeError) as exc:
            raise InvalidToken(f"malformed token: {exc}") from exc
