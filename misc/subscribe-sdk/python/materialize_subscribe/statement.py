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

"""Building the ``SUBSCRIBE`` statement, including the initial-versus-resume
``SNAPSHOT`` and ``AS OF`` handling that must line up with the resume token."""

from __future__ import annotations

from typing import Iterable, Optional

from .envelope import DiffEnvelope, Envelope, UpsertEnvelope
from .token import ResumeToken

# FNV-1a 64-bit constants. Matches the Rust SDK byte-for-byte so a fingerprint
# (and therefore a resume token) is interchangeable between the two.
_FNV_OFFSET = 0xCBF29CE484222325
_FNV_PRIME = 0x100000001B3
_U64_MASK = 0xFFFFFFFFFFFFFFFF


class Subscribe:
    """A subscription specification: what to read, how to shape it, and where to
    stop.

    ``PROGRESS`` is always requested. It is the primitive the batcher relies on
    to know when a timestamp is closed, and requesting it unconditionally keeps
    the column layout stable.

    Build one with :meth:`object` or :meth:`query`, then chain
    :meth:`envelope_upsert` and :meth:`up_to`.
    """

    def __init__(self, relation_is_query: bool, relation: str) -> None:
        self._relation_is_query = relation_is_query
        self._relation = relation
        self._envelope: Envelope = DiffEnvelope()
        self._up_to: Optional[int] = None

    @classmethod
    def object(cls, name: str) -> "Subscribe":
        """Subscribes to a catalog object (table, view, source, or index).

        ``name`` is passed through verbatim, so a qualified name must already be
        a valid SQL object reference (e.g. ``my_db.public.orders``).
        """
        return cls(relation_is_query=False, relation=name)

    @classmethod
    def query(cls, sql: str) -> "Subscribe":
        """Subscribes to the result of a ``SELECT``."""
        return cls(relation_is_query=True, relation=sql)

    def envelope_upsert(self, key: Iterable[str]) -> "Subscribe":
        """Switches to the upsert envelope keyed by the given columns."""
        self._envelope = UpsertEnvelope(key=list(key))
        return self

    def up_to(self, timestamp: int) -> "Subscribe":
        """Bounds the subscription: it terminates once the frontier reaches
        ``timestamp`` (exclusive), yielding a deterministic window."""
        self._up_to = timestamp
        return self

    @property
    def envelope(self) -> Envelope:
        """The envelope this subscription uses."""
        return self._envelope

    def is_bounded(self) -> bool:
        """Whether the subscription is bounded by an ``UP TO``."""
        return self._up_to is not None

    def fingerprint(self) -> str:
        """A stable identifier for this subscription's shape (relation plus
        envelope), embedded in resume tokens so a changed query is caught on
        resume rather than silently mixing incompatible results."""
        h = _FNV_OFFSET
        if self._relation_is_query:
            h = _fnv_update(h, b"query:")
        else:
            h = _fnv_update(h, b"object:")
        h = _fnv_update(h, self._relation.encode("utf-8"))
        if isinstance(self._envelope, UpsertEnvelope):
            h = _fnv_update(h, b"|upsert:")
            for k in self._envelope.key:
                h = _fnv_update(h, k.encode("utf-8"))
                h = _fnv_update(h, b",")
        else:
            h = _fnv_update(h, b"|diff")
        return f"{h:016x}"

    def to_sql_initial(self) -> str:
        """The SQL for an initial subscription: takes the snapshot and lets the
        server choose the ``AS OF``."""
        return self._to_sql(snapshot=True, as_of=None)

    def to_sql_resume(self, token: ResumeToken) -> str:
        """The SQL for resuming from ``token``: no snapshot, with ``AS OF`` set
        to ``token.as_of()`` so emission begins exactly where it left off."""
        return self.to_sql_resume_at(token.as_of())

    def to_sql_resume_at(self, as_of: int) -> str:
        """The SQL for resuming at an explicit ``as_of``: no snapshot, emitting
        only timestamps strictly greater than ``as_of``. Used by a cohort, where
        every member resumes at the same joint ``AS OF``."""
        return self._to_sql(snapshot=False, as_of=as_of)

    def _to_sql(self, snapshot: bool, as_of: Optional[int]) -> str:
        relation = (
            self._relation if not self._relation_is_query else f"({self._relation})"
        )
        sql = f"SUBSCRIBE {relation}"

        if isinstance(self._envelope, UpsertEnvelope):
            keys = ", ".join(_quote_ident(k) for k in self._envelope.key)
            sql += f" ENVELOPE UPSERT (KEY ({keys}))"

        snapshot_sql = "true" if snapshot else "false"
        sql += f" WITH (PROGRESS, SNAPSHOT = {snapshot_sql})"

        if as_of is not None:
            sql += f" AS OF {as_of}"
        if self._up_to is not None:
            sql += f" UP TO {self._up_to}"
        return sql


def _quote_ident(ident: str) -> str:
    """Double-quotes a SQL identifier, escaping embedded quotes, so a key column
    name cannot break out of the ``KEY (...)`` clause."""
    escaped = ident.replace('"', '""')
    return f'"{escaped}"'


def _fnv_update(hash_: int, data: bytes) -> int:
    for b in data:
        hash_ ^= b
        hash_ = (hash_ * _FNV_PRIME) & _U64_MASK
    return hash_
