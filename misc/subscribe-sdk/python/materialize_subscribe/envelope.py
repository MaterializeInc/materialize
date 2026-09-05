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

"""Turning raw ``SUBSCRIBE`` rows into typed changes and progress markers.

Decoding is by column *name*, not position, because the column layout differs
between envelopes (the upsert envelope interleaves key, ``mz_state``, and value
columns) and because it makes the decoder robust to the exact ordering the
server chooses.

The decoder accepts either the pgwire text encoding (``"t"``/``"f"``, numeric
strings) or the native Python values that psycopg produces (``bool``, ``int``,
``Decimal``); the metadata columns are coerced either way. Payload and key
values are passed through unchanged.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, List, Optional, Sequence, Union

from .errors import ProtocolError

#: A row of column values. ``None`` is SQL ``NULL``.
Row = List[Any]


@dataclass(frozen=True)
class DiffEnvelope:
    """Raw differential updates: each change carries a signed multiplicity."""


@dataclass(frozen=True)
class UpsertEnvelope:
    """Server-side upsert compaction keyed by the given columns."""

    key: Sequence[str]


#: How the server shapes the update stream. Fixed for a subscription's lifetime.
Envelope = Union[DiffEnvelope, UpsertEnvelope]


@dataclass(frozen=True)
class Diff:
    """A differential update.

    ``diff`` is the signed multiplicity: positive means ``diff`` copies were
    inserted, negative means ``|diff|`` were retracted. The magnitude is
    preserved rather than collapsed to +/-1.
    """

    row: Row
    diff: int


@dataclass(frozen=True)
class Upsert:
    """An upsert-envelope insert or update: ``value`` is the new row for ``key``."""

    key: Row
    value: Row


@dataclass(frozen=True)
class Delete:
    """An upsert-envelope delete: ``key`` no longer has a value."""

    key: Row


@dataclass(frozen=True)
class KeyViolation:
    """An upsert-envelope key violation (best-effort per the server)."""

    key: Row


#: A decoded change to the subscribed relation.
Change = Union[Diff, Upsert, Delete, KeyViolation]


@dataclass(frozen=True)
class Data:
    """A change occurring at ``timestamp``."""

    timestamp: int
    change: Change


@dataclass(frozen=True)
class Progress:
    """A progress marker: no more changes below ``frontier`` will arrive."""

    frontier: int


#: A decoded message from the stream.
StreamMessage = Union[Data, Progress]

_COL_TIMESTAMP = "mz_timestamp"
_COL_PROGRESSED = "mz_progressed"
_COL_DIFF = "mz_diff"
_COL_STATE = "mz_state"

_STATE_UPSERT = "upsert"
_STATE_DELETE = "delete"
_STATE_KEY_VIOLATION = "key_violation"


class Decoder:
    """Decodes raw ``SUBSCRIBE`` rows into :data:`StreamMessage` values.

    Built once from the result's column names (the SDK always subscribes
    ``WITH (PROGRESS)``), then reused for every row.
    """

    def __init__(self, columns: Sequence[str], envelope: Envelope) -> None:
        self._columns = list(columns)

        self._timestamp_idx = self._require(_COL_TIMESTAMP)
        self._progressed_idx = self._require(_COL_PROGRESSED)

        if isinstance(envelope, UpsertEnvelope):
            self._state_idx: Optional[int] = self._require(_COL_STATE)
            self._key_idxs = [self._require_key(k) for k in envelope.key]
            meta = {self._timestamp_idx, self._progressed_idx, self._state_idx}
            meta.update(self._key_idxs)
            self._value_idxs = [i for i in range(len(self._columns)) if i not in meta]
            self._diff_idx: Optional[int] = None
        else:
            self._diff_idx = self._require(_COL_DIFF)
            self._state_idx = None
            self._key_idxs = []
            meta = {self._timestamp_idx, self._progressed_idx, self._diff_idx}
            self._payload_idxs = [i for i in range(len(self._columns)) if i not in meta]

    def _require(self, name: str) -> int:
        try:
            return self._columns.index(name)
        except ValueError:
            raise ProtocolError(f"result is missing the {name} column") from None

    def _require_key(self, name: str) -> int:
        try:
            return self._columns.index(name)
        except ValueError:
            raise ProtocolError(
                f"upsert key column {name} is not in the result"
            ) from None

    def decode(self, row: Sequence[Any]) -> StreamMessage:
        """Decodes a single raw row."""
        timestamp = _to_int(row[self._timestamp_idx], _COL_TIMESTAMP)

        if _to_bool(row[self._progressed_idx]):
            return Progress(frontier=timestamp)

        if self._state_idx is not None:
            key = [row[i] for i in self._key_idxs]
            state = row[self._state_idx]
            state = str(state) if state is not None else None
            if state == _STATE_UPSERT:
                change: Change = Upsert(
                    key=key, value=[row[i] for i in self._value_idxs]
                )
            elif state == _STATE_DELETE:
                change = Delete(key=key)
            elif state == _STATE_KEY_VIOLATION:
                change = KeyViolation(key=key)
            else:
                raise ProtocolError(f"unexpected mz_state value: {state!r}")
        else:
            assert self._diff_idx is not None
            change = Diff(
                row=[row[i] for i in self._payload_idxs],
                diff=_to_int(row[self._diff_idx], _COL_DIFF),
            )

        return Data(timestamp=timestamp, change=change)


def _to_int(value: Any, column: str) -> int:
    """Coerces a metadata cell to ``int``, accepting text or native numbers."""
    if value is None:
        raise ProtocolError(f"{column} was NULL")
    try:
        return int(value)
    except (ValueError, TypeError) as exc:
        raise ProtocolError(f"{column} {value!r} is not an integer: {exc}") from exc


def _to_bool(value: Any) -> bool:
    """Coerces ``mz_progressed`` to ``bool``, accepting text or native values."""
    if value is None:
        return False
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        if value == "t":
            return True
        if value == "f":
            return False
        raise ProtocolError(f"unexpected mz_progressed value: {value!r}")
    return bool(value)
