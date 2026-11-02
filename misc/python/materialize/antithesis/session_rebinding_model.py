# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Input-derived bindings and begun cursor snapshots for independent sessions."""

from __future__ import annotations

from dataclasses import dataclass, field

Row = tuple[int, int | None, str]


@dataclass(frozen=True)
class Parameters:
    factor: int
    offset: int
    descending: bool

    def project(self, rows: tuple[Row, ...]) -> tuple[Row, ...]:
        projected = (
            (key, None if value is None else value * self.factor + self.offset, tag)
            for key, value, tag in rows
        )
        return tuple(sorted(projected, key=lambda row: row[0], reverse=self.descending))


@dataclass
class Cursor:
    rows: tuple[Row, ...]
    offset: int = 0

    def fetch(self, count: int | None) -> tuple[Row, ...]:
        end = (
            len(self.rows)
            if count is None
            else min(len(self.rows), self.offset + count)
        )
        if count is not None and count < 0:
            raise ValueError("fetch count must be nonnegative")
        result = self.rows[self.offset : end]
        self.offset = end
        return result


@dataclass
class Model:
    inputs: tuple[tuple[Row, ...], tuple[Row, ...]]
    durable: int = 0
    temporary: dict[str, int] = field(default_factory=dict)
    cursors: dict[str, Cursor] = field(default_factory=dict)

    def rows(
        self, session: str, parameters: Parameters, *, qualified: bool = False
    ) -> tuple[Row, ...]:
        binding = (
            self.durable if qualified else self.temporary.get(session, self.durable)
        )
        return parameters.project(self.inputs[binding])

    def replace(self, binding: int) -> None:
        if binding not in (0, 1):
            raise ValueError("unknown generated binding")
        self.durable = binding

    def create_temp(self, session: str, binding: int) -> None:
        if binding not in (0, 1) or session in self.temporary:
            raise ValueError("invalid temporary binding transition")
        self.temporary[session] = binding

    def drop_temp(self, session: str) -> None:
        del self.temporary[session]

    def begin_cursor(self, session: str, parameters: Parameters) -> None:
        if session in self.cursors:
            raise ValueError("cursor already exists in this transaction")
        self.cursors[session] = Cursor(self.rows(session, parameters, qualified=True))

    def fetch(self, session: str, count: int | None) -> tuple[Row, ...]:
        return self.cursors[session].fetch(count)

    def commit(self, session: str) -> None:
        self.cursors.pop(session, None)
