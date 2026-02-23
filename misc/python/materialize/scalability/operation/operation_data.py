# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from typing import Any

from psycopg import Cursor


class OperationData:
    def __init__(self, cursor: Cursor | None, worker_id: int):
        self._data: dict[str, Any] = dict()
        self._data["cursor"] = cursor
        self._data["worker_id"] = worker_id

    def cursor(self) -> Cursor:
        cursor = self._data["cursor"]
        if cursor is None:
            raise RuntimeError("Cursor is not set")
        return cursor

    def worker_id(self) -> Cursor:
        return self._data["worker_id"]

    def push(self, key: str, value: Any) -> None:
        self._data[key] = value

    def remove(self, key: str) -> None:
        self._data.pop(key, None)

    def get(self, key: str) -> Any:
        if key not in self._data.keys():
            raise RuntimeError(f"Key does not exist: {key}")

        return self._data[key]

    def validate_requirements(
        self, expected_keys: set[str], required_by: type[Any], requirement: str
    ) -> None:
        for key in expected_keys:
            if key not in self._data.keys():
                raise RuntimeError(
                    f"{required_by.__name__} {requirement} '{key}' but got only: {self._data.keys()}"
                )
