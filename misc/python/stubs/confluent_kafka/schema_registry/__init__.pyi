# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from typing import Any

class Schema:
    def __init__(
        self,
        schema_str: str,
        schema_type: str = "AVRO",
        references: list[Any] | None = None,
    ) -> None: ...

class SchemaRegistryClient:
    def __init__(self, conf: dict[str, Any]) -> None: ...
    def register_schema(
        self, subject_name: str, schema: Schema, **kwargs: Any
    ) -> int: ...
