# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# confluent-kafka >= 2.13 uses a metaclass (_BackwardsCompatDecorator) that
# dynamically wraps __init_impl into __init__, hiding the constructor from
# static type checkers. This stub provides the correct signature.

from collections.abc import Callable
from typing import Any

from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.serialization import SerializationContext

class AvroSerializer:
    def __init__(
        self,
        schema_registry_client: SchemaRegistryClient,
        schema_str: str | None = None,
        to_dict: Callable[[Any, SerializationContext], Any] | None = None,
        conf: dict | None = None,
    ) -> None: ...
    def __call__(self, obj: object, ctx: SerializationContext) -> bytes | None: ...
