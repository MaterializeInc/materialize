# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from typing import Any, Callable, Optional, Type

from pg8000.native import literal

from materialize.data_ingest.data_type import DataType


def identity(x: Any) -> Any:
    return identity


def formatted_value(value: Any) -> str:
    return literal(str(value))


class Field:
    name: str
    data_type: Type[DataType]
    is_key: bool
    # value_fn can be used to encode a value which is stored in this field, for example:
    # import uuid
    # namespace = uuid.uuid4()
    # value_fn = lambda x: uuid.uuid5(namespace, x)
    value_fn: Callable[[Any], Any]

    def __init__(
        self,
        name: str,
        data_type: Type[DataType],
        is_key: bool,
        value_fn: Optional[Callable[[Any], Any]] = None,
    ):
        self.name = name
        self.data_type = data_type
        self.is_key = is_key
        self.value_fn = value_fn or identity

    def __repr__(self) -> str:
        return f"Field({'key' if self.is_key else 'value'}, {self.name}: {self.data_type.__name__})"
