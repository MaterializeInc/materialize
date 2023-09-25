# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import random
from typing import Any


class DataType:
    @staticmethod
    def name() -> str:
        raise NotImplementedError

    @staticmethod
    def value(rng: random.Random, in_query: bool = False) -> Any:
        raise NotImplementedError


class Boolean(DataType):
    @staticmethod
    def name() -> str:
        return "boolean"

    @staticmethod
    def value(rng: random.Random, in_query: bool = False) -> Any:
        return rng.choice(["TRUE", "FALSE"])


class Smallint(DataType):
    @staticmethod
    def name() -> str:
        return "smallint"

    @staticmethod
    def value(rng: random.Random, in_query: bool = False) -> Any:
        if rng.randrange(10) == 0:
            return -32768
        if rng.randrange(10) == 0:
            return 32767
        return rng.randint(-32768, 32767)


class Int(DataType):
    @staticmethod
    def name() -> str:
        return "int"

    @staticmethod
    def value(rng: random.Random, in_query: bool = False) -> Any:
        if rng.randrange(10) == 0:
            return -2147483648
        if rng.randrange(10) == 0:
            return 2147483647
        return rng.randint(-2147483648, 2147483647)


class Bigint(DataType):
    @staticmethod
    def name() -> str:
        return "bigint"

    @staticmethod
    def value(rng: random.Random, in_query: bool = False) -> Any:
        if rng.randrange(10) == 0:
            return -9223372036854775808
        if rng.randrange(10) == 0:
            return 9223372036854775807
        return rng.randint(-9223372036854775808, 9223372036854775807)


class Text(DataType):
    @staticmethod
    def name() -> str:
        return "text"

    @staticmethod
    def value(rng: random.Random, in_query: bool = False) -> Any:
        result = rng.choice(
            [
                # "NULL", # TODO: Reenable after #21937 is fixed
                "0.0",
                "True",
                # "",
                "表ポあA鷗ŒéＢ逍Üßªąñ丂㐀𠀀",
                rng.randint(-100, 100),
            ]
        )
        return f"'{result}'" if in_query else str(result)


class Jsonb(DataType):
    @staticmethod
    def name() -> str:
        return "jsonb"

    @staticmethod
    def value(rng: random.Random, in_query: bool = False) -> Any:
        result = rng.randint(-100, 100)
        return f"'{result}'::jsonb"


class Bytea(DataType):
    @staticmethod
    def name() -> str:
        return "bytea"

    @staticmethod
    def value(rng: random.Random, in_query: bool = False) -> Any:
        result = rng.randint(-100, 100)
        return f"'{result}'::bytea"


class TextTextMap(DataType):
    @staticmethod
    def name() -> str:
        return "map[text=>text]"

    @staticmethod
    def value(rng: random.Random, in_query: bool = False) -> Any:
        values = [
            f"{Text.value(rng)} => {Text.value(rng)}"
            for i in range(0, rng.randint(0, 10))
        ]
        values_str = f"{{{', '.join(values)}}}"
        return f"'{values_str}'::map[text=>text]"


DATA_TYPES = DataType.__subclasses__()
