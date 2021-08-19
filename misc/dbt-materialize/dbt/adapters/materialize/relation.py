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

from dataclasses import dataclass
from enum import Enum
from typing import Optional

from dbt.adapters.postgres import PostgresRelation
from mashumaro.types import SerializableType


# Stolen from https://github.com/dbt-labs/dbt/blob/develop/core/dbt/dataclass_schema.py ,
# which is not importable.
class StrEnum(str, SerializableType, Enum):
    def __str__(self):
        return self.value

    # https://docs.python.org/3.6/library/enum.html#using-automatic-values
    def _generate_next_value_(name, *_):
        return name

    def _serialize(self) -> str:
        return self.value

    @classmethod
    def _deserialize(cls, value: str):
        return cls(value)


# Override RelationType to add Materialize-specifc materializatiton types:
#   - source
#   - index
#   - sink
class MaterializeRelationType(StrEnum):
    Source = "source"
    View = "view"
    MaterializedView = "materializedview"
    Table = "table"
    Index = "index"
    Sink = "sink"
    CTE = "cte"


@dataclass(frozen=True, eq=False, repr=False)
class MaterializeRelation(PostgresRelation):
    type: Optional[MaterializeRelationType] = None
