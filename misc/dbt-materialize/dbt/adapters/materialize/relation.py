# Copyright 2020 Josh Wills. All rights reserved.
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
from typing import Optional, Type

from dbt.adapters.postgres import PostgresRelation
from dbt.dataclass_schema import StrEnum
from dbt.utils import classproperty


class MaterializeRelationType(StrEnum):
    # Built-in materialization types.
    Table = "table"
    View = "view"
    CTE = "cte"
    External = "external"

    # Materialize-specific materialization types.
    Source = "source"
    MaterializedView = "materializedview"
    Index = "index"
    Sink = "sink"


@dataclass(frozen=True, eq=False, repr=False)
class MaterializeRelation(PostgresRelation):
    type: Optional[MaterializeRelationType] = None

    @classproperty
    def get_relation_type(cls) -> Type[MaterializeRelationType]:
        return MaterializeRelationType
