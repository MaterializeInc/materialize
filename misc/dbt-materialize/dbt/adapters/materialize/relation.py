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

from dbt_common.dataclass_schema import StrEnum

from dbt.adapters.postgres.relation import PostgresRelation
from dbt.adapters.utils import classproperty


# types in ./misc/dbt-materialize need to import generic types from typing
class MaterializeRelationType(StrEnum):
    # Built-in materialization types.
    Table = "table"
    View = "view"
    CTE = "cte"
    External = "external"
    MaterializedView = "materialized_view"

    # Materialize-specific materialization types.
    Source = "source"
    Sink = "sink"
    # NOTE(morsapaes): dbt supports materialized views as a built-in
    # materialization since v1.6.0, so we deprecate the legacy materialization
    # name but keep it around for backwards compatibility.
    MaterializedViewLegacy = "materializedview"


@dataclass(frozen=True, eq=False, repr=False)
class MaterializeRelation(PostgresRelation):
    type: Optional[MaterializeRelationType] = None
    require_alias: bool = False

    # Materialize does not have a 63-character limit for relation names, unlike
    # PostgreSQL (see dbt-core #2727). Instead, we set 255 as the maximum
    # identifier length (see #20931).
    def relation_max_name_length(self):
        return 255

    @classproperty
    def get_relation_type(cls) -> Type[MaterializeRelationType]:
        return MaterializeRelationType

    @property
    def is_materialized_view(self) -> bool:
        return self.type in [
            MaterializeRelationType.MaterializedView,
            MaterializeRelationType.MaterializedViewLegacy,
        ]
