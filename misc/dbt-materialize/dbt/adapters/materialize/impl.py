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
from typing import Any, List, Optional

import dbt.exceptions
from dbt.adapters.base.impl import AdapterConfig, ConstraintSupport
from dbt.adapters.materialize.connections import MaterializeConnectionManager
from dbt.adapters.materialize.relation import MaterializeRelation
from dbt.adapters.postgres import PostgresAdapter
from dbt.adapters.sql.impl import LIST_RELATIONS_MACRO_NAME
from dbt.contracts.graph.nodes import ConstraintType
from dbt.dataclass_schema import ValidationError, dbtClassMixin


# types in ./misc/dbt-materialize need to import generic types from typing
@dataclass
class MaterializeIndexConfig(dbtClassMixin):
    columns: Optional[List[str]] = None
    default: Optional[bool] = False
    name: Optional[str] = None
    cluster: Optional[str] = None

    @classmethod
    def parse(cls, raw_index) -> Optional["MaterializeIndexConfig"]:
        if raw_index is None:
            return None
        try:
            cls.validate(raw_index)
            return cls.from_dict(raw_index)
        except ValidationError as exc:
            msg = dbt.exceptions.validator_error_message(exc)
            dbt.exceptions.CompilationError(f"Could not parse index config: {msg}")
        except TypeError:
            dbt.exceptions.CompilationError(
                "Invalid index config:\n"
                f"  Got: {raw_index}\n"
                '  Expected a dictionary with at minimum a "columns" key'
            )


@dataclass
class MaterializeConfig(AdapterConfig):
    cluster: Optional[str] = None


class MaterializeAdapter(PostgresAdapter):
    ConnectionManager = MaterializeConnectionManager
    Relation = MaterializeRelation

    AdapterSpecificConfigs = MaterializeConfig

    # NOTE(morsapaes): Materialize supports enforcing the NOT NULL constraint,
    # but only for tables. In dbt-materialize, table models are materialized as
    # static materialized views (see #5266), so we're marking all constraints
    # as not supported.
    CONSTRAINT_SUPPORT = {
        ConstraintType.check: ConstraintSupport.NOT_SUPPORTED,
        ConstraintType.not_null: ConstraintSupport.NOT_SUPPORTED,
        ConstraintType.unique: ConstraintSupport.NOT_SUPPORTED,
        ConstraintType.primary_key: ConstraintSupport.NOT_SUPPORTED,
        ConstraintType.foreign_key: ConstraintSupport.NOT_SUPPORTED,
    }

    @classmethod
    def is_cancelable(cls) -> bool:
        return True

    def _link_cached_relations(self, manifest):
        # NOTE(benesch): this *should* reimplement the parent class's method
        # for Materialize, but presently none of our tests care if we just
        # disable this method instead.
        #
        # [0]: https://github.com/dbt-labs/dbt-core/blob/13b18654f03d92eab3f5a9113e526a2a844f145d/plugins/postgres/dbt/adapters/postgres/impl.py#L126-L133
        pass

    def verify_database(self, database):
        pass

    def parse_index(self, raw_index: Any) -> Optional[MaterializeIndexConfig]:
        return MaterializeIndexConfig.parse(raw_index)

    def list_relations_without_caching(
        self, schema_relation: MaterializeRelation
    ) -> List[MaterializeRelation]:
        kwargs = {"schema_relation": schema_relation}
        results = self.execute_macro(LIST_RELATIONS_MACRO_NAME, kwargs=kwargs)

        relations = []
        quote_policy = {"database": True, "schema": True, "identifier": True}

        columns = ["database", "schema", "name", "type"]
        for _database, _schema, _identifier, _type in results.select(columns):
            try:
                _type = self.Relation.get_relation_type(_type.lower())
            except ValueError:
                _type = self.Relation.get_relation_type.View
            relations.append(
                self.Relation.create(
                    database=_database,
                    schema=_schema,
                    identifier=_identifier,
                    quote_policy=quote_policy,
                    type=_type,
                )
            )

        return relations
