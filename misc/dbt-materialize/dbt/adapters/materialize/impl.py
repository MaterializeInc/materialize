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
from typing import Any, List, Mapping, Optional

from dbt.adapters.base.impl import AdapterConfig
from dbt.adapters.materialize import MaterializeConnectionManager
from dbt.adapters.materialize.relation import MaterializeRelation
from dbt.adapters.postgres import PostgresAdapter
from dbt.adapters.sql.impl import LIST_RELATIONS_MACRO_NAME
from dbt.exceptions import RuntimeException


@dataclass
class MaterializeConfig(AdapterConfig):
    materialize_cluster: Optional[str] = None


class MaterializeAdapter(PostgresAdapter):
    ConnectionManager = MaterializeConnectionManager
    Relation = MaterializeRelation

    AdapterSpecificConfigs = MaterializeConfig

    @classmethod
    def is_cancelable(cls) -> bool:
        # TODO: we can support cancellation if Materialize gets support for
        # pg_terminate_backend.
        return False

    def _link_cached_relations(self, manifest):
        # NOTE(benesch): this *should* reimplement the parent class's method
        # for Materialize, but presently none of our tests care if we just
        # disable this method instead.
        #
        # [0]: https://github.com/dbt-labs/dbt-core/blob/13b18654f03d92eab3f5a9113e526a2a844f145d/plugins/postgres/dbt/adapters/postgres/impl.py#L126-L133
        pass

    def verify_database(self, database):
        pass

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

    def _get_cluster(self) -> str:
        _, table = self.execute("SHOW CLUSTER", fetch=True)
        if len(table) == 0 or len(table[0]) == 0:
            raise RuntimeException("Could not get current cluster: no results")
        return str(table[0][0])

    def _set_cluster(self, cluster: str):
        self.execute("SET CLUSTER = %s" % cluster)

    def pre_model_hook(self, config: Mapping[str, Any]) -> Optional[str]:
        default_cluster = self.config.credentials.cluster
        cluster = config.get("materialize_cluster", default_cluster)
        if cluster == default_cluster or cluster is None:
            return None
        previous = self._get_cluster()
        self._set_cluster(cluster)
        return previous

    def post_model_hook(
        self, config: Mapping[str, Any], context: Optional[str]
    ) -> None:
        if context is not None:
            self._set_cluster(context)
