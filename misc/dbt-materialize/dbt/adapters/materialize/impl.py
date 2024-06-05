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
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import dbt_common.exceptions
from dbt_common.contracts.constraints import (
    ColumnLevelConstraint,
    ConstraintType,
)
from dbt_common.dataclass_schema import ValidationError, dbtClassMixin

from dbt.adapters.base.impl import AdapterConfig, ConstraintSupport
from dbt.adapters.base.meta import available
from dbt.adapters.capability import (
    Capability,
    CapabilityDict,
    CapabilitySupport,
    Support,
)
from dbt.adapters.materialize.connections import MaterializeConnectionManager
from dbt.adapters.materialize.exceptions import (
    RefreshIntervalConfigError,
    RefreshIntervalConfigNotDictError,
)
from dbt.adapters.materialize.relation import MaterializeRelation
from dbt.adapters.postgres.column import PostgresColumn
from dbt.adapters.postgres.impl import PostgresAdapter
from dbt.adapters.sql.impl import LIST_RELATIONS_MACRO_NAME


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
            msg = dbt_common.exceptions.validator_error_message(exc)
            dbt_common.exceptions.CompilationError(
                f"Could not parse index config: {msg}"
            )
        except TypeError:
            dbt_common.exceptions.CompilationError(
                "Invalid index config:\n"
                f"  Got: {raw_index}\n"
                '  Expected a dictionary with at minimum a "columns" key'
            )


# NOTE(morsapaes): Materialize allows configuring a refresh interval for the
# materialized view materialization. If no config option is specified, the
# default is REFRESH ON COMMIT. We add an explicitly attribute for the special
# case of parametrizing the configuration option e.g. in macros.
@dataclass
class MaterializeRefreshIntervalConfig(dbtClassMixin):
    at: Optional[str] = None
    at_creation: Optional[bool] = False
    every: Optional[str] = None
    aligned_to: Optional[str] = None
    on_commit: Optional[bool] = False

    @classmethod
    def parse(
        cls, raw_refresh_interval
    ) -> Optional["MaterializeRefreshIntervalConfig"]:
        if raw_refresh_interval is None:
            return None
        try:
            cls.validate(raw_refresh_interval)
            return cls.from_dict(raw_refresh_interval)
        except ValidationError as exc:
            raise RefreshIntervalConfigError(exc)
        except TypeError:
            raise RefreshIntervalConfigNotDictError(raw_refresh_interval)


@dataclass
class MaterializeConfig(AdapterConfig):
    cluster: Optional[str] = None
    refresh_interval: Optional[MaterializeRefreshIntervalConfig] = None


class MaterializeAdapter(PostgresAdapter):
    ConnectionManager = MaterializeConnectionManager
    Relation = MaterializeRelation

    AdapterSpecificConfigs = MaterializeConfig

    # NOTE(morsapaes): Materialize supports enforcing the NOT NULL constraint
    # for materialized views (via the ASSERT NOT NULL clause) and tables. As a
    # reminder, tables are modeled as static materialized views (see #5266).
    CONSTRAINT_SUPPORT = {
        ConstraintType.check: ConstraintSupport.NOT_SUPPORTED,
        ConstraintType.not_null: ConstraintSupport.ENFORCED,
        ConstraintType.unique: ConstraintSupport.NOT_SUPPORTED,
        ConstraintType.primary_key: ConstraintSupport.NOT_SUPPORTED,
        ConstraintType.foreign_key: ConstraintSupport.NOT_SUPPORTED,
    }

    # NOTE(morsapaes): Materialize supports the functionality required to enable
    # metadata source freshness checks, but the value of this feature in a data
    # warehouse built for real-time is limited, so we do not implement it.
    _capabilities = CapabilityDict(
        {
            Capability.SchemaMetadataByRelations: CapabilitySupport(
                support=Support.NotImplemented
            ),
            Capability.TableLastModifiedMetadata: CapabilitySupport(
                support=Support.NotImplemented
            ),
        }
    )

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

    @available
    def parse_refresh_interval(
        self, raw_refresh_interval: Any
    ) -> Optional[MaterializeRefreshIntervalConfig]:
        return MaterializeRefreshIntervalConfig.parse(raw_refresh_interval)

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

    # NOTE(morsapaes): Materialize doesn't inline not_null constraints
    # in the DDL in the same way as other adapters, so we override the
    # default constraint rendering functions.
    @classmethod
    def render_column_constraint(cls, constraint: ColumnLevelConstraint) -> str:
        if constraint.type == ConstraintType.not_null:
            rendered_column_constraint = None
            rendered_column_constraint = "assert not null"

            return rendered_column_constraint
        else:
            return ""

    @available
    @classmethod
    def render_raw_columns_constraints(
        cls, raw_columns: Dict[str, Dict[str, Any]]
    ) -> List:
        rendered_column_constraints = []

        for v in raw_columns.values():
            if v.get("constraints"):
                col_name = cls.quote(v["name"]) if v.get("quote") else v["name"]
                rendered_column_constraint = [col_name]
                for con in v.get("constraints", None):
                    constraint = cls._parse_column_constraint(con)
                    c = cls.process_parsed_constraint(
                        constraint, cls.render_column_constraint
                    )
                    if c is not None:
                        rendered_column_constraint.insert(0, c)
                rendered_column_constraints.append(" ".join(rendered_column_constraint))

        return rendered_column_constraints

    @available
    @classmethod
    def sleep(cls, seconds):
        time.sleep(seconds)

    def get_column_schema_from_query(self, sql: str) -> List[PostgresColumn]:
        # The idea is that this function returns the names and types of the
        # columns returned by `sql` without actually executing the statement.
        #
        # The easiest way to do this would be to create a prepared statement
        # using `sql` and then inspect the result type of the prepared
        # statement. Unfortunately the Python DB-API and the underlying psycopg2
        # driver do not support preparing statements.
        #
        # So instead we create a temporary view based on the SQL statement and
        # inspect the types of the columns in the view using `mz_columns`.
        #
        # Note that the upstream PostgreSQL adapter takes a different approach.
        # It executes `SELECT * FROM (<query>) WHERE FALSE LIMIT 0`, which in
        # PostgreSQL is optimized to a no-op that returns no rows, and then
        # inspects the description of the returned cursor. That doesn't work
        # well in Materialize, because `SELECT ... LIMIT 0` requires a cluster
        # (even though it returns no rows), and we're not guaranteed that the
        # connection has a valid cluster.

        # Determine the name to use for the view. Unfortunately Materialize
        # comingles data about temporary views from all sessions in the system
        # catalog, so we need to manually include the connection ID in the name
        # to be able to identify the catalog entries for the temporary views
        # created by this specific connection.
        connection_id = self.connections.get_thread_connection().handle.info.backend_pid
        view_name = f"__dbt_sbq{connection_id}"

        # NOTE(morsapaes): because we need to consider the existence of a
        # sql_header configuration (see dbt-core #7714) but don't support
        # running the header SQL in the same transaction as the model SQL
        # statement, we split the input based on the string appended to the
        # header in materialize__get_empty_subquery_sql.

        sql_header, sql_view_def = sql.split("--#dbt_sbq_parse_header#--")

        if sql_header:
            self.connections.execute(sql_header)
        self.connections.execute(f"create temporary view {view_name} as {sql_view_def}")

        # Fetch the names and types of each column in the view. Schema ID 0
        # indicates the temporary schema.
        _, table = self.connections.execute(
            f"""select c.name, c.type_oid
            from mz_columns c
            join mz_relations r ON c.id = r.id
            where r.schema_id = '0' AND r.name = '{view_name}'""",
            fetch=True,
        )
        columns = [
            self.Column.create(
                column_name, self.connections.data_type_code_to_name(column_type_code)
            )
            # https://peps.python.org/pep-0249/#description
            for column_name, column_type_code in table.rows
        ]

        # Drop the temporary view, so that we can reuse its name if this
        # function is called again on the same thread. It's okay if we leak this
        # view (e.g., if an error above causes us to return early), as temporary
        # views are automatically dropped by Materialize when the connection
        # terminates.
        self.connections.execute(f"drop view {view_name}")

        return columns

    @available
    def generate_final_cluster_name(
        self, cluster_name: str, force_deploy_suffix: bool = False
    ) -> str:
        cluster_name = self.execute_macro(
            "generate_cluster_name",
            kwargs={"custom_cluster_name": cluster_name},
        )
        if (
            self.connections.profile.cli_vars.get("deploy", False)
            or force_deploy_suffix
        ):
            cluster_name = self.execute_macro(
                "generate_deploy_cluster_name",
                kwargs={"custom_cluster_name": cluster_name},
            )
        return cluster_name
