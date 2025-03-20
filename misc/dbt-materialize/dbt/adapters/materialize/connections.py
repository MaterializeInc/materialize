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
from typing import Optional

import dbt_common.exceptions
import psycopg2
from dbt_common.semver import versions_compatible

from dbt.adapters.events.logging import AdapterLogger
from dbt.adapters.postgres import PostgresConnectionManager, PostgresCredentials

from .__version__ import version as __version__

# If you bump this version, bump it in README.md too.
SUPPORTED_MATERIALIZE_VERSIONS = ">=0.68.0"

logger = AdapterLogger("Materialize")


# Override the psycopg2 connect function in order to inject Materialize-specific
# session parameter defaults.
#
# This approach is a bit hacky, but some of these session parameters *must* be
# set as part of connection initiation, so we can't simply run `SET` commands
# after the session is established.
def connect(**kwargs):
    options = [
        # Ensure that dbt's catalog queries get routed to the
        # `mz_catalog_server` cluster, even if the server or role's default is
        # different.
        "--auto_route_catalog_queries=on",
        # dbt prints notices to stdout, which is very distracting because dbt
        # can establish many new connections during `dbt run`.
        "--welcome_message=off",
        # Disable warnings about the session's default database or cluster not
        # existing, as these get quite spammy, especially with multiple threads.
        #
        # Details: it's common for the default cluster for the role dbt is
        # connecting as (often `quickstart`) to be absent. For many dbt
        # deployments, clusters are explicitly specified on a model-by-model
        # basis, and there in fact is no natural "default" cluster. So warning
        # repeatedly that the default cluster doesn't exist isn't helpful, since
        # each DDL statement will specify a different, valid cluster. If a DDL
        # statement ever specifies an invalid cluster, dbt will still produce an
        # error about the invalid cluster, even with this setting enabled.
        "--current_object_missing_warnings=off",
        *(kwargs.get("options") or []),
    ]
    kwargs["options"] = " ".join(options)

    return _connect(**kwargs)


_connect = psycopg2.connect
psycopg2.connect = connect


@dataclass
class MaterializeCredentials(PostgresCredentials):
    # NOTE(morsapaes) The cluster parameter defined in `profiles.yml` is not
    # picked up in the connection string, but is picked up wherever we fall
    # back to `target.cluster`. When no cluster is specified, either in
    # `profiles.yml` or as a configuration, we should default to the default
    # cluster configured for the connected dbt user (or, the active cluster for
    # the connection). This is strictly better than hardcoding `quickstart` as
    # the `target.cluster`, which might not exist and leads to all sorts of
    # annoying errors. This will still fail if the defalt cluster for the
    # connected user is invalid or set to `mz_catalog_server` (which cannot be
    # modified).
    cluster: Optional[str] = None
    application_name: Optional[str] = f"dbt-materialize v{__version__}"

    @property
    def type(self):
        return "materialize"

    def _connection_keys(self):
        return (
            "host",
            "port",
            "user",
            "database",
            "schema",
            "cluster",
            "sslmode",
            "keepalives_idle",
            "connect_timeout",
            "search_path",
            "retries",
            "application_name",
        )


class MaterializeConnectionManager(PostgresConnectionManager):
    TYPE = "materialize"

    @classmethod
    def open(cls, connection):
        connection = super().open(connection)

        # Prevents psycopg connection from automatically opening transactions.
        # More info: https://www.psycopg.org/docs/usage.html#transactions-control
        connection.handle.autocommit = True

        mz_version = connection.handle.info.parameter_status(
            "mz_version"
        )  # e.g. v0.79.0-dev (937dfde5e)
        mz_version = mz_version.split()[0]  # e.g. v0.79.0-dev
        mz_version = mz_version[1:]  # e.g. 0.79.0-dev
        if not versions_compatible(mz_version, SUPPORTED_MATERIALIZE_VERSIONS):
            raise dbt_common.exceptions.DbtRuntimeError(
                f"Detected unsupported Materialize version {mz_version}\n"
                f"  Supported versions: {SUPPORTED_MATERIALIZE_VERSIONS}"
            )

        return connection

    def cancel(self, connection):
        # The PostgreSQL implementation calls `pg_terminate_backend` from a new
        # connection to terminate `connection`. At the time of writing,
        # Materialize doesn't support `pg_terminate_backend`, so we implement
        # cancellation by calling `close` on the connection.
        #
        # NOTE(benesch): I'm not entirely sure why the PostgreSQL implementation
        # uses `pg_terminate_backend`. I suspect that disconnecting the network
        # connection by calling `connection.handle.close()` is not immediately
        # noticed by the PostgreSQL server, and so the queries running on that
        # connection may continue executing to completion. Materialize, however,
        # will quickly notice if the network socket disconnects and cancel any
        # queries that were initiated by that connection.

        connection_name = connection.name
        try:
            logger.debug("Closing connection '{}' to force cancellation")
            connection.handle.close()
        except psycopg2.InterfaceError as exc:
            # if the connection is already closed, not much to cancel!
            if "already closed" in str(exc):
                logger.debug(f"Connection {connection_name} was already closed")
                return
            # probably bad, re-raise it
            raise

    # Disable transactions. Materialize transactions do not support arbitrary
    # queries in transactions and therefore many of dbt's internal macros
    # produce invalid transactions.
    #
    # Disabling transactions has precedent in dbt-snowflake and dbt-biquery.
    # See, for example:
    # https://github.com/dbt-labs/dbt-snowflake/blob/ffbb05391/dbt/adapters/snowflake/connections.py#L359-L374

    def add_begin_query(self, *args, **kwargs):
        pass

    def add_commit_query(self, *args, **kwargs):
        pass

    def begin(self):
        pass

    def commit(self):
        pass

    def clear_transaction(self):
        pass
