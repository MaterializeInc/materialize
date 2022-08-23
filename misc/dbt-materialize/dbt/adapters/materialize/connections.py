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

import dbt.exceptions
from dbt.adapters.postgres import PostgresConnectionManager, PostgresCredentials
from dbt.semver import versions_compatible

# If you bump this version, bump it in README.md too.
SUPPORTED_MATERIALIZE_VERSIONS = ">=0.20.0"


@dataclass
class MaterializeCredentials(PostgresCredentials):
    cluster: Optional[str] = None

    @property
    def type(self):
        return "materialize"


class MaterializeConnectionManager(PostgresConnectionManager):
    TYPE = "materialize"

    @classmethod
    def open(cls, connection):
        connection = super().open(connection)

        # Prevents psycopg connection from automatically opening transactions.
        # More info: https://www.psycopg.org/docs/usage.html#transactions-control
        connection.handle.autocommit = True

        cursor = connection.handle.cursor()
        cursor.execute("SELECT mz_version()")
        mz_version = cursor.fetchone()[0].split()[0].strip("v")
        if not versions_compatible(mz_version, SUPPORTED_MATERIALIZE_VERSIONS):
            raise dbt.exceptions.RuntimeException(
                f"Detected unsupported Materialize version {mz_version}\n"
                f"  Supported versions: {SUPPORTED_MATERIALIZE_VERSIONS}"
            )

        creds = connection.credentials
        if creds.cluster:
            cursor.execute("SET cluster = %s" % creds.cluster)

        return connection

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
