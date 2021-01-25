# Copyright 2020 Josh Wills. All rights reserved.
# Copyright Materialize, Inc. All rights reserved.
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

from dbt.adapters.postgres import PostgresConnectionManager
from dbt.adapters.postgres import PostgresCredentials

import dbt.exceptions
from dataclasses import dataclass
from dbt import flags
from dbt.logger import GLOBAL_LOGGER as logger


@dataclass
class MaterializeCredentials(PostgresCredentials):
    @property
    def type(self):
        return "materialize"


class MaterializeConnectionManager(PostgresConnectionManager):
    TYPE = "materialize"

    @classmethod
    def open(cls, connection):
        connection = super().open(connection)
        # Prevents psycopg connection from automatically opening transactions
        # More info: https://www.psycopg.org/docs/usage.html#transactions-control
        connection.handle.autocommit = True
        return connection

    def commit(self):
        connection = self.get_thread_connection()
        if flags.STRICT_MODE:
            if not isinstance(connection, Connection):
                raise dbt.exceptions.CompilerException(
                    f"In commit, got {connection} - not a Connection!"
                )

        # Instead of throwing an error, quietly log if something tries to commit
        # without an open transaction.
        # This is needed because the dbt-adapter-tests commit after executing SQL,
        # but Materialize can't handle all of the required transactions.
        # https://github.com/fishtown-analytics/dbt/blob/42a85ac39f34b058678fd0c03ff8e8d2835d2808/test/integration/base.py#L681
        if not connection.transaction_open:
            logger.debug(
                'Tried to commit without a transaction on connection "%s"',
                connection.name,
            )

        logger.debug("On %s: COMMIT", connection.name)
        self.add_commit_query()

        connection.transaction_open = False

        return connection
