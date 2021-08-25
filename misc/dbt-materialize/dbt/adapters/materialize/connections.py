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
import psycopg2
from dbt import flags
from dbt.adapters.postgres import PostgresConnectionManager, PostgresCredentials
from dbt.logger import GLOBAL_LOGGER as logger


@dataclass
class MaterializeCredentials(PostgresCredentials):
    sslcert: Optional[str] = None
    sslkey: Optional[str] = None
    sslrootcert: Optional[str] = None

    @property
    def type(self):
        return "materialize"


class MaterializeConnectionManager(PostgresConnectionManager):
    TYPE = "materialize"

    @classmethod
    def open(cls, connection):
        if connection.state == "open":
            logger.debug("Connection is already open, skipping open.")
            return connection

        credentials = cls.get_credentials(connection.credentials)
        kwargs = {}
        # we don't want to pass 0 along to connect() as postgres will try to
        # call an invalid setsockopt() call (contrary to the docs).
        if credentials.keepalives_idle:
            kwargs["keepalives_idle"] = credentials.keepalives_idle

        # psycopg2 doesn't support search_path officially,
        # see https://github.com/psycopg/psycopg2/issues/465
        search_path = credentials.search_path
        if search_path is not None and search_path != "":
            # see https://postgresql.org/docs/9.5/libpq-connect.html
            kwargs["options"] = "-c search_path={}".format(
                search_path.replace(" ", "\\ ")
            )

        if credentials.sslmode:
            kwargs["sslmode"] = credentials.sslmode

        if credentials.sslcert is not None:
            kwargs["sslcert"] = credentials.sslcert

        if credentials.sslkey is not None:
            kwargs["sslkey"] = credentials.sslkey

        if credentials.sslrootcert is not None:
            kwargs["sslrootcert"] = credentials.sslrootcert

        try:
            handle = psycopg2.connect(
                dbname=credentials.database,
                user=credentials.user,
                host=credentials.host,
                password=credentials.password,
                port=credentials.port,
                connect_timeout=10,
                **kwargs,
            )

            if credentials.role:
                handle.cursor().execute("set role {}".format(credentials.role))

            connection.handle = handle
            connection.state = "open"
        except psycopg2.Error as e:
            logger.debug(
                "Got an error when attempting to open a postgres "
                "connection: '{}'".format(e)
            )

            connection.handle = None
            connection.state = "fail"

            raise dbt.exceptions.FailedToConnectException(str(e))

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
