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
        return 'materialize'

class MaterializeConnectionManager(PostgresConnectionManager):
    TYPE = 'materialize'

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
                    f'In commit, got {connection} - not a Connection!'
                )

        # Instead of throwing an error, quietly log if something tries to commit
        # without an open transaction.
        # This is needed because the dbt-adapter-tests commit after executing SQL,
        # but Materialize can't handle all of the required transactions.
        # https://github.com/fishtown-analytics/dbt/blob/42a85ac39f34b058678fd0c03ff8e8d2835d2808/test/integration/base.py#L681
        if connection.transaction_open is False:
            logger.debug('Tried to commit without a transaction on connection "{}"'.format(connection.name))

        logger.debug('On {}: COMMIT'.format(connection.name))
        self.add_commit_query()

        connection.transaction_open = False

        return connection