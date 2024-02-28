# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import os
from textwrap import dedent
from urllib.parse import quote

from materialize.mzcompose.composition import Composition, WorkflowArgumentParser
from materialize.mzcompose.services.dbt import Dbt
from materialize.mzcompose.services.testdrive import Testdrive

# The actual values are stored as Pulumi secrets in the i2 repository
MATERIALIZE_PROD_SANDBOX_HOSTNAME = os.environ["MATERIALIZE_PROD_SANDBOX_HOSTNAME"]
MATERIALIZE_PROD_SANDBOX_USERNAME = os.environ["MATERIALIZE_PROD_SANDBOX_USERNAME"]
MATERIALIZE_PROD_SANDBOX_APP_PASSWORD = os.environ[
    "MATERIALIZE_PROD_SANDBOX_APP_PASSWORD"
]

MATERIALIZE_PROD_SANDBOX_RDS_HOSTNAME = os.environ[
    "MATERIALIZE_PROD_SANDBOX_RDS_HOSTNAME"
]
MATERIALIZE_PROD_SANDBOX_RDS_PASSWORD = os.environ[
    "MATERIALIZE_PROD_SANDBOX_RDS_PASSWORD"
]

MATERIALIZE_PROD_SANDBOX_RDS_MYSQL_HOSTNAME = os.environ[
    "MATERIALIZE_PROD_SANDBOX_RDS_MYSQL_HOSTNAME"
]
MATERIALIZE_PROD_SANDBOX_RDS_MYSQL_PASSWORD = os.environ[
    "MATERIALIZE_PROD_SANDBOX_RDS_MYSQL_PASSWORD"
]

CONFLUENT_CLOUD_FIELDENG_KAFKA_BROKER = os.environ[
    "CONFLUENT_CLOUD_FIELDENG_KAFKA_BROKER"
]
CONFLUENT_CLOUD_FIELDENG_CSR_URL = os.environ["CONFLUENT_CLOUD_FIELDENG_CSR_URL"]

CONFLUENT_CLOUD_FIELDENG_CSR_USERNAME = os.environ[
    "CONFLUENT_CLOUD_FIELDENG_CSR_USERNAME"
]
CONFLUENT_CLOUD_FIELDENG_CSR_PASSWORD = os.environ[
    "CONFLUENT_CLOUD_FIELDENG_CSR_PASSWORD"
]
CONFLUENT_CLOUD_FIELDENG_KAFKA_USERNAME = os.environ[
    "CONFLUENT_CLOUD_FIELDENG_KAFKA_USERNAME"
]
CONFLUENT_CLOUD_FIELDENG_KAFKA_PASSWORD = os.environ[
    "CONFLUENT_CLOUD_FIELDENG_KAFKA_PASSWORD"
]


SERVICES = [
    Dbt(
        environment=[
            "MATERIALIZE_PROD_SANDBOX_HOSTNAME",
            "MATERIALIZE_PROD_SANDBOX_USERNAME",
            "MATERIALIZE_PROD_SANDBOX_APP_PASSWORD",
        ]
    ),
    Testdrive(no_reset=True),
]

POSTGRES_RANGE = 1024
POSTGRES_RANGE_FUNCTION = "FLOOR(RANDOM() * (SELECT MAX(id) FROM people))"
MYSQL_RANGE = 1024
MYSQL_RANGE_FUNCTION = (
    "FLOOR(RAND() * (SELECT MAX(id) FROM (SELECT * FROM people) AS p))"
)


def workflow_create(c: Composition, parser: WorkflowArgumentParser) -> None:
    c.up("dbt", persistent=True)
    c.up("testdrive", persistent=True)

    materialize_url = f"postgres://{quote(MATERIALIZE_PROD_SANDBOX_USERNAME)}:{quote(MATERIALIZE_PROD_SANDBOX_APP_PASSWORD)}@{quote(MATERIALIZE_PROD_SANDBOX_HOSTNAME)}:6875"

    with c.override(
        Testdrive(
            default_timeout="1200s",
            materialize_url=materialize_url,
            no_reset=True,  # Required so that admin port 6877 is not used
        )
    ):
        c.testdrive(
            input=dedent(
                f"""
            > SET DATABASE=qa_canary_environment
            > CREATE SECRET IF NOT EXISTS kafka_username AS '{CONFLUENT_CLOUD_FIELDENG_KAFKA_USERNAME}'
            > CREATE SECRET IF NOT EXISTS kafka_password AS '{CONFLUENT_CLOUD_FIELDENG_KAFKA_PASSWORD}'

            > CREATE CONNECTION IF NOT EXISTS kafka_connection TO KAFKA (
              BROKER '{CONFLUENT_CLOUD_FIELDENG_KAFKA_BROKER}',
              SASL MECHANISMS = 'PLAIN',
              SASL USERNAME = SECRET kafka_username,
              SASL PASSWORD = SECRET kafka_password
              )

            > CREATE SECRET IF NOT EXISTS csr_username AS '{CONFLUENT_CLOUD_FIELDENG_CSR_USERNAME}'
            > CREATE SECRET IF NOT EXISTS csr_password AS '{CONFLUENT_CLOUD_FIELDENG_CSR_PASSWORD}'

            > CREATE CONNECTION IF NOT EXISTS csr_connection TO CONFLUENT SCHEMA REGISTRY (
              URL '{CONFLUENT_CLOUD_FIELDENG_CSR_URL}',
              USERNAME = SECRET csr_username,
              PASSWORD = SECRET csr_password
              )
        """
            )
        )

        c.testdrive(
            input=dedent(
                f"""
            > SET DATABASE=qa_canary_environment
            $ mysql-connect name=mysql url=mysql://admin@{MATERIALIZE_PROD_SANDBOX_RDS_MYSQL_HOSTNAME} password={MATERIALIZE_PROD_SANDBOX_RDS_MYSQL_PASSWORD}
            $ mysql-execute name=mysql
            DROP DATABASE IF EXISTS public;
            CREATE DATABASE public;
            USE public;

            CREATE TABLE IF NOT EXISTS people (id INTEGER PRIMARY KEY, name TEXT, incarnation INTEGER DEFAULT 1);
            CREATE TABLE IF NOT EXISTS relationships (a INTEGER, b INTEGER, incarnation INTEGER DEFAULT 1, PRIMARY KEY (a,b));

            CREATE EVENT insert_people ON SCHEDULE EVERY 1 SECOND DO INSERT INTO people (id, name) VALUES (FLOOR(RAND() * {MYSQL_RANGE}), 'aaaaaaaaaaaaaaaa'), (FLOOR(RAND() * {MYSQL_RANGE}), 'aaaaaaaaaaaaaaaa') ON DUPLICATE KEY UPDATE incarnation = people.incarnation + 1;
            CREATE EVENT update_people_name ON SCHEDULE EVERY 1 SECOND DO UPDATE people SET name = REPEAT(id, 16) WHERE id = {MYSQL_RANGE_FUNCTION};
            CREATE EVENT update_people_incarnation ON SCHEDULE EVERY 1 SECOND DO UPDATE people SET incarnation = incarnation + 1 WHERE id = {MYSQL_RANGE_FUNCTION};
            CREATE EVENT delete_people ON SCHEDULE EVERY 1 SECOND DO DELETE FROM people WHERE id = {MYSQL_RANGE_FUNCTION};

            -- MOD() is used to prevent truly random relationships from being created, as this overwhelms WMR
            -- See https://materialize.com/docs/sql/recursive-ctes/#queries-with-update-locality
            CREATE EVENT insert_relationships ON SCHEDULE EVERY 1 SECOND DO INSERT INTO relationships (a, b) VALUES (MOD({MYSQL_RANGE_FUNCTION}, 10), {MYSQL_RANGE_FUNCTION}), (MOD({MYSQL_RANGE_FUNCTION}, 10), {MYSQL_RANGE_FUNCTION}) ON DUPLICATE KEY UPDATE incarnation = relationships.incarnation + 1;
            CREATE EVENT update_relationships_incarnation ON SCHEDULE EVERY 1 SECOND DO UPDATE relationships SET incarnation = incarnation + 1 WHERE a = {MYSQL_RANGE_FUNCTION} and b = {MYSQL_RANGE_FUNCTION};
            CREATE EVENT delete_relationships ON SCHEDULE EVERY 1 SECOND DO DELETE FROM relationships WHERE a = {MYSQL_RANGE_FUNCTION} AND b = {MYSQL_RANGE_FUNCTION};

            > CREATE SECRET IF NOT EXISTS mysql_password AS '{MATERIALIZE_PROD_SANDBOX_RDS_MYSQL_PASSWORD}'

            > CREATE CONNECTION IF NOT EXISTS mysql TO MYSQL (
              HOST '{MATERIALIZE_PROD_SANDBOX_RDS_MYSQL_HOSTNAME}',
              USER admin,
              PASSWORD SECRET mysql_password
              )

            $ postgres-execute connection=postgres://postgres:{MATERIALIZE_PROD_SANDBOX_RDS_PASSWORD}@{MATERIALIZE_PROD_SANDBOX_RDS_HOSTNAME}
            -- ALTER USER postgres WITH replication;

            DROP SCHEMA IF EXISTS public CASCADE;
            CREATE SCHEMA public;

            CREATE TABLE IF NOT EXISTS people (id INTEGER PRIMARY KEY, name TEXT DEFAULT REPEAT('a', 16), incarnation INTEGER DEFAULT 1);
            ALTER TABLE people REPLICA IDENTITY FULL;

            CREATE TABLE IF NOT EXISTS relationships (a INTEGER, b INTEGER, incarnation INTEGER DEFAULT 1, PRIMARY KEY (a,b));
            ALTER TABLE relationships REPLICA IDENTITY FULL;

            DROP PUBLICATION IF EXISTS mz_source;
            CREATE PUBLICATION mz_source FOR ALL TABLES;

            CREATE EXTENSION IF NOT EXISTS pg_cron;

            SELECT cron.schedule('insert-people', '1 seconds', 'INSERT INTO people (id) SELECT FLOOR(RANDOM() * {POSTGRES_RANGE}) FROM generate_series(1,2) ON CONFLICT (id) DO UPDATE SET incarnation = people.incarnation + 1');
            SELECT cron.schedule('update-people-name', '1 seconds', 'UPDATE people SET name = REPEAT(id::text, 16) WHERE id = {POSTGRES_RANGE_FUNCTION}');
            SELECT cron.schedule('update-people-incarnation', '1 seconds', 'UPDATE people SET incarnation = incarnation + 1 WHERE id = {POSTGRES_RANGE_FUNCTION}');
            SELECT cron.schedule('delete-people', '1 seconds', 'DELETE FROM people WHERE id = {POSTGRES_RANGE_FUNCTION}');

            -- MOD() is used to prevent truly random relationships from being created, as this overwhelms WMR
            -- See https://materialize.com/docs/sql/recursive-ctes/#queries-with-update-locality
            SELECT cron.schedule('insert-relationships', '1 seconds', 'INSERT INTO relationships (a,b) SELECT MOD({POSTGRES_RANGE_FUNCTION}::INTEGER, 10), {POSTGRES_RANGE_FUNCTION} FROM generate_series(1,2) ON CONFLICT (a, b) DO UPDATE SET incarnation = relationships.incarnation + 1');
            SELECT cron.schedule('update-relationships-incarnation', '1 seconds', 'UPDATE relationships SET incarnation = incarnation + 1 WHERE a = {POSTGRES_RANGE_FUNCTION} AND b = {POSTGRES_RANGE_FUNCTION}');

            SELECT cron.schedule('delete-relationships', '1 seconds', 'DELETE FROM relationships WHERE a = {POSTGRES_RANGE_FUNCTION} AND b = {POSTGRES_RANGE_FUNCTION}');

            > CREATE SECRET IF NOT EXISTS pg_password AS '{MATERIALIZE_PROD_SANDBOX_RDS_PASSWORD}'

            > CREATE CONNECTION IF NOT EXISTS pg TO POSTGRES (
              HOST '{MATERIALIZE_PROD_SANDBOX_RDS_HOSTNAME}',
              DATABASE postgres,
              USER postgres,
              PASSWORD SECRET pg_password,
              SSL MODE 'require'
              )
            """
            )
        )

    c.exec("dbt", "dbt", "run", "--threads", "8", workdir="/workdir")


def workflow_test(c: Composition, parser: WorkflowArgumentParser) -> None:
    c.up("dbt", persistent=True)
    c.exec("dbt", "dbt", "test", workdir="/workdir")


def workflow_clean(c: Composition, parser: WorkflowArgumentParser) -> None:
    c.exec("dbt", "dbt", "clean", workdir="/workdir")
