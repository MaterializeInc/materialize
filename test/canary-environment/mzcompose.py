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

from materialize.mzcompose import Composition, WorkflowArgumentParser
from materialize.mzcompose.services import Dbt, Materialized, Postgres, Testdrive

# The FieldEng cluster in the Confluent Cloud is used to provide Kafka services
KAFKA_BOOTSTRAP_SERVER = "pkc-n00kk.us-east-1.aws.confluent.cloud:9092"
SCHEMA_REGISTRY_ENDPOINT = "https://psrc-e0919.us-east-2.aws.confluent.cloud"
# The actual values are stored as Pulumi secrets in the i2 repository
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

SERVICES = [Materialized(), Dbt(), Testdrive(no_reset=True), Postgres()]

POSTGRES_RANGE = 1024
POSTGRES_RANGE_FUNCTION = "FLOOR(RANDOM() * (SELECT MAX(id) FROM people))"


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    c.up("materialized", "postgres")
    c.up("dbt", persistent=True)
    c.up("testdrive", persistent=True)

    c.testdrive(
        input=dedent(
            f"""
            > CREATE SECRET IF NOT EXISTS kafka_username AS '{CONFLUENT_CLOUD_FIELDENG_KAFKA_USERNAME}'
            > CREATE SECRET IF NOT EXISTS kafka_password AS '{CONFLUENT_CLOUD_FIELDENG_KAFKA_PASSWORD}'

            > CREATE CONNECTION IF NOT EXISTS kafka_connection TO KAFKA (
              BROKER '{KAFKA_BOOTSTRAP_SERVER}',
              SASL MECHANISMS = 'PLAIN',
              SASL USERNAME = SECRET kafka_username,
              SASL PASSWORD = SECRET kafka_password
              )

            > CREATE SECRET IF NOT EXISTS csr_username AS '{CONFLUENT_CLOUD_FIELDENG_CSR_USERNAME}'
            > CREATE SECRET IF NOT EXISTS csr_password AS '{CONFLUENT_CLOUD_FIELDENG_CSR_PASSWORD}'

            > CREATE CONNECTION IF NOT EXISTS csr_connection TO CONFLUENT SCHEMA REGISTRY (
              URL '{SCHEMA_REGISTRY_ENDPOINT}',
              USERNAME = SECRET csr_username,
              PASSWORD = SECRET csr_password
              )
        """
        )
    )

    c.testdrive(
        input=dedent(
            f"""
            $ postgres-execute connection=postgres://postgres:postgres@postgres
            ALTER USER postgres WITH replication;

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

            > CREATE SECRET IF NOT EXISTS pgpass AS 'postgres'

            > CREATE CONNECTION IF NOT EXISTS pg TO POSTGRES (
              HOST postgres,
              DATABASE postgres,
              USER postgres,
              PASSWORD SECRET pgpass
              )
            """
        )
    )

    c.exec("dbt", "dbt", "run", "--threads", "8", workdir="/workdir")
    workflow_test(c, parser)


def workflow_test(c: Composition, parser: WorkflowArgumentParser) -> None:
    c.exec("dbt", "dbt", "test", "--threads", "8", workdir="/workdir")


def workflow_clean(c: Composition, parser: WorkflowArgumentParser) -> None:
    c.exec("dbt", "dbt", "clean", workdir="/workdir")
