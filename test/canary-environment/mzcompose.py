# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Create and validate the QA canary environment objects.

The bulk of the environment — connections, secrets, sources, source-tables,
materialized views and sinks — is declared as an mz-deploy project under
``models/`` and reconciled with ``mz-deploy apply`` (infrastructure) plus
``stage``/``wait``/``promote`` (views/MVs blue-green). Two things stay in
testdrive because mz-deploy can't manage them:

* ``public_webhook.webhook_source`` — ``CREATE SOURCE ... FROM WEBHOOK`` is a
  distinct statement type mz-deploy does not accept.
* the ``public_table`` domain (``table`` + ``table_mv`` + its sinks) — ``table``
  is a reserved word, so it cannot be path-matched as an mz-deploy object, and
  ``table_mv`` reads it.

The upstream RDS/MySQL load (pg_cron jobs, MySQL events, publication) and the
BigLake namespace bootstrap also stay here — they live outside Materialize.
"""

import argparse
import base64
import json
import os
import subprocess
import threading
import time
from collections.abc import Callable
from textwrap import dedent
from urllib.parse import quote

import psycopg
from psycopg import sql

from materialize.biglake import bootstrap_namespace
from materialize.mzcompose import loader
from materialize.mzcompose.composition import (
    Composition,
    Service,
    WorkflowArgumentParser,
)
from materialize.mzcompose.service import Service as ServiceConfig
from materialize.mzcompose.services.testdrive import Testdrive
from materialize.parallel_task import TaskSpec, run_parallel

# The actual values are stored as Pulumi secrets in the i2 repository
MATERIALIZE_PROD_SANDBOX_HOSTNAME = os.getenv("MATERIALIZE_PROD_SANDBOX_HOSTNAME")
MATERIALIZE_PROD_SANDBOX_USERNAME = os.getenv("MATERIALIZE_PROD_SANDBOX_USERNAME")
MATERIALIZE_PROD_SANDBOX_APP_PASSWORD = os.getenv(
    "MATERIALIZE_PROD_SANDBOX_APP_PASSWORD"
)

MATERIALIZE_PROD_SANDBOX_RDS_HOSTNAME = os.getenv(
    "MATERIALIZE_PROD_SANDBOX_RDS_HOSTNAME"
)
MATERIALIZE_PROD_SANDBOX_RDS_PASSWORD = os.getenv(
    "MATERIALIZE_PROD_SANDBOX_RDS_PASSWORD"
)

MATERIALIZE_PROD_SANDBOX_RDS_MYSQL_HOSTNAME = os.getenv(
    "MATERIALIZE_PROD_SANDBOX_RDS_MYSQL_HOSTNAME"
)
MATERIALIZE_PROD_SANDBOX_RDS_MYSQL_PASSWORD = os.getenv(
    "MATERIALIZE_PROD_SANDBOX_RDS_MYSQL_PASSWORD"
)

CONFLUENT_CLOUD_QA_CANARY_KAFKA_BROKER = os.getenv(
    "CONFLUENT_CLOUD_QA_CANARY_KAFKA_BROKER"
)
CONFLUENT_CLOUD_QA_CANARY_CSR_URL = os.getenv("CONFLUENT_CLOUD_QA_CANARY_CSR_URL")

# Base64-encoded GCP service account key JSON with access to the GCS bucket,
# defined in the i2 repository (i2/buildkite.py). The bucket is dedicated to
# the canary (no age-based object expiry, unlike ICEBERG_GCS_BUCKET).
QA_CANARY_ICEBERG_GCP_SA_JSON_B64 = os.getenv("QA_CANARY_ICEBERG_GCP_SA_JSON_B64")
QA_CANARY_ICEBERG_GCS_BUCKET = os.getenv("QA_CANARY_ICEBERG_GCS_BUCKET")

# Environment variables forwarded into the mz-deploy container. The secret
# values are read by `CREATE SECRET ... env_var(...)` at apply time; the
# password is expanded into profiles.toml; the connection-endpoint variables are
# written into project.toml (see write_project_toml).
MZ_DEPLOY_ENV = [
    "MATERIALIZE_PROD_SANDBOX_APP_PASSWORD",
    "MATERIALIZE_PROD_SANDBOX_RDS_PASSWORD",
    "MATERIALIZE_PROD_SANDBOX_RDS_MYSQL_PASSWORD",
    "CONFLUENT_CLOUD_QA_CANARY_KAFKA_USERNAME",
    "CONFLUENT_CLOUD_QA_CANARY_KAFKA_PASSWORD",
    "CONFLUENT_CLOUD_QA_CANARY_CSR_USERNAME",
    "CONFLUENT_CLOUD_QA_CANARY_CSR_PASSWORD",
    # Set at runtime by workflow_create from the base64-encoded key.
    "QA_CANARY_ICEBERG_GCP_SA_JSON",
]

SERVICES = [
    # mz-deploy runs as a prebuilt mzbuild image (see src/mz-deploy/ci) so CI
    # doesn't recompile it on every run. The project directory (this directory)
    # is mounted at /workdir; the image entrypoint is the binary itself.
    ServiceConfig(
        name="mz-deploy",
        config={
            "mzbuild": "mz-deploy",
            "environment": MZ_DEPLOY_ENV,
            "volumes": [f"{loader.composition_path}:/workdir"],
        },
    ),
    Testdrive(
        no_reset=True,
        no_consistency_checks=True,  # No access to HTTP for coordinator check
    ),
]

POSTGRES_RANGE = 1024
POSTGRES_RANGE_FUNCTION = "FLOOR(RANDOM() * (SELECT MAX(id) FROM people))"
MYSQL_RANGE = 1024
MYSQL_RANGE_FUNCTION = (
    "FLOOR(RAND() * (SELECT MAX(id) FROM (SELECT * FROM people) AS p))"
)

# Materialized views referenced by test/canary-load and test/parallel-benchmark.
# They must keep their consumer-facing FQNs (the storage objects that feed them
# live in the parallel `_sources` schemas instead).
MONITORED_MVS = [
    "qa_canary_environment.public_tpch.tpch_q01",
    "qa_canary_environment.public_tpch.tpch_q18",
    "qa_canary_environment.public_pg_cdc.pg_wmr",
    "qa_canary_environment.public_mysql_cdc.mysql_wmr",
    "qa_canary_environment.public_loadgen.sales_product_product_category",
]
# Continuously-updated source tables that should also make progress.
MONITORED_SOURCE_TABLES = [
    "qa_canary_environment.public_tpch_sources.tpch_orders",
    "qa_canary_environment.public_pg_cdc_sources.pg_people",
    "qa_canary_environment.public_pg_cdc_sources.pg_relationships",
    "qa_canary_environment.public_mysql_cdc_sources.mysql_people",
    "qa_canary_environment.public_mysql_cdc_sources.mysql_relationships",
    "qa_canary_environment.public_loadgen_sources.customer_tbl",
    "qa_canary_environment.public_loadgen_sources.sales_tbl",
]


def write_profiles() -> None:
    """Write the git-ignored profiles.toml pointing at the prod sandbox.

    The password is taken from $MATERIALIZE_PROD_SANDBOX_APP_PASSWORD at
    connect time via the `${VAR}` form. The default sslmode for a non-loopback
    host is `require`, which is what the cloud sandbox serves.
    """
    assert MATERIALIZE_PROD_SANDBOX_HOSTNAME is not None
    assert MATERIALIZE_PROD_SANDBOX_USERNAME is not None
    assert loader.composition_path is not None
    with open(loader.composition_path / "profiles.toml", "w") as f:
        f.write(
            "[prod]\n"
            f'host = "{MATERIALIZE_PROD_SANDBOX_HOSTNAME}"\n'
            "port = 6875\n"
            f'username = "{MATERIALIZE_PROD_SANDBOX_USERNAME}"\n'
            'password = "${MATERIALIZE_PROD_SANDBOX_APP_PASSWORD}"\n'
        )


def write_project_toml() -> None:
    """Render the git-ignored project.toml with the real connection endpoints
    from the environment.

    These are not secret (credentials go through CREATE SECRET) but they are
    environment-specific and shouldn't be public, and mz-deploy has no env/CLI
    override for profile variables, so they have to live in project.toml. We
    therefore keep project.toml out of git (see .gitignore) and render it here;
    the committed template with placeholder values is project.toml.example.
    Keep the [prod.variables] keys below in sync with that template.
    """
    assert CONFLUENT_CLOUD_QA_CANARY_KAFKA_BROKER is not None
    assert CONFLUENT_CLOUD_QA_CANARY_CSR_URL is not None
    assert MATERIALIZE_PROD_SANDBOX_RDS_HOSTNAME is not None
    assert MATERIALIZE_PROD_SANDBOX_RDS_MYSQL_HOSTNAME is not None
    assert QA_CANARY_ICEBERG_GCS_BUCKET is not None
    assert loader.composition_path is not None
    with open(loader.composition_path / "project.toml", "w") as f:
        f.write(dedent(f"""\
                # Copyright Materialize, Inc. and contributors. All rights reserved.
                #
                # Use of this software is governed by the Business Source License
                # included in the LICENSE file at the root of this repository.
                #
                # As of the Change Date specified in that file, in accordance with
                # the Business Source License, use of this software will be governed
                # by the Apache License, Version 2.0.

                # GENERATED by mzcompose.py from the environment. The committed copy holds
                # placeholder variable values for offline `mz-deploy compile`; do not commit
                # the runtime-rendered values.

                mz_version = "cloud"
                dependencies = []

                [prod.variables]
                kafka_broker = "{CONFLUENT_CLOUD_QA_CANARY_KAFKA_BROKER}"
                csr_url = "{CONFLUENT_CLOUD_QA_CANARY_CSR_URL}"
                pg_host = "{MATERIALIZE_PROD_SANDBOX_RDS_HOSTNAME}"
                mysql_host = "{MATERIALIZE_PROD_SANDBOX_RDS_MYSQL_HOSTNAME}"
                gcs_warehouse = "gs://{QA_CANARY_ICEBERG_GCS_BUCKET}"
                """))


def mz_deploy(c: Composition, *args: str, check: bool = True) -> None:
    """Run `mz-deploy <args>` against the prod sandbox project."""
    # capture_and_print streams output live (so long-running commands like
    # `wait` show their progress dashboard as it happens) while still capturing
    # it for the error message. Plain `capture` buffers until the command exits.
    result = c.run(
        "mz-deploy",
        "-d",
        "/workdir",
        "--profiles-dir",
        "/workdir",
        "--profile",
        "prod",
        *args,
        capture_and_print=True,
        check=False,
        rm=True,
    )
    if check and result.returncode != 0:
        raise subprocess.CalledProcessError(
            result.returncode, ["mz-deploy", *args], result.stdout, result.stderr
        )


def log(msg: str) -> None:
    """Print a progress line. `flush=True` because mzcompose block-buffers
    Python stdout when it isn't attached to a TTY, which otherwise hides
    progress until the workflow exits."""
    print(msg, flush=True)


def deploy_id() -> str:
    """A stable per-commit deploy id. Same commit + no project changes ⇒ the
    changeset is empty and `stage` is a no-op; a new commit gets a fresh id."""
    try:
        sha = subprocess.check_output(
            ["git", "rev-parse", "--short=7", "HEAD"], text=True
        ).strip()
        return f"canary-{sha}"
    except subprocess.CalledProcessError:
        return "canary"


def workflow_create(c: Composition, parser: WorkflowArgumentParser) -> None:
    parser.add_argument(
        "--wait-timeout",
        default="1800",
        help="Seconds to watch staging hydration before promoting anyway. "
        "Hydration continues in production after promote, so this is just a "
        "best-effort visibility window; 0 skips waiting.",
    )
    args = parser.parse_args()

    assert MATERIALIZE_PROD_SANDBOX_USERNAME is not None
    assert MATERIALIZE_PROD_SANDBOX_APP_PASSWORD is not None
    assert MATERIALIZE_PROD_SANDBOX_HOSTNAME is not None
    assert QA_CANARY_ICEBERG_GCP_SA_JSON_B64 is not None
    assert QA_CANARY_ICEBERG_GCS_BUCKET is not None

    # The secret resolver only resolves a top-level `env_var(...)`, so it can't
    # base64-decode like the old `decode(<b64>, 'base64')` did. `CREATE SECRET`
    # stores its value as bytea, and a raw-JSON literal fails `bytea_in` on the
    # `\n` escapes in the private key. Hand the resolver the JSON *bytes* in
    # bytea hex form (`\x...`), which reproduces exactly what the base64 decode
    # produced.
    os.environ["QA_CANARY_ICEBERG_GCP_SA_JSON"] = (
        "\\x" + base64.b64decode(QA_CANARY_ICEBERG_GCP_SA_JSON_B64).hex()
    )

    write_profiles()
    write_project_toml()

    # mz-deploy is invoked one-shot via `c.run` (see mz_deploy), so only
    # testdrive needs to be brought up.
    c.up(Service("testdrive", idle=True))

    materialize_url = f"postgres://{quote(MATERIALIZE_PROD_SANDBOX_USERNAME)}:{quote(MATERIALIZE_PROD_SANDBOX_APP_PASSWORD)}@{quote(MATERIALIZE_PROD_SANDBOX_HOSTNAME)}:6875"

    # ── 1. Upstream RDS/MySQL setup (outside Materialize) ─────────────────
    # Create the replicated tables, publication, and the cron/event jobs that
    # keep them churning. mz-deploy can't manage these — they live on RDS.
    with c.override(
        Testdrive(
            default_timeout="1200s",
            materialize_url=materialize_url,
            no_reset=True,  # Required so that admin port 6877 is not used
            no_consistency_checks=True,  # No access to HTTP for coordinator check
        )
    ):
        c.testdrive(input=dedent(f"""
            > SELECT 1
            1

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

            $ postgres-execute connection=postgres://postgres:{MATERIALIZE_PROD_SANDBOX_RDS_PASSWORD}@{MATERIALIZE_PROD_SANDBOX_RDS_HOSTNAME}
            -- ALTER USER postgres WITH replication;

            -- Stop the per-second pg_cron jobs from a previous run before
            -- resetting the schema; otherwise an in-flight job holds a lock on
            -- people/relationships and deadlocks with DROP SCHEMA ... CASCADE.
            -- pg_sleep lets any currently-executing job finish first.
            CREATE EXTENSION IF NOT EXISTS pg_cron;
            SELECT cron.unschedule(jobid) FROM cron.job;
            SELECT pg_sleep(2);

            DROP SCHEMA IF EXISTS public CASCADE;
            CREATE SCHEMA public;

            CREATE TABLE IF NOT EXISTS people (id INTEGER PRIMARY KEY, name TEXT DEFAULT REPEAT('a', 16), incarnation INTEGER DEFAULT 1);
            ALTER TABLE people REPLICA IDENTITY FULL;

            CREATE TABLE IF NOT EXISTS relationships (a INTEGER, b INTEGER, incarnation INTEGER DEFAULT 1, PRIMARY KEY (a,b));
            ALTER TABLE relationships REPLICA IDENTITY FULL;

            DROP PUBLICATION IF EXISTS mz_source;
            CREATE PUBLICATION mz_source FOR ALL TABLES;

            SELECT cron.schedule('insert-people', '1 seconds', 'INSERT INTO people (id) SELECT FLOOR(RANDOM() * {POSTGRES_RANGE}) FROM generate_series(1,2) ON CONFLICT (id) DO UPDATE SET incarnation = people.incarnation + 1');
            SELECT cron.schedule('update-people-name', '1 seconds', 'UPDATE people SET name = REPEAT(id::text, 16) WHERE id = {POSTGRES_RANGE_FUNCTION}');
            SELECT cron.schedule('update-people-incarnation', '1 seconds', 'UPDATE people SET incarnation = incarnation + 1 WHERE id = {POSTGRES_RANGE_FUNCTION}');
            SELECT cron.schedule('delete-people', '1 seconds', 'DELETE FROM people WHERE id = {POSTGRES_RANGE_FUNCTION}');

            -- MOD() is used to prevent truly random relationships from being created, as this overwhelms WMR
            -- See https://materialize.com/docs/sql/recursive-ctes/#queries-with-update-locality
            SELECT cron.schedule('insert-relationships', '1 seconds', 'INSERT INTO relationships (a,b) SELECT MOD({POSTGRES_RANGE_FUNCTION}::INTEGER, 10), {POSTGRES_RANGE_FUNCTION} FROM generate_series(1,2) ON CONFLICT (a, b) DO UPDATE SET incarnation = relationships.incarnation + 1');
            SELECT cron.schedule('update-relationships-incarnation', '1 seconds', 'UPDATE relationships SET incarnation = incarnation + 1 WHERE a = {POSTGRES_RANGE_FUNCTION} AND b = {POSTGRES_RANGE_FUNCTION}');

            SELECT cron.schedule('delete-relationships', '1 seconds', 'DELETE FROM relationships WHERE a = {POSTGRES_RANGE_FUNCTION} AND b = {POSTGRES_RANGE_FUNCTION}');
            """))

    # ── 2. BigLake namespace ──────────────────────────────────────────────
    # The Materialize Iceberg sink creates tables but not namespaces, and
    # BigLake does not auto-create them on first commit (unlike the S3 Tables
    # bucket, whose `qa_canary_environment` namespace is provisioned alongside
    # the bucket). Create the BigLake catalog + namespace before the GCS Iceberg
    # sinks are promoted, or they fail at runtime with "Tried to create a table
    # under a namespace that does not exist".
    bootstrap_namespace(
        json.loads(base64.b64decode(QA_CANARY_ICEBERG_GCP_SA_JSON_B64)),
        QA_CANARY_ICEBERG_GCS_BUCKET,
        "qa_canary_environment",
    )

    # ── 3. mz-deploy: setup + apply infrastructure ────────────────────────
    # `setup` creates the _mz_deploy metadata database/cluster/roles (the prod
    # sandbox user is a superuser, which setup requires under RBAC). `apply`
    # reconciles secrets, connections, sources, source-tables and seed tables,
    # then refreshes types.lock so `stage` can type-check offline.
    mz_deploy(c, "setup")
    mz_deploy(c, "apply")

    # ── 4. testdrive-managed objects ──────────────────────────────────────
    # The webhook source and the `public_table` domain can't be mz-deploy
    # objects (see module docstring), and the loadgen product tables need seed
    # data. All of this runs after `apply` so the connections/clusters exist.
    with c.override(
        Testdrive(
            default_timeout="1200s",
            materialize_url=materialize_url,
            no_reset=True,
            no_consistency_checks=True,
        )
    ):
        c.testdrive(input=dedent("""
            > SET DATABASE = qa_canary_environment

            # Consumer (test/canary-load) privileges. To read an object the
            # `infra+qacanaryload@materialize.io` role needs the object-level
            # grant (in the model files / below), USAGE on the object's schema,
            # USAGE on the database, and USAGE on the cluster its query runs on.
            # dbt only granted the object level; the rest was a one-time manual
            # grant that survived because dbt never dropped schemas. `reset`
            # recreates schemas and the split clusters are new, so the USAGE
            # grants must be reapplied. Database + the four mz-deploy schemas are
            # handled by the schema/database modifiers
            # (models/qa_canary_environment*.sql). What mz-deploy can't own is
            # granted here: the cluster (not a project object) and the two
            # testdrive-managed schemas (public_table/public_webhook).
            > GRANT USAGE ON CLUSTER qa_canary_environment_compute TO "infra+bot@materialize.com", "infra+qacanaryload@materialize.io"

            # Webhook source: CREATE SOURCE ... FROM WEBHOOK is unsupported by mz-deploy.
            > CREATE SCHEMA IF NOT EXISTS public_webhook
            > GRANT USAGE ON SCHEMA public_webhook TO "infra+bot@materialize.com", "infra+qacanaryload@materialize.io"
            > CREATE SOURCE IF NOT EXISTS public_webhook.webhook_source
              IN CLUSTER qa_canary_environment_storage
              FROM WEBHOOK
              BODY FORMAT JSON
            > GRANT SELECT ON public_webhook.webhook_source TO "infra+qacanaryload@materialize.io"

            # `table` domain: `table` is a reserved word (not mz-deploy-path-matchable)
            # and the canary load job inserts into public_table.table directly.
            > CREATE SCHEMA IF NOT EXISTS public_table
            > GRANT USAGE ON SCHEMA public_table TO "infra+bot@materialize.com", "infra+qacanaryload@materialize.io"
            > CREATE TABLE IF NOT EXISTS public_table.table (c INT)
            > GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE public_table.table TO "infra+qacanaryload@materialize.io"

            > CREATE MATERIALIZED VIEW IF NOT EXISTS public_table.table_mv
              IN CLUSTER qa_canary_environment_compute
              AS SELECT max(c) FROM public_table.table
            > CREATE INDEX IF NOT EXISTS table_mv_idx
              IN CLUSTER qa_canary_environment_compute
              ON public_table.table_mv (max)
            > GRANT ALL PRIVILEGES ON TABLE public_table.table_mv TO "infra+bot@materialize.com", "infra+qacanaryload@materialize.io"

            > CREATE SINK IF NOT EXISTS public_table.table_mv_sink
              IN CLUSTER qa_canary_environment_sinks
              FROM public_table.table_mv
              INTO KAFKA CONNECTION public.kafka_connection (TOPIC 'table_mv')
              FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION public.csr_connection
              ENVELOPE DEBEZIUM

            > CREATE SINK IF NOT EXISTS public_table.table_mv_iceberg_sink
              IN CLUSTER qa_canary_environment_sinks
              FROM public_table.table_mv
              INTO ICEBERG CATALOG CONNECTION public.qa_canary_iceberg_catalog (NAMESPACE = 'qa_canary_environment', TABLE = 'table_mv')
              USING AWS CONNECTION public.qa_canary_aws_connection
              MODE APPEND
              WITH (COMMIT INTERVAL = '60s')

            > CREATE SINK IF NOT EXISTS public_table.table_mv_gcs_iceberg_sink
              IN CLUSTER qa_canary_environment_sinks
              FROM public_table.table_mv
              INTO ICEBERG CATALOG CONNECTION public.qa_canary_gcs_iceberg_catalog (NAMESPACE = 'qa_canary_environment', TABLE = 'table_mv')
              MODE APPEND
              WITH (COMMIT INTERVAL = '60s')

            # Seed data for the loadgen product/category tables (created by `apply`).
            > DELETE FROM public_loadgen_sources.product_category
            > INSERT INTO public_loadgen_sources.product_category (category_id, category_name, category_description) VALUES
              (1, 'Soccer Balls', 'A variety of soccer balls for every type of play'),
              (2, 'Soccer Cleats', 'High-quality soccer cleats for all ages and skill levels'),
              (3, 'Goalkeeper Gear', 'Everything a goalkeeper needs to protect the net'),
              (4, 'Soccer Apparel', 'Soccer clothing for players and fans alike'),
              (5, 'Training Equipment', 'Tools and equipment to improve your soccer skills'),
              (6, 'Soccer Accessories', 'Accessories for every soccer player and fan'),
              (7, 'Referee Equipment', 'All the essentials for soccer referees'),
              (8, 'Soccer Books and Media', 'Books, DVDs, and online resources about soccer'),
              (9, 'Soccer Fan Gear', 'Show your team spirit with these fan favorites'),
              (10, 'Field Equipment', 'Everything you need to set up a soccer field')

            > DELETE FROM public_loadgen_sources.product
            > INSERT INTO public_loadgen_sources.product (product_id, category_id, product_name, product_description, product_price) VALUES
              (1, 1, 'Pro League Match Soccer Ball', 'Professional standard match soccer ball', 59.99),
              (2, 1, 'Junior Training Soccer Ball', 'Perfect for young players learning the game', 19.99),
              (3, 1, 'Indoor Futsal Ball', 'Designed for indoor soccer games', 29.99),
              (4, 1, 'Mini Skill Development Ball', 'Small ball for skill development', 14.99),
              (5, 1, 'Classic Black and White Soccer Ball', 'Classic design soccer ball', 24.99),
              (6, 2, 'Men''s High-Performance Soccer Cleats', 'Designed for speed and control on the field', 89.99),
              (7, 2, 'Women''s Comfort Fit Soccer Cleats', 'Combines performance with comfort', 79.99),
              (8, 3, 'Pro Goalkeeper Gloves', 'Professional standard goalkeeper gloves', 59.99),
              (9, 4, 'Men''s Soccer Jersey', 'Comfortable and breathable soccer jersey for men', 39.99),
              (10, 5, 'Speed and Agility Ladder', 'Improve your speed and agility with this training ladder', 29.99)
            """))

    # ── 5. mz-deploy: deploy views/MVs blue-green ─────────────────────────
    # `--allow-dirty` because mzcompose rewrote project.toml/profiles.toml.
    #
    # `wait` is best-effort (check=False): a from-scratch hydration of the
    # canary (TPC-H scale factor 1, plus re-reading the Kafka loadgen topic
    # backlog) can take far longer than any practical timeout. The original dbt
    # canary never waited at all — `dbt run` created the objects and let them
    # hydrate asynchronously in production — so we give hydration a bounded
    # window for visibility, then promote regardless. `--no-ready-check` lets
    # promote proceed without re-gating on hydration; the MVs finish hydrating
    # in production and the `test` workflow checks their progress separately.
    did = deploy_id()
    mz_deploy(c, "stage", "--deploy-id", did, "--allow-dirty")
    if args.wait_timeout != "0":
        mz_deploy(
            c,
            "wait",
            did,
            "--timeout",
            args.wait_timeout,
            "--allowed-lag",
            "86400",
            check=False,
        )
    mz_deploy(c, "promote", did, "--no-ready-check")


def _materialize_conn(dbname: str = "qa_canary_environment") -> psycopg.Connection:
    return psycopg.connect(
        host=MATERIALIZE_PROD_SANDBOX_HOSTNAME,
        port=6875,
        user=MATERIALIZE_PROD_SANDBOX_USERNAME,
        password=MATERIALIZE_PROD_SANDBOX_APP_PASSWORD,
        dbname=dbname,
        sslmode="require",
        autocommit=True,
    )


def _check_rds_slots() -> list[str]:
    """Guard: RDS Postgres should have fewer than 2 replication slots."""
    con = psycopg.connect(
        host=MATERIALIZE_PROD_SANDBOX_RDS_HOSTNAME,
        user="postgres",
        dbname="postgres",
        password=MATERIALIZE_PROD_SANDBOX_RDS_PASSWORD,
        sslmode="require",
    )
    try:
        with con.cursor() as cur:
            cur.execute("SELECT slot_name, active FROM pg_replication_slots")
            slots = cur.fetchall()
    finally:
        con.close()
    if len(slots) >= 2:
        return [
            f"RDS Postgres has {len(slots)} replication slots (expected < 2): "
            f"{[name for name, _ in slots]} — drop the stale one(s) manually so "
            "Postgres doesn't run out of disk (see README)"
        ]
    return []


def _check_sources_running() -> list[str]:
    """Every canary source is running (or in an expected transient state)."""
    mz = _materialize_conn()
    try:
        with mz.cursor() as cur:
            cur.execute("""
                SELECT s.name, st.status, st.error
                FROM mz_internal.mz_source_statuses st
                JOIN mz_sources s USING (id)
                JOIN mz_schemas sc ON (s.schema_id = sc.id)
                JOIN mz_databases db ON (sc.database_id = db.id)
                WHERE db.name = 'qa_canary_environment'
                AND NOT (
                    st.status = 'running'
                    OR (s.type = 'progress' AND st.status = 'created')
                    OR (s.type = 'subsource' AND st.status = 'starting')
                )
                ORDER BY s.name
                """)
            return [
                f"source {name} not running: status={status} error={error}"
                for name, status, error in cur.fetchall()
            ]
    finally:
        mz.close()


def _check_replicas_online() -> list[str]:
    """Every storage/compute cluster replica is online."""
    mz = _materialize_conn()
    try:
        with mz.cursor() as cur:
            cur.execute("""
                SELECT c.name, cr.name, rs.status, rs.reason
                FROM mz_internal.mz_cluster_replica_statuses rs
                JOIN mz_cluster_replicas cr ON (cr.id = rs.replica_id)
                JOIN mz_clusters c ON (c.id = cr.cluster_id)
                WHERE rs.status != 'online'
                AND c.name IN ('qa_canary_environment_storage', 'qa_canary_environment_compute')
                ORDER BY c.name, cr.name
                """)
            return [
                f"replica {cluster}.{replica} not online: status={status} reason={reason}"
                for cluster, replica, status, reason in cur.fetchall()
            ]
    finally:
        mz.close()


def _check_frontiers() -> list[str]:
    """Compute frontiers (and their imports) are within a minute of now.

    mz_introspection is per-replica, so query it on the compute cluster. Query
    introspection ALONE (with mz_now()) like the original dbt test — joining
    per-replica introspection to a catalog relation under mz_now() makes the
    peek wait for frontiers to align and hangs — then resolve export_id -> name
    with a separate catalog lookup.
    """
    mz = _materialize_conn()
    out: list[str] = []
    try:
        with mz.cursor() as cur:
            cur.execute("SET cluster = qa_canary_environment_compute")
            for relation in ("mz_compute_frontiers", "mz_compute_import_frontiers"):
                cur.execute(
                    sql.SQL(
                        "SELECT DISTINCT export_id "
                        "FROM mz_introspection.{relation} "
                        "WHERE to_timestamp(time::text::bigint / 1000) "
                        "< to_timestamp(mz_now()::text::bigint / 1000) - INTERVAL '1 minute'"
                    ).format(relation=sql.Identifier(relation))
                )
                lagging_ids = [export_id for (export_id,) in cur.fetchall()]
                for export_id in lagging_ids:
                    cur.execute(
                        "SELECT name FROM mz_objects WHERE id = %s", (export_id,)
                    )
                    row = cur.fetchone()
                    # A transient id (e.g. `t11184`) is a dataflow with no catalog
                    # entry — typically one of the SUBSCRIBE "makes progress"
                    # checks running concurrently on this same cluster. Those
                    # aren't canary objects, so skip them; report only objects
                    # that resolve to a name.
                    if row is None:
                        continue
                    out.append(f"{relation}: {row[0]} lagging more than 1 minute")
    finally:
        mz.close()
    return out


def workflow_test(c: Composition, parser: WorkflowArgumentParser) -> None:
    # These replace the former `dbt test` data tests: every source is running,
    # every replica is online, compute frontiers (and their imports) keep up,
    # and every monitored object's write frontier keeps advancing (i.e. it is
    # still sealing new output times — making progress). Unlike the dbt tests
    # (which only checked the result was empty), each check reports the specific
    # failing objects.
    #
    # The checks are independent (each opens its own connection), so they run
    # concurrently and report as each finishes via run_parallel — the same
    # spinner/✓/✗ output as bin/lint.
    #
    # Progress is measured by a write-frontier advance, NOT by a
    # SUBSCRIBE(SNAPSHOT=false) emitting a row. A subscribe is the wrong probe
    # for the big UPSERT source-tables: it emits nothing for a live source whose
    # content happens not to change (customer re-sends no-op upserts), and it can
    # take minutes just to hydrate a fresh dataflow over a ~1B-row persist shard
    # — both false timeouts. A frozen write frontier, by contrast, is exactly
    # "not making progress", and the sample is one cheap catalog query.
    parser.add_argument(
        "--progress-window",
        default=15.0,
        type=float,
        help="Seconds to watch each object's write frontier; it must advance within this window.",
    )
    args = parser.parse_args()

    # `stop` lets the frontier checks bail out promptly on Ctrl-C: signals go to
    # the main thread, so the workers can't see the KeyboardInterrupt — they poll
    # this event while waiting out the sample window.
    stop = threading.Event()

    def task(fn: Callable[[], list[str]]) -> Callable[[], tuple[bool, str]]:
        # Adapt a check (returns the list of failures) to run_parallel's
        # (success, output) contract; the output is shown only on failure.
        def run() -> tuple[bool, str]:
            fails = fn()
            return not fails, "\n".join(fails)

        return run

    checks: list[tuple[str, TaskSpec]] = [
        ("RDS Postgres replication-slot count", task(_check_rds_slots)),
        ("all canary sources running", task(_check_sources_running)),
        ("all canary cluster replicas online", task(_check_replicas_online)),
        ("compute frontiers within 1 minute of now", task(_check_frontiers)),
    ]
    for obj in MONITORED_MVS + MONITORED_SOURCE_TABLES:
        # Label with just the object name (the db/schema is always
        # qa_canary_environment.public_*); the check uses the FQN.
        checks.append(
            (
                f"{obj.split('.')[-1]} makes progress",
                task(
                    lambda obj=obj: _frontier_advances(obj, args.progress_window, stop)
                ),
            )
        )

    log(f"running {len(checks)} canary checks in parallel")
    try:
        failed = run_parallel(
            checks, spinner_suffix="canary checks", print_summary=False
        )
    except KeyboardInterrupt:
        stop.set()
        log("interrupted — exiting")
        raise SystemExit(130) from None

    if failed:
        raise AssertionError(f"{len(failed)} canary check(s) failed (see above)")
    log("all checks passed")


def _frontier_advances(obj: str, window: float, stop: threading.Event) -> list[str]:
    """Return failures if `obj`'s write frontier doesn't advance within `window`.

    A live source/MV continuously seals new output times, so its write frontier
    tracks wall-clock (measured ~1s per 1s, ~1.2s behind now); a frozen write
    frontier means the object isn't making progress.

    This replaces a SUBSCRIBE(SNAPSHOT = false) probe, which gives false
    timeouts on the big UPSERT source-tables: it emits nothing for a live source
    whose content doesn't change (customer re-sends no-op upserts), and it can
    take minutes just to hydrate a fresh dataflow over a ~1B-row persist shard.
    The write frontier is one cheap catalog query and means exactly "progress".
    """
    db, schema, name = obj.split(".")

    def write_frontier(cur) -> int | None:
        cur.execute(
            """
            SELECT f.write_frontier
            FROM mz_internal.mz_frontiers f
            JOIN mz_objects o ON o.id = f.object_id
            JOIN mz_schemas sc ON o.schema_id = sc.id
            JOIN mz_databases d ON sc.database_id = d.id
            WHERE d.name = %s AND sc.name = %s AND o.name = %s
            """,
            (db, schema, name),
        )
        row = cur.fetchone()
        if row is None or row[0] is None:
            return None
        return int(row[0])

    mz = _materialize_conn()
    try:
        with mz.cursor() as cur:
            before = write_frontier(cur)
            if before is None:
                return [
                    f"{obj}: no advancing write frontier (missing or already complete)"
                ]
            # Interruptible wait so Ctrl-C (which sets `stop`) returns in ~1s.
            deadline = time.monotonic() + window
            while not stop.is_set():
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                time.sleep(min(1.0, remaining))
            after = write_frontier(cur)
    finally:
        mz.close()
    if after is None or after <= before:
        return [
            f"{obj}: write frontier not advancing "
            f"({before} -> {after}) over {int(window)}s"
        ]
    return []


def workflow_reset(c: Composition, parser: WorkflowArgumentParser) -> None:
    """Drop the canary objects so `create` can rebuild from scratch.

    DESTRUCTIVE. Drops every per-domain schema of ``qa_canary_environment``
    (sources, source-tables, MVs, sinks, and the testdrive-managed
    ``public_table`` / ``public_webhook`` objects), the ``_mz_deploy``
    deployment metadata, and any orphaned ``*_<deploy-id>`` staging clusters
    left by a failed deploy.

    The ``public`` schema — connections and secrets — is deliberately KEPT, and
    so are the four base clusters (``…_storage`` / ``…_upsert`` / ``…_sinks`` /
    ``…_compute``). This is load-bearing for the AWS
    connection: Materialize derives an AWS connection's external ID from the
    connection's id (``mz_<prefix>_<connection_id>``), so dropping and
    recreating ``qa_canary_aws_connection`` rotates the external ID and breaks
    the ``qa-canary-environment-iceberg-role`` trust policy, which is pinned to
    one external ID. Keeping ``public`` lets `apply` adopt the existing
    connections (UpToDate/ALTER, never DROP+CREATE), preserving the external ID.

    Dropping the Postgres/MySQL sources releases their RDS replication slots, so
    a subsequent `create` starts clean (the `test` workflow guards
    ``pg_replication_slots < 2``).

    Run `create` afterwards to rebuild:

        ./mzcompose run reset
        ./mzcompose run create
    """
    # Connect to the default `materialize` database so this works even if
    # qa_canary_environment doesn't exist yet.
    con = _materialize_conn(dbname="materialize")
    try:
        with con.cursor() as cur:
            # Orphaned staging clusters from a previous failed deploy. The base
            # clusters have no `_<deploy-id>` suffix and are excluded.
            cur.execute(r"""
                SELECT name FROM mz_clusters
                WHERE (
                    name LIKE 'qa\_canary\_environment\_compute\_%' ESCAPE '\'
                    OR name LIKE 'qa\_canary\_environment\_storage\_%' ESCAPE '\'
                )
                """)
            for (name,) in cur.fetchall():
                print(f"dropping orphaned staging cluster {name}")
                cur.execute(
                    sql.SQL("DROP CLUSTER IF EXISTS {} CASCADE").format(
                        sql.Identifier(name)
                    )
                )

            # Every user schema except `public` (connections/secrets are kept so
            # the AWS connection's external ID survives). System schemas are not
            # tied to the user database, so they don't appear here.
            cur.execute("""
                SELECT s.name FROM mz_schemas s
                JOIN mz_databases d ON s.database_id = d.id
                WHERE d.name = 'qa_canary_environment' AND s.name <> 'public'
                """)
            for (schema,) in cur.fetchall():
                print(f"dropping schema qa_canary_environment.{schema}")
                cur.execute(
                    sql.SQL("DROP SCHEMA {}.{} CASCADE").format(
                        sql.Identifier("qa_canary_environment"),
                        sql.Identifier(schema),
                    )
                )

            print("dropping database _mz_deploy")
            cur.execute("DROP DATABASE IF EXISTS _mz_deploy CASCADE")
    finally:
        con.close()


def workflow_mz_deploy(c: Composition, parser: WorkflowArgumentParser) -> None:
    """Run an arbitrary mz-deploy command against the prod sandbox, e.g.:

        ./mzcompose run mz-deploy list
        ./mzcompose run mz-deploy promote <deploy-id> --no-ready-check
        ./mzcompose run mz-deploy abort <deploy-id>
        ./mzcompose run mz-deploy wait <deploy-id> --timeout 600

    This workflow is intentionally named after the `mz-deploy` service so that
    `./mzcompose run mz-deploy …` resolves to it (workflows shadow same-named
    services). Running the service directly — `./mzcompose run` against a name
    that is NOT a workflow — execs the bare binary without the
    `-d /workdir --profiles-dir /workdir --profile prod` wiring (and without
    writing profiles.toml), which fails with a missing-profiles error. This
    workflow adds that wiring, same as the others.
    """
    parser.add_argument("mz_args", nargs=argparse.REMAINDER)
    cli_args = parser.parse_args()
    if not cli_args.mz_args:
        raise ValueError("usage: ./mzcompose run mz <mz-deploy command> [args...]")
    write_profiles()
    write_project_toml()
    mz_deploy(c, *cli_args.mz_args)


def workflow_clean(c: Composition, parser: WorkflowArgumentParser) -> None:
    write_profiles()
    write_project_toml()
    mz_deploy(c, "clean")
