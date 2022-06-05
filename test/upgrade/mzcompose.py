# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Verifies that objects created in previous versions of Materialize are still
operational after an upgrade.
"""

import random
from typing import Dict, List, Tuple

from semver import Version

from materialize import util
from materialize.mzcompose import Composition, WorkflowArgumentParser
from materialize.mzcompose.services import (
    Kafka,
    Materialized,
    Postgres,
    SchemaRegistry,
    TestCerts,
    Testdrive,
    Zookeeper,
)

# All released Materialize versions, in order from most to least recent.
all_versions = util.known_materialize_versions()

mz_options: Dict[Version, str] = {}

SERVICES = [
    TestCerts(),
    Zookeeper(),
    Kafka(
        # for some reason docker-compose wants kafka to be setup
        # with the same volumes when overriden
        depends_on=["zookeeper", "test-certs"],
        volumes=["secrets:/etc/kafka/secrets"],
    ),
    SchemaRegistry(
        depends_on=["kafka", "zookeeper", "test-certs"],
        volumes=[
            "secrets:/etc/schema-registry/secrets",
        ],
    ),
    Postgres(),
    Materialized(
        options=" ".join(mz_options.values()),
        environment_extra=[
            "SSL_KEY_PASSWORD=mzmzmz",
        ],
        volumes_extra=["secrets:/share/secrets"],
    ),
    # N.B.: we need to use `validate_data_dir=False` because testdrive uses
    # HEAD to load the catalog from disk but does *not* run migrations. There
    # is no guarantee that HEAD can load an old catalog without running
    # migrations.
    #
    # When testdrive is targeting a HEAD materialized, we re-enable catalog
    # validation below by manually passing the `--validate-data-dir` flag.
    #
    # Disabling catalog validation is preferable to using a versioned testdrive
    # because that would involve maintaining backwards compatibility for all
    # testdrive commands.
    Testdrive(
        validate_data_dir=False,
        volumes_extra=["secrets:/share/secrets"],
    ),
]


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    """Test upgrades from various versions."""

    parser.add_argument(
        "--min-version",
        metavar="VERSION",
        type=Version.parse,
        default=Version.parse("0.27.0"),
        help="the minimum version to test from",
    )
    parser.add_argument(
        "--most-recent",
        metavar="N",
        type=int,
        help="limit testing to the N most recent versions",
    )
    parser.add_argument(
        "--tests",
        choices=["all", "non-ssl", "ssl"],
        default="all",
        help="limit testing to certain scenarios",
    )
    parser.add_argument(
        "filter", nargs="?", default="*", help="limit to only the files matching filter"
    )
    args = parser.parse_args()

    tested_versions = [v for v in all_versions if v >= args.min_version]
    if args.most_recent is not None:
        tested_versions = tested_versions[: args.most_recent]
    tested_versions.reverse()

    c.start_and_wait_for_tcp(
        services=["zookeeper", "kafka", "schema-registry", "postgres"]
    )

    if args.tests in ["all", "non-ssl"]:
        for version in tested_versions:
            priors = [f"v{v}" for v in all_versions if v < version]
            test_upgrade_from_version(c, f"v{version}", priors, filter=args.filter)

        test_upgrade_from_version(c, "current_source", priors=["*"], filter=args.filter)

    if args.tests in ["all", "ssl"]:
        kafka, schema_registry, testdrive = ssl_services()
        with c.override(kafka, schema_registry, testdrive):
            c.start_and_wait_for_tcp(services=["kafka", "schema-registry"])
            for version in tested_versions:
                priors = [f"v{v}" for v in all_versions if v < version]
                test_upgrade_from_version(
                    c, f"v{version}", priors, filter=args.filter, style="ssl-"
                )
            # we don't test current_source -> current_source for `ssl-` tests


def test_upgrade_from_version(
    c: Composition, from_version: str, priors: List[str], filter: str, style: str = ""
) -> None:
    print(f"===>>> Testing upgrade from Materialize {from_version} to current_source.")

    version_glob = "{" + ",".join(["any_version", *priors, from_version]) + "}"
    print(">>> Version glob pattern: " + version_glob)

    c.rm("materialized", "testdrive", stop=True)
    c.rm_volumes("mzdata", "pgdata", "tmp")

    if from_version != "current_source":
        mz_from = Materialized(
            image=f"materialize/materialized:{from_version}",
            options=" ".join(
                opt
                for start_version, opt in mz_options.items()
                if from_version[1:] >= start_version
            ),
            environment_extra=[
                "SSL_KEY_PASSWORD=mzmzmz",
            ],
            volumes_extra=["secrets:/share/secrets"],
        )
        with c.override(mz_from):
            c.up("materialized")
    else:
        c.up("materialized")

    c.wait_for_materialized("materialized")

    temp_dir = f"--temp-dir=/share/tmp/upgrade-from-{from_version}"
    seed = f"--seed={random.getrandbits(32)}"
    c.run(
        "testdrive",
        "--no-reset",
        f"--var=upgrade-from-version={from_version}",
        temp_dir,
        seed,
        f"create-{style}in-{version_glob}-{filter}.td",
    )

    c.kill("materialized")
    c.rm("materialized", "testdrive")

    c.up("materialized")
    c.wait_for_materialized("materialized")

    c.run(
        "testdrive",
        "--no-reset",
        f"--var=upgrade-from-version={from_version}",
        temp_dir,
        seed,
        "--validate-data-dir=/mzdata",
        f"check-{style}from-{version_glob}-{filter}.td",
    )


def ssl_services() -> Tuple[Kafka, SchemaRegistry, Testdrive]:
    """sets"""

    kafka = Kafka(
        depends_on=["zookeeper", "test-certs"],
        environment=[
            # Default
            "KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181",
            "KAFKA_CONFLUENT_SUPPORT_METRICS_ENABLE=false",
            "KAFKA_MIN_INSYNC_REPLICAS=1",
            "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1",
            "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1",
            "KAFKA_MESSAGE_MAX_BYTES=15728640",
            "KAFKA_REPLICA_FETCH_MAX_BYTES=15728640",
            # For this test
            "KAFKA_SSL_KEYSTORE_FILENAME=kafka.keystore.jks",
            "KAFKA_SSL_KEYSTORE_CREDENTIALS=cert_creds",
            "KAFKA_SSL_KEY_CREDENTIALS=cert_creds",
            "KAFKA_SSL_TRUSTSTORE_FILENAME=kafka.truststore.jks",
            "KAFKA_SSL_TRUSTSTORE_CREDENTIALS=cert_creds",
            "KAFKA_SSL_CLIENT_AUTH=required",
            "KAFKA_SECURITY_INTER_BROKER_PROTOCOL=SSL",
        ],
        listener_type="SSL",
        volumes=["secrets:/etc/kafka/secrets"],
    )
    schema_registry = SchemaRegistry(
        depends_on=["kafka", "zookeeper", "test-certs"],
        environment=[
            "SCHEMA_REGISTRY_KAFKASTORE_TIMEOUT_MS=10000",
            "SCHEMA_REGISTRY_HOST_NAME=schema-registry",
            "SCHEMA_REGISTRY_LISTENERS=https://0.0.0.0:8081",
            "SCHEMA_REGISTRY_KAFKASTORE_CONNECTION_URL=zookeeper:2181",
            "SCHEMA_REGISTRY_KAFKASTORE_SECURITY_PROTOCOL=SSL",
            "SCHEMA_REGISTRY_KAFKASTORE_SSL_KEYSTORE_LOCATION=/etc/schema-registry/secrets/schema-registry.keystore.jks",
            "SCHEMA_REGISTRY_SSL_KEYSTORE_LOCATION=/etc/schema-registry/secrets/schema-registry.keystore.jks",
            "SCHEMA_REGISTRY_KAFKASTORE_SSL_KEYSTORE_PASSWORD=mzmzmz",
            "SCHEMA_REGISTRY_SSL_KEYSTORE_PASSWORD=mzmzmz",
            "SCHEMA_REGISTRY_KAFKASTORE_SSL_KEY_PASSWORD=mzmzmz",
            "SCHEMA_REGISTRY_SSL_KEY_PASSWORD=mzmzmz",
            "SCHEMA_REGISTRY_KAFKASTORE_SSL_TRUSTSTORE_LOCATION=/etc/schema-registry/secrets/schema-registry.truststore.jks",
            "SCHEMA_REGISTRY_SSL_TRUSTSTORE_LOCATION=/etc/schema-registry/secrets/schema-registry.truststore.jks",
            "SCHEMA_REGISTRY_KAFKASTORE_SSL_TRUSTSTORE_PASSWORD=mzmzmz",
            "SCHEMA_REGISTRY_SSL_TRUSTSTORE_PASSWORD=mzmzmz",
            "SCHEMA_REGISTRY_SCHEMA_REGISTRY_INTER_INSTANCE_PROTOCOL=https",
            "SCHEMA_REGISTRY_SSL_CLIENT_AUTH=true",
        ],
        volumes=[
            "secrets:/etc/schema-registry/secrets",
        ],
        bootstrap_server_type="SSL",
    )
    testdrive = Testdrive(
        entrypoint=[
            "bash",
            "-c",
            "cp /share/secrets/ca.crt /usr/local/share/ca-certificates/ca.crt && "
            "update-ca-certificates && "
            "testdrive "
            "--kafka-addr=kafka:9092 "
            "--schema-registry-url=https://schema-registry:8081 "
            "--materialized-url=postgres://materialize@materialized:6875 "
            "--cert=/share/secrets/producer.p12 "
            "--cert-password=mzmzmz "
            "--ccsr-password=sekurity "
            "--ccsr-username=materialize "
            '"$$@"',
        ],
        volumes_extra=["secrets:/share/secrets"],
        # Required to install root certs above
        propagate_uid_gid=False,
        validate_data_dir=False,
    )

    return (kafka, schema_registry, testdrive)
