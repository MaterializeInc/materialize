# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import json
import random
import subprocess
import time
from textwrap import dedent

import pytest
import requests
from pg8000.dbapi import DatabaseError, ProgrammingError

from materialize.cloudtest import DEFAULT_K8S_NAMESPACE
from materialize.cloudtest.app.materialize_application import MaterializeApplication
from materialize.cloudtest.k8s.toxiproxy import (
    PrivateLinkExternalNameService,
    ToxiproxyDeployment,
    ToxiproxyService,
)
from materialize.cloudtest.util.common import retry
from materialize.cloudtest.util.exists import exists, not_exists
from materialize.cloudtest.util.wait import wait
from materialize.ui import UIError


def test_create_privatelink_connection(mz: MaterializeApplication) -> None:
    # Create a PrivateLink SQL connection object,
    # which should create a K8S VpcEndpoint object.
    # We don't run the environment-controller,
    # so no AWS VPC Endpoint will be created.
    # so we don't need the named service to actually exist.
    create_connection_statement = dedent("""\
        CREATE CONNECTION privatelinkconn
        TO AWS PRIVATELINK (
            SERVICE NAME 'com.amazonaws.vpce.us-east-1.vpce-svc-0e123abc123198abc',
            AVAILABILITY ZONES ('use1-az1', 'use1-az2')
        )
        """)

    # This should fail until max_aws_privatelink_connections is increased.
    with pytest.raises(
        ProgrammingError,
        match="creating AWS PrivateLink Connection would violate max_aws_privatelink_connections limit",
    ):
        mz.environmentd.sql(create_connection_statement)

    next_gid = mz.environmentd.sql_query(
        "SELECT MAX(SUBSTR(id, 2, LENGTH(id) - 1)::int) + 1 FROM mz_objects WHERE id LIKE 'u%'"
    )[0][0]

    not_exists(resource=f"vpcendpoint/connection-u{next_gid}")

    mz.environmentd.sql(
        "ALTER SYSTEM SET max_aws_privatelink_connections = 5",
        port="internal",
        user="mz_system",
    )
    mz.environmentd.sql(create_connection_statement)

    aws_connection_id = mz.environmentd.sql_query(
        "SELECT id FROM mz_connections WHERE name = 'privatelinkconn'"
    )[0][0]

    exists(resource=f"vpcendpoint/connection-{aws_connection_id}")

    # Less flaky if we sleep before checking the status
    time.sleep(5)

    assert (
        "unknown"
        == mz.environmentd.sql_query(
            f"SELECT status FROM mz_internal.mz_aws_privatelink_connection_status_history WHERE connection_id = '{aws_connection_id}'"
        )[0][0]
    )

    # TODO: validate the contents of the VPC endpoint resource, rather than just
    # its existence.

    mz.environmentd.sql(
        "ALTER SYSTEM SET enable_connection_validation_syntax = true",
        port="internal",
        user="mz_system",
    )
    mz.environmentd.sql(dedent("""\
            CREATE CONNECTION kafkaconn TO KAFKA (
                BROKERS (
                    'customer-hostname-1:9092' USING AWS PRIVATELINK privatelinkconn,
                    'customer-hostname-2:9092' USING AWS PRIVATELINK privatelinkconn (PORT 9093),
                    'customer-hostname-3:9092' USING AWS PRIVATELINK privatelinkconn (AVAILABILITY ZONE 'use1-az1', PORT 9093),
                    'customer-hostname-4:9094'
                ),
                SECURITY PROTOCOL PLAINTEXT
            ) WITH (VALIDATE = false);
            """))
    mz.environmentd.sql_query("SELECT id FROM mz_connections WHERE name = 'kafkaconn'")[
        0
    ][0]

    principal = mz.environmentd.sql_query(
        "SELECT principal FROM mz_aws_privatelink_connections"
    )[0][0]

    assert principal == (
        f"arn:aws:iam::123456789000:role/mz_eb5cb59b-e2fe-41f3-87ca-d2176a495345_{aws_connection_id}"
    )

    # Validate default privatelink connections for kafka
    mz.environmentd.sql(dedent("""\
            CREATE CONNECTION kafkaconn_alt TO KAFKA (
                AWS PRIVATELINK privatelinkconn (PORT 9092),
                SECURITY PROTOCOL PLAINTEXT
            ) WITH (VALIDATE = false);
            """))
    mz.environmentd.sql_query(
        "SELECT id FROM mz_connections WHERE name = 'kafkaconn_alt'"
    )[0][0]

    mz.environmentd.sql(dedent("""\
            CREATE CONNECTION sshconn TO SSH TUNNEL (
                HOST 'ssh-bastion-host',
                USER 'mz',
                PORT 22
            );
            """))
    with pytest.raises(
        ProgrammingError, match="cannot specify both SSH TUNNEL and AWS PRIVATELINK"
    ):
        mz.environmentd.sql(dedent("""\
            CREATE CONNECTION pg TO POSTGRES (
                HOST 'postgres',
                DATABASE postgres,
                USER postgres,
                AWS PRIVATELINK privatelinkconn,
                SSH TUNNEL sshconn
            ) WITH (VALIDATE = false);
            """))

    with pytest.raises(
        ProgrammingError, match='invalid AWS PrivateLink availability zone "us-east-1a"'
    ):
        mz.environmentd.sql(dedent("""\
                CREATE CONNECTION privatelinkconn2
                TO AWS PRIVATELINK (
                SERVICE NAME 'com.amazonaws.vpce.us-east-1.vpce-svc-0e123abc123198abc',
                AVAILABILITY ZONES ('use1-az2', 'us-east-1a')
                );
                """))

    with pytest.raises(
        ProgrammingError,
        match="connection cannot contain duplicate availability zones",
    ):
        mz.environmentd.sql(dedent("""\
                CREATE CONNECTION privatelinkconn2
                TO AWS PRIVATELINK (
                SERVICE NAME 'com.amazonaws.vpce.us-east-1.vpce-svc-0e123abc123198abc',
                AVAILABILITY ZONES ('use1-az1', 'use1-az1', 'use1-az2')
                );
                """))

    with pytest.raises(
        ProgrammingError,
        match='AWS PrivateLink availability zone "use1-az3" does not match any of the availability zones on the AWS PrivateLink connection',
    ):
        mz.environmentd.sql(dedent("""\
                CREATE CONNECTION kafkaconn2 TO KAFKA (
                    BROKERS (
                        'customer-hostname-3:9092' USING AWS PRIVATELINK privatelinkconn (AVAILABILITY ZONE 'use1-az3', PORT 9093)
                    ),
                    SECURITY PROTOCOL PLAINTEXT
                ) WITH (VALIDATE = false);
                """))

    with pytest.raises(
        DatabaseError,
        match="invalid CONNECTION: can only set one of BROKER, BROKERS, or AWS PRIVATELINK",
    ):
        mz.environmentd.sql(dedent("""\
                CREATE CONNECTION kafkaconn2_alt TO KAFKA (
                    AWS PRIVATELINK privatelinkconn (PORT 9092),
                    BROKERS (
                        'customer-hostname-3:9092' USING AWS PRIVATELINK privatelinkconn (PORT 9093)
                    ),
                    SECURITY PROTOCOL PLAINTEXT
                ) WITH (VALIDATE = false);
                """))
    with pytest.raises(
        ProgrammingError,
        match="invalid CONNECTION: POSTGRES does not support PORT for AWS PRIVATELINK",
    ):
        mz.environmentd.sql(dedent("""\
            CREATE CONNECTION pg TO POSTGRES (
                HOST 'postgres',
                DATABASE postgres,
                USER postgres,
                AWS PRIVATELINK privatelinkconn ( PORT 1234 ),
                PORT 1234
            ) WITH (VALIDATE = false);
            """))

    mz.environmentd.sql("DROP CONNECTION kafkaconn CASCADE")
    mz.environmentd.sql("DROP CONNECTION privatelinkconn CASCADE")

    not_exists(resource=f"vpcendpoint/connection-{aws_connection_id}")


def test_background_drop_privatelink_connection(mz: MaterializeApplication) -> None:
    # Ensure that privatelink connections are
    # deleted in a background task
    mz.environmentd.sql(
        "ALTER SYSTEM SET max_aws_privatelink_connections = 5",
        port="internal",
        user="mz_system",
    )
    create_connection_statement = dedent("""\
        CREATE CONNECTION privatelinkconn
        TO AWS PRIVATELINK (
            SERVICE NAME 'com.amazonaws.vpce.us-east-1.vpce-svc-0e123abc123198abc',
            AVAILABILITY ZONES ('use1-az1', 'use1-az2')
        )
        """)
    mz.environmentd.sql(create_connection_statement)
    aws_connection_id = mz.environmentd.sql_query(
        "SELECT id FROM mz_connections WHERE name = 'privatelinkconn'"
    )[0][0]
    mz.environmentd.sql("SET FAILPOINTS = 'drop_vpc_endpoint=pause'")
    mz.environmentd.sql("DROP CONNECTION privatelinkconn CASCADE")
    exists(resource=f"vpcendpoint/connection-{aws_connection_id}")
    mz.environmentd.sql("SET FAILPOINTS = 'drop_vpc_endpoint=off'")
    not_exists(resource=f"vpcendpoint/connection-{aws_connection_id}")


def test_retry_drop_privatelink_connection(mz: MaterializeApplication) -> None:
    # Ensure that privatelink connections are
    # deleted in a background task
    mz.environmentd.sql(
        "ALTER SYSTEM SET max_aws_privatelink_connections = 5",
        port="internal",
        user="mz_system",
    )
    create_connection_statement = dedent("""\
        CREATE CONNECTION privatelinkconn
        TO AWS PRIVATELINK (
            SERVICE NAME 'com.amazonaws.vpce.us-east-1.vpce-svc-0e123abc123198abc',
            AVAILABILITY ZONES ('use1-az1', 'use1-az2')
        )
        """)
    mz.environmentd.sql(create_connection_statement)
    aws_connection_id = mz.environmentd.sql_query(
        "SELECT id FROM mz_connections WHERE name = 'privatelinkconn'"
    )[0][0]
    mz.environmentd.sql("SET FAILPOINTS = 'drop_vpc_endpoint=return(failed)'")
    mz.environmentd.sql("DROP CONNECTION privatelinkconn CASCADE")
    exists(resource=f"vpcendpoint/connection-{aws_connection_id}")
    mz.environmentd.sql("SET FAILPOINTS = 'drop_vpc_endpoint=off'")
    retry(
        f=lambda: not_exists(resource=f"vpcendpoint/connection-{aws_connection_id}"),
        max_attempts=10,
        exception_types=[UIError],
    )


def test_privatelink_e2e_connectivity(mz: MaterializeApplication) -> None:
    """
    End-to-end test validating PrivateLink connectivity via Toxiproxy.

    Uses a single exact-match rule for bootstrap and routing. Standard Redpanda
    (unpatched) advertises the same address it bootstraps from, so a single
    rule suffices. This test validates:
    1. Connectivity through the PrivateLink simulation (Toxiproxy)
    2. Source goes stalled when the proxy is disabled
    3. Source recovers when the proxy is re-enabled

    Architecture:
        Materialize --> ExternalName (connection-{id}) --> Toxiproxy --> Redpanda
    """
    namespace = DEFAULT_K8S_NAMESPACE

    # Track resources for cleanup
    toxiproxy_deployment = None
    toxiproxy_service = None
    privatelink_svc: PrivateLinkExternalNameService | None = None

    # Step 1: Deploy a single Toxiproxy instance
    toxiproxy_deployment = ToxiproxyDeployment(namespace, name="toxiproxy-e2e")
    toxiproxy_service = ToxiproxyService(namespace, name="toxiproxy-e2e")
    toxiproxy_deployment.create()
    toxiproxy_service.create()

    try:
        wait(condition="condition=Available", resource="deployment/toxiproxy-e2e")

        # Step 2: Enable PrivateLink connections and create connection
        mz.environmentd.sql(
            "ALTER SYSTEM SET max_aws_privatelink_connections = 5",
            port="internal",
            user="mz_system",
        )
        mz.environmentd.sql(
            "ALTER SYSTEM SET enable_connection_validation_syntax = true",
            port="internal",
            user="mz_system",
        )
        mz.environmentd.sql(
            "ALTER SYSTEM SET enable_kafka_broker_matching_rules = true",
            port="internal",
            user="mz_system",
        )

        mz.environmentd.sql(dedent("""\
                CREATE CONNECTION privatelink_e2e_conn
                TO AWS PRIVATELINK (
                    SERVICE NAME 'com.amazonaws.vpce.test.vpce-svc-e2e-test',
                    AVAILABILITY ZONES ('use1-az1')
                )
                """))

        connection_id = mz.environmentd.sql_query(
            "SELECT id FROM mz_connections WHERE name = 'privatelink_e2e_conn'"
        )[0][0]

        # Step 3: Verify VpcEndpoint resource exists
        exists(resource=f"vpcendpoint/connection-{connection_id}")

        # Step 4: Create ExternalName service to simulate VpcEndpoint controller
        privatelink_svc = PrivateLinkExternalNameService(
            connection_id=connection_id,
            target_service=f"toxiproxy-e2e.{namespace}.svc.cluster.local",
            namespace=namespace,
            availability_zone=None,
        )
        privatelink_svc.create()

        # Step 5: Configure Toxiproxy to proxy to Redpanda
        admin_port = toxiproxy_service.node_port("admin")
        requests.post(
            f"http://localhost:{admin_port}/proxies",
            json={
                "name": "kafka",
                "listen": "0.0.0.0:9092",
                "upstream": f"redpanda.{namespace}.svc.cluster.local:9092",
                "enabled": True,
            },
        )

        # Step 6: Create Kafka connection with a static broker (for bootstrap)
        # and a catch-all MATCHING rule that routes through the PrivateLink endpoint.
        redpanda_addr = f"redpanda.{namespace}.svc.cluster.local:9092"
        mz.environmentd.sql(dedent(f"""\
                CREATE CONNECTION kafka_via_privatelink_e2e TO KAFKA (
                    BROKERS (
                        '{redpanda_addr}' USING AWS PRIVATELINK privatelink_e2e_conn (PORT 9092),
                        MATCHING '*' USING AWS PRIVATELINK privatelink_e2e_conn (PORT 9092)
                    ),
                    SECURITY PROTOCOL PLAINTEXT
                ) WITH (VALIDATE = false)
                """))

        # Create a topic for testing with an explicit seed for reproducibility
        topic_base = "privatelink-e2e-test"
        seed = random.randint(0, 2**31 - 1)
        full_topic_name = f"testdrive-{topic_base}-{seed}"

        mz.testdrive.run(
            input=f"$ kafka-create-topic topic={topic_base}\n",
            no_reset=True,
            seed=seed,
        )

        # Step 7: Create source (proxy is enabled so this should succeed)
        mz.environmentd.sql(dedent(f"""\
                CREATE SOURCE privatelink_e2e_source
                FROM KAFKA CONNECTION kafka_via_privatelink_e2e (
                    TOPIC '{full_topic_name}'
                )
                """))

        mz.environmentd.sql(dedent(f"""\
                CREATE TABLE privatelink_e2e_tbl
                FROM SOURCE privatelink_e2e_source (
                    REFERENCE "{full_topic_name}"
                )
                FORMAT BYTES
                ENVELOPE NONE
                """))

        # Verify data flows initially
        mz.testdrive.run(
            input=dedent(f"""\
                $ kafka-ingest topic={topic_base} format=bytes
                test_data_via_privatelink

                > SELECT COUNT(*) FROM privatelink_e2e_tbl
                1
                """),
            no_reset=True,
            seed=seed,
        )

        # Step 8: Disable the proxy to test error handling
        requests.post(
            f"http://localhost:{admin_port}/proxies/kafka",
            json={
                "name": "kafka",
                "listen": "0.0.0.0:9092",
                "upstream": f"redpanda.{namespace}.svc.cluster.local:9092",
                "enabled": False,
            },
        )

        # Wait for the source to detect connection loss
        def check_source_not_running() -> None:
            status = mz.environmentd.sql_query(
                "SELECT status FROM mz_internal.mz_source_statuses WHERE name = 'privatelink_e2e_source'"
            )[0][0]
            assert status in (
                "stalled",
                "starting",
            ), f"Expected source to be stalled or starting when proxy is down, got: {status}"

        retry(
            f=check_source_not_running,
            max_attempts=30,
            exception_types=[AssertionError],
        )

        # Step 9: Re-enable the proxy and verify recovery
        requests.post(
            f"http://localhost:{admin_port}/proxies/kafka",
            json={
                "name": "kafka",
                "listen": "0.0.0.0:9092",
                "upstream": f"redpanda.{namespace}.svc.cluster.local:9092",
                "enabled": True,
            },
        )

        mz.testdrive.run(
            input=dedent(f"""\
                $ kafka-ingest topic={topic_base} format=bytes
                recovery_data

                > SELECT COUNT(*) FROM privatelink_e2e_tbl
                2
                """),
            no_reset=True,
            seed=seed,
        )

        # Verify source is now running
        def check_source_running() -> None:
            status = mz.environmentd.sql_query(
                "SELECT status FROM mz_internal.mz_source_statuses WHERE name = 'privatelink_e2e_source'"
            )[0][0]
            assert status == "running", f"Source status is {status}, expected running"

        retry(
            f=check_source_running,
            max_attempts=30,
            exception_types=[AssertionError],
        )

    finally:
        # Cleanup
        mz.environmentd.sql("DROP TABLE IF EXISTS privatelink_e2e_tbl CASCADE")
        mz.environmentd.sql("DROP SOURCE IF EXISTS privatelink_e2e_source CASCADE")
        mz.environmentd.sql(
            "DROP CONNECTION IF EXISTS kafka_via_privatelink_e2e CASCADE"
        )
        mz.environmentd.sql("DROP CONNECTION IF EXISTS privatelink_e2e_conn CASCADE")
        if privatelink_svc is not None:
            privatelink_svc.delete()
        if toxiproxy_service is not None:
            toxiproxy_service.delete()
        if toxiproxy_deployment is not None:
            toxiproxy_deployment.delete()


def test_privatelink_pattern_matching(mz: MaterializeApplication) -> None:
    """
    Test that pattern-based AZ routing works with MATCHING broker rules.

    Patches Redpanda to advertise an AZ-specific broker address, then verifies
    that the MATCHING rule routes post-metadata traffic through the AZ-specific
    toxiproxy, NOT the default one.

    Architecture:
        Bootstrap:
          Static broker 'redpanda.default.svc.cluster.local:9092' in BROKERS
          -> resolve_broker_addr -> connection-{id} (default)
          -> ExternalName -> toxiproxy-default -> Redpanda

        Post-metadata (after Redpanda returns AZ-specific advertised address):
          resolve_broker_addr('redpanda-use1-az1.default.svc.cluster.local', 9092)
          -> MATCHING '*use1-az1*' -> connection-{id}-use1-az1 (AZ endpoint)
          -> ExternalName -> toxiproxy-use1-az1 -> Redpanda

    After bootstrap, toxiproxy-default is DISABLED. If data still flows, it
    proves the MATCHING rule routed through the AZ toxiproxy.
    """
    namespace = DEFAULT_K8S_NAMESPACE
    az = "use1-az1"
    broker_alias = f"redpanda-{az}"
    advertised_addr = f"{broker_alias}.{namespace}.svc.cluster.local:9092"
    # The bootstrap address uses the standard Redpanda service name.
    # This is different from the advertised address, which contains the AZ.
    bootstrap_addr = f"redpanda.{namespace}.svc.cluster.local:9092"

    # Track resources for cleanup
    toxiproxy_az_deployment = None
    toxiproxy_az_service = None
    toxiproxy_default_deployment = None
    toxiproxy_default_service = None
    privatelink_svc_az = None
    privatelink_svc_default = None
    original_redpanda_args = None
    broker_alias_service_created = False

    try:
        # Step 1: Create a K8s service alias for Redpanda with an AZ-specific name.
        # Redpanda will advertise this address, and it must resolve in the cluster
        # so that testdrive can also reach it for topic creation.
        broker_alias_svc = {
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {"name": broker_alias, "namespace": namespace},
            "spec": {
                "selector": {"app": "redpanda"},
                "ports": [{"port": 9092, "targetPort": 9092}],
            },
        }
        subprocess.run(
            ["kubectl", "--context=kind-mzcloud", "apply", "-f", "-"],
            input=json.dumps(broker_alias_svc),
            text=True,
            check=True,
        )
        broker_alias_service_created = True

        # Step 2: Save original Redpanda config for restoration
        redpanda_deployment = mz.kubectl("get", "deployment", "redpanda", "-o", "json")
        redpanda_config = json.loads(redpanda_deployment)
        original_redpanda_args = redpanda_config["spec"]["template"]["spec"][
            "containers"
        ][0].get("command", [])

        # Step 3: Patch Redpanda to advertise the AZ-specific address.
        # After bootstrap, metadata will return this address instead of the
        # bootstrap address, which is what triggers pattern-based routing.
        patch = {
            "spec": {
                "template": {
                    "spec": {
                        "containers": [
                            {
                                "name": "redpanda",
                                "command": [
                                    "/usr/bin/rpk",
                                    "redpanda",
                                    "start",
                                    "--overprovisioned",
                                    "--smp",
                                    "1",
                                    "--memory",
                                    "1G",
                                    "--reserve-memory",
                                    "0M",
                                    "--node-id",
                                    "0",
                                    "--check=false",
                                    "--set",
                                    "redpanda.enable_transactions=true",
                                    "--set",
                                    "redpanda.enable_idempotence=true",
                                    "--set",
                                    "redpanda.auto_create_topics_enabled=true",
                                    "--advertise-kafka-addr",
                                    advertised_addr,
                                ],
                            }
                        ]
                    }
                }
            }
        }
        mz.kubectl(
            "patch",
            "deployment",
            "redpanda",
            "--type=strategic",
            "-p",
            json.dumps(patch),
        )

        mz.kubectl("rollout", "status", "deployment/redpanda", "--timeout=120s")
        wait(condition="condition=Available", resource="deployment/redpanda")
        time.sleep(5)

        # Step 4: Deploy two toxiproxy instances
        toxiproxy_az_deployment = ToxiproxyDeployment(namespace, name=f"toxiproxy-{az}")
        toxiproxy_az_service = ToxiproxyService(namespace, name=f"toxiproxy-{az}")
        toxiproxy_az_deployment.create()
        toxiproxy_az_service.create()

        toxiproxy_default_deployment = ToxiproxyDeployment(
            namespace, name="toxiproxy-default"
        )
        toxiproxy_default_service = ToxiproxyService(
            namespace, name="toxiproxy-default"
        )
        toxiproxy_default_deployment.create()
        toxiproxy_default_service.create()

        wait(condition="condition=Available", resource=f"deployment/toxiproxy-{az}")
        wait(condition="condition=Available", resource="deployment/toxiproxy-default")
        time.sleep(5)

        # Configure both toxiproxies to route to Redpanda (both ENABLED initially)
        az_admin_port = toxiproxy_az_service.node_port("admin")
        requests.post(
            f"http://localhost:{az_admin_port}/proxies",
            json={
                "name": "kafka",
                "listen": "0.0.0.0:9092",
                "upstream": f"redpanda.{namespace}.svc.cluster.local:9092",
                "enabled": True,
            },
        )

        default_admin_port = toxiproxy_default_service.node_port("admin")
        requests.post(
            f"http://localhost:{default_admin_port}/proxies",
            json={
                "name": "kafka",
                "listen": "0.0.0.0:9092",
                "upstream": f"redpanda.{namespace}.svc.cluster.local:9092",
                "enabled": True,
            },
        )

        # Step 5: Create PrivateLink connection
        mz.environmentd.sql(
            "ALTER SYSTEM SET max_aws_privatelink_connections = 5",
            port="internal",
            user="mz_system",
        )
        mz.environmentd.sql(
            "ALTER SYSTEM SET enable_connection_validation_syntax = true",
            port="internal",
            user="mz_system",
        )
        mz.environmentd.sql(
            "ALTER SYSTEM SET enable_kafka_broker_matching_rules = true",
            port="internal",
            user="mz_system",
        )

        mz.environmentd.sql(dedent(f"""\
                CREATE CONNECTION privatelink_pattern_conn
                TO AWS PRIVATELINK (
                    SERVICE NAME 'com.amazonaws.vpce.test.vpce-svc-pattern-test',
                    AVAILABILITY ZONES ('{az}')
                )
                """))

        connection_id = mz.environmentd.sql_query(
            "SELECT id FROM mz_connections WHERE name = 'privatelink_pattern_conn'"
        )[0][0]

        # Step 6: Create ExternalName services
        # AZ-specific endpoint -> toxiproxy-az (stays enabled, for post-metadata traffic)
        privatelink_svc_az = PrivateLinkExternalNameService(
            connection_id=connection_id,
            target_service=f"toxiproxy-{az}.{namespace}.svc.cluster.local",
            namespace=namespace,
            availability_zone=az,
        )
        privatelink_svc_az.create()

        # Default endpoint -> toxiproxy-default (enabled for bootstrap, disabled later)
        privatelink_svc_default = PrivateLinkExternalNameService(
            connection_id=connection_id,
            target_service=f"toxiproxy-default.{namespace}.svc.cluster.local",
            namespace=namespace,
            availability_zone=None,
        )
        privatelink_svc_default.create()

        # Step 7: Create Kafka connection with a static broker (for bootstrap)
        # and a MATCHING rule for AZ-specific routing.
        # Static broker: bootstrap address -> default endpoint (no AZ)
        # MATCHING rule: anything containing the AZ -> AZ-specific endpoint
        mz.environmentd.sql(dedent(f"""\
                CREATE CONNECTION kafka_pattern_test TO KAFKA (
                    BROKERS (
                        '{bootstrap_addr}' USING AWS PRIVATELINK privatelink_pattern_conn (PORT 9092),
                        MATCHING '*{az}*' USING AWS PRIVATELINK privatelink_pattern_conn (
                            AVAILABILITY ZONE '{az}',
                            PORT 9092
                        )
                    ),
                    SECURITY PROTOCOL PLAINTEXT
                ) WITH (VALIDATE = false)
                """))

        # Step 8: Create topic and source
        topic_base = "privatelink-pattern-test"
        seed = random.randint(0, 2**31 - 1)
        full_topic_name = f"testdrive-{topic_base}-{seed}"

        mz.testdrive.run(
            input=f"$ kafka-create-topic topic={topic_base}\n",
            no_reset=True,
            seed=seed,
        )

        mz.environmentd.sql(dedent(f"""\
                CREATE SOURCE privatelink_pattern_source
                FROM KAFKA CONNECTION kafka_pattern_test (
                    TOPIC '{full_topic_name}'
                )
                """))

        mz.environmentd.sql(dedent(f"""\
                CREATE TABLE privatelink_pattern_tbl
                FROM SOURCE privatelink_pattern_source (
                    REFERENCE "{full_topic_name}"
                )
                FORMAT BYTES
                ENVELOPE NONE
                """))

        # Step 9: Wait for bootstrap + initial metadata fetch to complete,
        # then DISABLE toxiproxy-default. After this, the only working path
        # to Redpanda is through toxiproxy-use1-az1 (the AZ-specific proxy).
        # If pattern matching fails, traffic falls back to the default endpoint
        # which now hits a dead proxy -> source stalls.
        time.sleep(10)

        requests.post(
            f"http://localhost:{default_admin_port}/proxies/kafka",
            json={
                "name": "kafka",
                "listen": "0.0.0.0:9092",
                "upstream": f"redpanda.{namespace}.svc.cluster.local:9092",
                "enabled": False,
            },
        )

        # Step 10: Verify data flows through the AZ-specific path.
        # This only succeeds if resolve_broker_addr matched '*use1-az1*'
        # against the advertised address 'redpanda-use1-az1.default...:9092'
        # and routed through the AZ endpoint instead of the (now dead) default.
        mz.testdrive.run(
            input=dedent(f"""\
                $ kafka-ingest topic={topic_base} format=bytes
                pattern_matching_works

                > SELECT COUNT(*) FROM privatelink_pattern_tbl
                1
                """),
            no_reset=True,
            seed=seed,
        )

        # Verify source is running
        def check_source_running() -> None:
            status = mz.environmentd.sql_query(
                "SELECT status FROM mz_internal.mz_source_statuses WHERE name = 'privatelink_pattern_source'"
            )[0][0]
            assert (
                status == "running"
            ), f"Source should be running (pattern matching worked!), got: {status}"

        retry(
            f=check_source_running,
            max_attempts=30,
            exception_types=[AssertionError],
        )

    finally:
        # Cleanup SQL objects
        mz.environmentd.sql("DROP TABLE IF EXISTS privatelink_pattern_tbl CASCADE")
        mz.environmentd.sql("DROP SOURCE IF EXISTS privatelink_pattern_source CASCADE")
        mz.environmentd.sql("DROP CONNECTION IF EXISTS kafka_pattern_test CASCADE")
        mz.environmentd.sql(
            "DROP CONNECTION IF EXISTS privatelink_pattern_conn CASCADE"
        )

        # Cleanup ExternalName services
        if privatelink_svc_az is not None:
            privatelink_svc_az.delete()
        if privatelink_svc_default is not None:
            privatelink_svc_default.delete()

        # Cleanup toxiproxy
        if toxiproxy_az_service is not None:
            toxiproxy_az_service.delete()
        if toxiproxy_az_deployment is not None:
            toxiproxy_az_deployment.delete()
        if toxiproxy_default_service is not None:
            toxiproxy_default_service.delete()
        if toxiproxy_default_deployment is not None:
            toxiproxy_default_deployment.delete()

        # Cleanup broker alias service
        if broker_alias_service_created:
            try:
                mz.kubectl("delete", "service", broker_alias)
            except Exception:
                pass  # Best effort cleanup

        # Restore original Redpanda configuration
        if original_redpanda_args:
            restore_patch = {
                "spec": {
                    "template": {
                        "spec": {
                            "containers": [
                                {"name": "redpanda", "command": original_redpanda_args}
                            ]
                        }
                    }
                }
            }
            try:
                mz.kubectl(
                    "patch",
                    "deployment",
                    "redpanda",
                    "--type=strategic",
                    "-p",
                    json.dumps(restore_patch),
                )
                mz.kubectl("rollout", "status", "deployment/redpanda", "--timeout=120s")
            except Exception:
                pass  # Best effort restoration
