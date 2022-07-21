# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from textwrap import dedent

from materialize.cloudtest.k8s.environmentd import (
    EnvironmentdService,
    EnvironmentdStatefulSet,
)
from materialize.cloudtest.k8s.minio import Minio
from materialize.cloudtest.k8s.postgres import POSTGRES_RESOURCES
from materialize.cloudtest.k8s.redpanda import REDPANDA_RESOURCES
from materialize.cloudtest.k8s.role_binding import AdminRoleBinding
from materialize.cloudtest.k8s.testdrive import Testdrive
from materialize.cloudtest.testcase import CloudTestCase
from materialize.cloudtest.wait import wait


class SimpleCloudTest(CloudTestCase):
    def test_simple(self) -> None:
        self.acquire_images()

        ed_service = EnvironmentdService()
        testdrive = Testdrive()

        for resource in [
            *POSTGRES_RESOURCES,
            *REDPANDA_RESOURCES,
            Minio(),
            AdminRoleBinding(),
            EnvironmentdStatefulSet(),
            ed_service,
            testdrive,
        ]:
            print(resource)
            resource.create()

        wait(condition="condition=Ready", resource="pod/compute-cluster-1-replica-1-0")

        ed_service.sql("SELECT 1")

        wait(condition="condition=Ready", resource="pod/testdrive")
        testdrive.run_string(
            input=dedent(
                """
                $ kafka-create-topic topic=test

                $ kafka-ingest format=bytes topic=test
                ABC

                > CREATE TABLE t1 (f1 INTEGER);
                > CREATE DEFAULT INDEX ON t1;
                > INSERT INTO t1 VALUES (1);

                > CREATE CLUSTER c1 REPLICAS (r1 (SIZE '1'), r2 (SIZE '2-2'));
                > SET cluster=c1

                > CREATE CONNECTION kafka FOR KAFKA BROKER '${testdrive.kafka-addr}'

                > CREATE SOURCE s1
                  FROM KAFKA CONNECTION kafka
                  TOPIC'testdrive-test-${testdrive.seed}'
                  FORMAT BYTES
                  ENVELOPE NONE;

                > CREATE MATERIALIZED VIEW v1 AS SELECT COUNT(*) FROM t1;
                > SELECT * FROM v1;
                1

                > CREATE MATERIALIZED VIEW v2 AS SELECT COUNT(*) FROM s1;
                > SELECT * FROM v2;
                1
                """
            )
        )
