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
from materialize.mzcompose.services import Dbt, Materialized, Testdrive

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

SERVICES = [Materialized(), Dbt(), Testdrive(no_reset=True)]


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    c.up("materialized")
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

    c.exec("dbt", "dbt", "run", "--threads", "8", workdir="/workdir")
    workflow_test(c, parser)


def workflow_test(c: Composition, parser: WorkflowArgumentParser) -> None:
    c.exec("dbt", "dbt", "test", "--threads", "8", workdir="/workdir")


def workflow_clean(c: Composition, parser: WorkflowArgumentParser) -> None:
    c.exec("dbt", "dbt", "clean", workdir="/workdir")
