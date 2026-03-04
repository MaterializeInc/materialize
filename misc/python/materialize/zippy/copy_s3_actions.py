# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import random
from textwrap import dedent

from materialize.mzcompose.composition import Composition
from materialize.zippy.balancerd_capabilities import BalancerdIsRunning
from materialize.zippy.framework import Action, Capabilities, Capability, State
from materialize.zippy.mz_capabilities import MzIsRunning
from materialize.zippy.table_capabilities import TableExists


class CopyToFromS3(Action):
    """Performs a COPY TO S3 followed by COPY FROM S3 roundtrip and validates the row count."""

    @classmethod
    def requires(cls) -> set[type[Capability]]:
        return {BalancerdIsRunning, MzIsRunning, TableExists}

    def __init__(self, capabilities: Capabilities) -> None:
        self.table = random.choice(capabilities.get(TableExists))
        super().__init__(capabilities)

    def run(self, c: Composition, state: State) -> None:
        conn_name = f"zippy_aws_conn_{self.seqno}"
        secret_name = f"zippy_minio_secret_{self.seqno}"
        staging_table = f"zippy_s3_staging_{self.seqno}"
        s3_key = f"zippy/{self.seqno}"
        expected_count = self.table.watermarks.max - self.table.watermarks.min + 1

        c.testdrive(
            dedent(
                f"""
                $ postgres-execute connection=postgres://mz_system:materialize@${{testdrive.materialize-internal-sql-addr}}
                ALTER SYSTEM SET enable_copy_from_remote = true;

                > CREATE SECRET {secret_name} AS '${{testdrive.aws-secret-access-key}}'
                > CREATE CONNECTION {conn_name} TO AWS (ENDPOINT '${{testdrive.aws-endpoint}}', REGION 'us-east-1', ACCESS KEY ID '${{testdrive.aws-access-key-id}}', SECRET ACCESS KEY SECRET {secret_name})
                > COPY (SELECT f1 FROM {self.table.name}) TO 's3://copytos3/{s3_key}' WITH (AWS CONNECTION = {conn_name}, FORMAT = 'csv')
                > CREATE TABLE {staging_table} (f1 INTEGER)
                > COPY INTO {staging_table} FROM 's3://copytos3/{s3_key}' (FORMAT CSV, AWS CONNECTION = {conn_name})
                > SELECT COUNT(*) FROM {staging_table}
                {expected_count}
                > DROP TABLE {staging_table}
                > DROP CONNECTION {conn_name}
                > DROP SECRET {secret_name}
                """
            ),
            mz_service=state.mz_service,
        )
