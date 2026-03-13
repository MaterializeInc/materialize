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
from materialize.zippy.copy_s3_capabilities import S3ObjectExists
from materialize.zippy.framework import Action, Capabilities, Capability, State
from materialize.zippy.mz_capabilities import MzIsRunning
from materialize.zippy.table_capabilities import TableExists


class CopyToS3(Action):
    """Performs a COPY TO S3 and records the resulting S3 object as a capability."""

    @classmethod
    def requires(cls) -> set[type[Capability]]:
        return {BalancerdIsRunning, MzIsRunning, TableExists}

    def __init__(self, capabilities: Capabilities) -> None:
        self.table = random.choice(capabilities.get(TableExists))
        super().__init__(capabilities)
        self.s3_key = f"zippy/{self.seqno}"
        self.s3_object = S3ObjectExists(
            s3_key=self.s3_key,
            min_val=self.table.watermarks.min,
            max_val=self.table.watermarks.max,
        )

    def provides(self) -> list[Capability]:
        return [self.s3_object]

    def run(self, c: Composition, state: State) -> None:
        conn_name = f"zippy_aws_conn_{self.seqno}"
        secret_name = f"zippy_minio_secret_{self.seqno}"

        c.testdrive(
            dedent(
                f"""
                > CREATE SECRET {secret_name} AS '${{testdrive.aws-secret-access-key}}'
                > CREATE CONNECTION {conn_name} TO AWS (ENDPOINT '${{testdrive.aws-endpoint}}', REGION 'us-east-1', ACCESS KEY ID '${{testdrive.aws-access-key-id}}', SECRET ACCESS KEY SECRET {secret_name})
                > COPY (SELECT f1 FROM {self.table.name}) TO 's3://copytos3/{self.s3_key}' WITH (AWS CONNECTION = {conn_name}, FORMAT = 'csv')
                > DROP CONNECTION {conn_name}
                > DROP SECRET {secret_name}
                """
            ),
            mz_service=state.mz_service,
        )
        self.s3_object.min_val = self.table.watermarks.min
        self.s3_object.max_val = self.table.watermarks.max


class CopyFromS3(Action):
    """Reads a previously written S3 object back into a staging table and validates the row count."""

    @classmethod
    def requires(cls) -> set[type[Capability]]:
        return {BalancerdIsRunning, MzIsRunning, S3ObjectExists}

    def __init__(self, capabilities: Capabilities) -> None:
        self.s3_object = random.choice(capabilities.get(S3ObjectExists))
        super().__init__(capabilities)

    def run(self, c: Composition, state: State) -> None:
        conn_name = f"zippy_aws_conn_{self.seqno}"
        secret_name = f"zippy_minio_secret_{self.seqno}"
        staging_table = f"zippy_s3_staging_{self.seqno}"

        c.testdrive(
            dedent(
                f"""
                > CREATE SECRET {secret_name} AS '${{testdrive.aws-secret-access-key}}'
                > CREATE CONNECTION {conn_name} TO AWS (ENDPOINT '${{testdrive.aws-endpoint}}', REGION 'us-east-1', ACCESS KEY ID '${{testdrive.aws-access-key-id}}', SECRET ACCESS KEY SECRET {secret_name})
                > CREATE TABLE {staging_table} (f1 INTEGER)
                > COPY INTO {staging_table} FROM 's3://copytos3/{self.s3_object.s3_key}' (FORMAT CSV, AWS CONNECTION = {conn_name})
                > SELECT MIN(f1), MAX(f1), COUNT(f1), COUNT(DISTINCT f1) FROM {staging_table}
                {self.s3_object.min_val} {self.s3_object.max_val} {self.s3_object.max_val - self.s3_object.min_val + 1} {self.s3_object.max_val - self.s3_object.min_val + 1}
                > DROP TABLE {staging_table}
                > DROP CONNECTION {conn_name}
                > DROP SECRET {secret_name}
                """
            ),
            mz_service=state.mz_service,
        )
