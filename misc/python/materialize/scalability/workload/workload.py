# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from psycopg import Cursor

from materialize.scalability.endpoint.endpoint import Endpoint
from materialize.scalability.operation.operation_data import OperationData
from materialize.scalability.operation.scalability_operation import Operation
from materialize.scalability.schema.schema import Schema
from materialize.scalability.workload.workload_version import WorkloadVersion


class Workload:
    def init_operations(self) -> list[Operation]:
        return []

    def operations(self) -> list[Operation]:
        raise NotImplementedError

    def execute_operation(
        self,
        operation: Operation,
        cursor: Cursor,
        worker_id: int,
        transaction_index: int,
        verbose: bool,
    ) -> None:
        data = OperationData(cursor, worker_id)
        self.amend_data_before_execution(data)

        if verbose:
            print(f"#{transaction_index}: {operation} (worker_id={worker_id})")

        operation.execute(data)

    def amend_data_before_execution(self, data: OperationData) -> None:
        pass

    def name(self) -> str:
        return self.__class__.__name__

    def version(self) -> WorkloadVersion:
        return WorkloadVersion.create(1, 0, 0)


class WorkloadWithContext(Workload):
    endpoint: Endpoint
    schema: Schema

    def set_endpoint(self, endpoint: Endpoint) -> None:
        self.endpoint = endpoint

    def set_schema(self, schema: Schema) -> None:
        self.schema = schema
