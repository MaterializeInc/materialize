# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from materialize.scalability.operation.operation_data import OperationData
from materialize.scalability.operation.operations.operations import (
    Connect,
    ConnectPassword,
    ConnectSasl,
    Disconnect,
    SelectOne,
)
from materialize.scalability.operation.scalability_operation import (
    Operation,
    OperationChainWithDataExchange,
)
from materialize.scalability.workload.workload import WorkloadWithContext
from materialize.scalability.workload.workload_markers import ConnectionWorkload


class EstablishConnectionWorkload(WorkloadWithContext, ConnectionWorkload):
    def amend_data_before_execution(self, data: OperationData) -> None:
        data.push("endpoint", self.endpoint)
        data.push("schema", self.schema)
        data.remove("cursor")

    def operations(self) -> list["Operation"]:
        return [OperationChainWithDataExchange([Connect(), SelectOne(), Disconnect()])]


class EstablishPasswordConnectionWorkload(WorkloadWithContext, ConnectionWorkload):
    def amend_data_before_execution(self, data: OperationData) -> None:
        data.push("endpoint", self.endpoint)
        data.push("schema", self.schema)
        data.remove("cursor")

    def operations(self) -> list["Operation"]:
        return [
            OperationChainWithDataExchange(
                [ConnectPassword(), SelectOne(), Disconnect()]
            )
        ]


class EstablishSaslConnectionWorkload(WorkloadWithContext, ConnectionWorkload):
    def amend_data_before_execution(self, data: OperationData) -> None:
        data.push("endpoint", self.endpoint)
        data.push("schema", self.schema)
        data.remove("cursor")

    def operations(self) -> list["Operation"]:
        return [
            OperationChainWithDataExchange([ConnectSasl(), SelectOne(), Disconnect()])
        ]
