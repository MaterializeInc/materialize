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
import time
from collections.abc import Iterator
from enum import Enum
from typing import TYPE_CHECKING

from materialize.data_ingest.definition import Definition
from materialize.data_ingest.field import Field
from materialize.data_ingest.rowlist import RowList
from materialize.data_ingest.transaction import Transaction
from materialize.mzcompose.composition import Composition
from materialize.mzcompose.services.materialized import Materialized

if TYPE_CHECKING:
    from materialize.data_ingest.workload import Workload


class TransactionSize(Enum):
    SINGLE_OPERATION = 1
    HUGE = 1_000_000_000


class TransactionDef:
    operations: list[Definition]
    size: TransactionSize

    def __init__(
        self,
        operations: list[Definition],
        size: TransactionSize = TransactionSize.SINGLE_OPERATION,
    ):
        self.operations = operations
        self.size = size

    def generate(self, fields: list[Field]) -> Iterator[Transaction | None]:
        full_rowlist: list[RowList] = []
        for definition in self.operations:
            for i, rowlist in enumerate(definition.generate(fields)):
                full_rowlist.append(rowlist)
                if i + 1 == self.size.value:
                    yield Transaction(full_rowlist)
                    full_rowlist = []
        if full_rowlist:
            yield Transaction(full_rowlist)


class RestartMz(TransactionDef):
    composition: Composition
    probability: float

    def __init__(self, composition: Composition, probability: float):
        self.composition = composition
        self.probability = probability

    def generate(self, fields: list[Field]) -> Iterator[Transaction | None]:
        if random.random() < self.probability:
            self.composition.kill("materialized")
            time.sleep(1)
            self.composition.up("materialized")
        yield None


class ZeroDowntimeDeploy(TransactionDef):
    composition: Composition
    probability: float
    deploy_generation: int
    workload: "Workload"

    def __init__(
        self, composition: Composition, probability: float, workload: "Workload"
    ):
        self.composition = composition
        self.probability = probability
        self.workload = workload
        self.deploy_generation = 0

    def generate(self, fields: list[Field]) -> Iterator[Transaction | None]:
        if random.random() < self.probability:
            self.deploy_generation += 1

            if self.deploy_generation % 2 == 0:
                self.workload.mz_service = "materialized"
                ports = ["16875:6875"]
            else:
                self.workload.mz_service = "materialized2"
                ports = ["26875:6875"]

            print(
                f"Deploying generation {self.deploy_generation} on {self.workload.mz_service}"
            )

            with self.composition.override(
                Materialized(
                    name=self.workload.mz_service,
                    ports=ports,
                    external_minio=True,
                    external_cockroach=True,
                    additional_system_parameter_defaults={"enable_table_keys": "true"},
                    deploy_generation=self.deploy_generation,
                    restart="on-failure",
                    healthcheck=[
                        "CMD",
                        "curl",
                        "-f",
                        "localhost:6878/api/leader/status",
                    ],
                ),
            ):
                self.composition.up(self.workload.mz_service, detach=True)

                # Wait until ready to promote
                while True:
                    result = json.loads(
                        self.composition.exec(
                            self.workload.mz_service,
                            "curl",
                            "localhost:6878/api/leader/status",
                            capture=True,
                        ).stdout
                    )
                    if result["status"] == "ReadyToPromote":
                        break
                    assert (
                        result["status"] == "Initializing"
                    ), f"Unexpected status {result}"
                    print("Not ready yet, waiting 1 s")
                    time.sleep(1)

                # Promote new Mz service
                result = json.loads(
                    self.composition.exec(
                        self.workload.mz_service,
                        "curl",
                        "-X",
                        "POST",
                        "http://127.0.0.1:6878/api/leader/promote",
                        capture=True,
                    ).stdout
                )
                assert result["result"] == "Success", f"Unexpected result {result}"

        yield None
