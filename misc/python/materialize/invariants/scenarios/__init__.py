# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize.invariants.framework import Scenario
from materialize.invariants.scenarios.cdc_bank import (
    MySqlCdcBank,
    PgCdcBank,
    SqlServerCdcBank,
)
from materialize.invariants.scenarios.kafka_ledger import KafkaLedger
from materialize.invariants.scenarios.kafka_upsert import KafkaUpsert
from materialize.invariants.scenarios.sink_roundtrip import SinkRoundtrip
from materialize.invariants.scenarios.table_bank import TableBank

SCENARIOS: dict[str, type[Scenario]] = {
    scenario.name: scenario
    for scenario in [
        TableBank,
        PgCdcBank,
        MySqlCdcBank,
        SqlServerCdcBank,
        KafkaLedger,
        KafkaUpsert,
        SinkRoundtrip,
    ]
}
