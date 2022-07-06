# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from typing import Dict, Type

from materialize.zippy.framework import Action, Scenario
from materialize.zippy.kafka_actions import CreateTopic, Ingest, KafkaStart
from materialize.zippy.mz_actions import MzStart, MzStop
from materialize.zippy.source_actions import CreateSource
from materialize.zippy.table_actions import DML, CreateTable, ValidateTable
from materialize.zippy.view_actions import CreateView, ValidateView


class KafkaSources(Scenario):
    def config(self) -> Dict[Type[Action], float]:
        return {
            MzStart: 1,
            MzStop: 10,
            KafkaStart: 1,
            CreateTopic: 5,
            CreateSource: 5,
            CreateView: 5,
            ValidateView: 10,
            Ingest: 90,
        }


class UserTables(Scenario):
    def config(self) -> Dict[Type[Action], float]:
        return {
            MzStart: 1,
            MzStop: 15,
            KafkaStart: 1,
            CreateTable: 10,
            CreateView: 10,
            ValidateTable: 20,
            ValidateView: 20,
            DML: 30,
        }
