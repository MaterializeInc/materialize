# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Helpers for working with Prometheus metrics in mzcompose tests."""

from copy import copy


class Metrics:
    """A collection of Prometheus metrics fetched from a service."""

    metrics: dict[str, str]

    def __init__(self, raw: str) -> None:
        self.metrics = {}
        for line in raw.splitlines():
            if line.startswith("#") or not line.strip():
                continue
            key, value = line.split(maxsplit=1)
            self.metrics[key] = value

    def for_instance(self, id: str) -> "Metrics":
        new = copy(self)
        new.metrics = {
            k: v for k, v in self.metrics.items() if f'instance_id="{id}"' in k
        }
        return new

    def with_name(self, metric_name: str) -> dict[str, float]:
        items = {}
        for key, value in self.metrics.items():
            if key.startswith(metric_name):
                items[key] = float(value)
        return items

    def get_value(self, metric_name: str) -> float:
        metrics = self.with_name(metric_name)
        values = list(metrics.values())
        assert len(values) == 1
        return values[0]

    def get_summed_value(self, metric_name: str) -> float:
        metrics = self.with_name(metric_name)
        return sum(metrics.values())

    def get_command_count(self, metric: str, command_type: str) -> float:
        metrics = self.with_name(metric)
        values = [
            v for k, v in metrics.items() if f'command_type="{command_type}"' in k
        ]
        assert len(values) == 1
        return values[0]

    def get_response_count(self, metric: str, response_type: str) -> float:
        metrics = self.with_name(metric)
        values = [
            v for k, v in metrics.items() if f'response_type="{response_type}"' in k
        ]
        assert len(values) == 1
        return values[0]

    def get_replica_history_command_count(self, command_type: str) -> float:
        return self.get_command_count(
            "mz_compute_replica_history_command_count", command_type
        )

    def get_compute_controller_history_command_count(self, command_type: str) -> float:
        return self.get_command_count(
            "mz_compute_controller_history_command_count", command_type
        )

    def get_storage_controller_history_command_count(self, command_type: str) -> float:
        return self.get_command_count(
            "mz_storage_controller_history_command_count", command_type
        )

    def get_compute_commands_total(self, command_type: str) -> float:
        return self.get_command_count("mz_compute_commands_total", command_type)

    def get_compute_responses_total(self, response_type: str) -> float:
        return self.get_response_count("mz_compute_responses_total", response_type)

    def get_storage_commands_total(self, command_type: str) -> float:
        return self.get_command_count("mz_storage_commands_total", command_type)

    def get_storage_responses_total(self, response_type: str) -> float:
        return self.get_response_count("mz_storage_responses_total", response_type)

    def get_peeks_total(self, result: str) -> float:
        metrics = self.with_name("mz_compute_peeks_total")
        values = [v for k, v in metrics.items() if f'result="{result}"' in k]
        assert len(values) == 1
        return values[0]

    def get_wallclock_lag_count(self, collection_id: str) -> float | None:
        metrics = self.with_name("mz_dataflow_wallclock_lag_seconds_count")
        values = [
            v for k, v in metrics.items() if f'collection_id="{collection_id}"' in k
        ]
        assert len(values) <= 1
        return next(iter(values), None)

    def get_e2e_optimization_time(self, object_type: str) -> float:
        metrics = self.with_name("mz_optimizer_e2e_optimization_time_seconds_sum")
        values = [v for k, v in metrics.items() if f'object_type="{object_type}"' in k]
        assert len(values) == 1
        return values[0]

    def get_pgwire_message_processing_seconds(self, message_type: str) -> float:
        metrics = self.with_name("mz_pgwire_message_processing_seconds_sum")
        values = [
            v for k, v in metrics.items() if f'message_type="{message_type}"' in k
        ]
        assert len(values) == 1
        return values[0]

    def get_result_rows_first_to_last_byte_seconds(self, statement_type: str) -> float:
        metrics = self.with_name("mz_result_rows_first_to_last_byte_seconds_sum")
        values = [
            v for k, v in metrics.items() if f'statement_type="{statement_type}"' in k
        ]
        assert len(values) == 1
        return values[0]

    def get_last_command_received(self, server_name: str) -> float:
        metrics = self.with_name("mz_grpc_server_last_command_received")
        values = [v for k, v in metrics.items() if server_name in k]
        assert len(values) == 1
        return values[0]

    def get_compute_collection_count(self, type_: str, hydrated: str) -> float:
        metrics = self.with_name("mz_compute_collection_count")
        values = [
            v
            for k, v in metrics.items()
            if f'type="{type_}"' in k and f'hydrated="{hydrated}"' in k
        ]
        assert len(values) == 1
        return values[0]
