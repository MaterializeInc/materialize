# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


from materialize.mzcompose.service import (
    Service,
    ServiceDependency,
)


class Debezium(Service):
    def __init__(
        self,
        name: str = "debezium",
        port: int = 8083,
        redpanda: bool = False,
        environment: list[str] = [
            "CONNECT_BOOTSTRAP_SERVERS=kafka:9092",
            "CONNECT_GROUP_ID=connect",
            "CONNECT_CONFIG_STORAGE_TOPIC=connect_configs",
            "CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR=1",
            "CONNECT_OFFSET_STORAGE_TOPIC=connect_offsets",
            "CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR=1",
            "CONNECT_STATUS_STORAGE_TOPIC=connect_statuses",
            "CONNECT_STATUS_STORAGE_REPLICATION_FACTOR=1",
            # We don't support JSON, so ensure that connect uses Avro to encode
            # messages and CSR to record the schema.
            "CONNECT_KEY_CONVERTER=io.confluent.connect.avro.AvroConverter",
            "CONNECT_VALUE_CONVERTER=io.confluent.connect.avro.AvroConverter",
            "CONNECT_KEY_CONVERTER_SCHEMA_REGISTRY_URL=http://schema-registry:8081",
            "CONNECT_VALUE_CONVERTER_SCHEMA_REGISTRY_URL=http://schema-registry:8081",
            "CONNECT_OFFSET_COMMIT_POLICY=AlwaysCommitOffsetPolicy",
            "CONNECT_ERRORS_RETRY_TIMEOUT=60000",
            "CONNECT_ERRORS_RETRY_DELAY_MAX_MS=1000",
        ],
    ) -> None:
        depends_on: dict[str, ServiceDependency] = {
            "kafka": {"condition": "service_healthy"},
            "schema-registry": {"condition": "service_healthy"},
        }
        environment.append(f"CONNECT_REST_ADVERTISED_HOST_NAME={name}")
        if redpanda:
            depends_on = {"redpanda": {"condition": "service_healthy"}}
        super().__init__(
            name=name,
            config={
                "mzbuild": "debezium",
                "init": True,
                "ports": [port],
                "environment": environment,
                "depends_on": depends_on,
                "healthcheck": {
                    "test": ["CMD", "curl", "-f", "localhost:8083"],
                    "interval": "1s",
                    "start_period": "120s",
                },
            },
        )
