# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import time

from materialize.mzcompose.composition import Composition, WorkflowArgumentParser
from materialize.mzcompose.services.kafka import Kafka
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.schema_registry import SchemaRegistry
from materialize.mzcompose.services.testdrive import Testdrive
from materialize.mzcompose.services.zookeeper import Zookeeper

SERVICES = [
    Zookeeper(),
    Kafka(environment_extra=["KAFKA_LOG_RETENTION_CHECK_INTERVAL_MS=1000"]),
    SchemaRegistry(),
    Materialized(),
    Testdrive(default_timeout="60s"),
]

TOPIC = "low-watermark-test"
BS = "localhost:9092"


def _get_offsets(c: Composition, topic: str) -> tuple[int, int]:
    def _get(time_flag: str) -> int:
        out = c.exec(
            "kafka",
            "bash",
            "-c",
            f"kafka-get-offsets --bootstrap-server={BS} --topic={topic} --time={time_flag}",
            capture=True,
        ).stdout
        return int(out.strip().split(":")[-1])

    return _get("-2"), _get("-1")


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    c.up("zookeeper", "kafka", "schema-registry", "materialized")

    c.exec(
        "kafka",
        "bash",
        "-c",
        f"kafka-topics --bootstrap-server={BS} --create --topic={TOPIC}"
        " --partitions=1 --replication-factor=1"
        " --config=cleanup.policy=delete --config=retention.ms=5000"
        " --config=segment.ms=500 --config=segment.bytes=65536",
    )

    c.exec(
        "kafka",
        "kafka-console-producer",
        f"--bootstrap-server={BS}",
        f"--topic={TOPIC}",
        stdin="\n".join(f"msg-{i}" for i in range(500)),
    )

    for _ in range(90):
        low, high = _get_offsets(c, TOPIC)
        if low > 0 and low == high:
            break
        time.sleep(1)
    else:
        raise RuntimeError(f"Topic never became empty (low={low}, high={high})")
    print(f"Topic empty: low=high={low}")

    for start_offset_clause in ["", ", START OFFSET (0)"]:
        suffix = "explicit" if start_offset_clause else "default"
        select_cmd = "!" if start_offset_clause else ">"
        expected = "contains:Low watermark" if start_offset_clause else "0"
        c.testdrive(f"""
> CREATE CONNECTION IF NOT EXISTS kafka_conn
  TO KAFKA (BROKER '${{testdrive.kafka-addr}}', SECURITY PROTOCOL PLAINTEXT)
> BEGIN
> CREATE SOURCE lwm_{suffix}
  FROM KAFKA CONNECTION kafka_conn (TOPIC '{TOPIC}'{start_offset_clause})
> CREATE TABLE lwm_{suffix}_tbl FROM SOURCE lwm_{suffix} (REFERENCE "{TOPIC}")
  FORMAT TEXT ENVELOPE NONE
> COMMIT
{select_cmd} SELECT count(*) FROM lwm_{suffix}_tbl
{expected}
""")
        print(f"PASSED: {suffix} START OFFSET with low watermark > 0")
