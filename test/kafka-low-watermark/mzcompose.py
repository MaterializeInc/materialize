# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import re
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
    workflow_low_watermark(c)
    workflow_responsible_partition_filter(c)


def workflow_low_watermark(c: Composition) -> None:
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


def workflow_responsible_partition_filter(c: Composition) -> None:
    c.up("zookeeper", "kafka", "schema-registry", "materialized")

    topic = "responsible-partition-test"
    n = 4

    c.exec(
        "kafka",
        "bash",
        "-c",
        f"kafka-topics --bootstrap-server={BS} --create --topic={topic}"
        f" --partitions={n} --replication-factor=1",
    )
    c.exec(
        "kafka",
        "kafka-console-producer",
        f"--bootstrap-server={BS}",
        f"--topic={topic}",
        stdin="\n".join(f"msg-{i}" for i in range(n * 4)),
    )

    c.testdrive(f"""
> CREATE CONNECTION IF NOT EXISTS kafka_conn
  TO KAFKA (BROKER '${{testdrive.kafka-addr}}', SECURITY PROTOCOL PLAINTEXT)
> CREATE CLUSTER rpf_cluster SIZE 'scale=1,workers={n}'
> CREATE SOURCE rpf_src IN CLUSTER rpf_cluster
  FROM KAFKA CONNECTION kafka_conn (TOPIC '{topic}')
> CREATE TABLE rpf_tbl FROM SOURCE rpf_src (REFERENCE "{topic}")
  FORMAT TEXT ENVELOPE NONE
> SELECT count(*) FROM rpf_tbl
{n * 4}
""")

    source_id = c.sql_query("SELECT id FROM mz_sources WHERE name = 'rpf_src'")[0][0]
    logs = c.invoke("logs", "materialized", capture=True).stdout
    warn_re = re.compile(
        r"WARN.*partition (?P<pid>\d+) has a non-zero low watermark.*"
        r"Setting start offset to low watermark"
    )
    worker_re = re.compile(r"\bworker_id[=:\"\s]+(?P<w>\d+)")

    pairs: set[tuple[str, str]] = set()
    for line in logs.splitlines():
        if source_id not in line:
            continue
        if m := warn_re.search(line):
            w = worker_re.search(line)
            assert w, f"warning missing worker_id: {line!r}"
            pairs.add((w.group("w"), m.group("pid")))

    assert len(pairs) == n, (
        f"expected {n} (worker,pid) pairs (one per partition), got {len(pairs)}: "
        f"{sorted(pairs)}"
    )
    assert {pid for _, pid in pairs} == {str(p) for p in range(n)}
    counts = {pid: sum(1 for _, p in pairs if p == pid) for _, pid in pairs}
    for pid, c_ in counts.items():
        assert c_ == 1, f"partition {pid} warned by {c_} workers (expected 1)"
