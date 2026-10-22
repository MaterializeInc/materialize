# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
THROWAWAY spike test, never merge. Runs the WASM-hosted sink spike: clusterd
swaps the render of every Kafka sink for a WASM guest that upserts into Valkey
(`MZ_SPIKE_WASM_SINK`, see `src/storage/src/sink/wasm.rs`). The guest in
`misc/spike-wasm-sink/guest` is built for wasm32-wasip2 before each workflow.

Each workflow checks that the Valkey contents equal the materialized view once
the sink has caught up.
"""

import json
import socket
import subprocess
import time
from typing import Any

from materialize import MZ_ROOT

from materialize.mzcompose.composition import Composition, WorkflowArgumentParser
from materialize.mzcompose.service import Service
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.redpanda import Redpanda

GUEST_DIR = MZ_ROOT / "misc" / "spike-wasm-sink" / "guest"
GUEST_WASM = "target/wasm32-wasip2/release/wasm_sink_guest.wasm"

SPIKE_CONFIG = {"wasm": "/spike/guest.wasm", "valkey": "valkey:6379"}

SERVICES = [
    Redpanda(),
    Service(
        name="valkey",
        config={
            "image": "valkey/valkey:8.1",
            "command": ["valkey-server", "--appendonly", "yes"],
            "ports": [6379],
        },
    ),
    Materialized(
        environment_extra=[f"MZ_SPIKE_WASM_SINK={json.dumps(SPIKE_CONFIG)}"],
        volumes_extra=[
            f"../../misc/spike-wasm-sink/guest/{GUEST_WASM}:/spike/guest.wasm:ro"
        ],
    ),
]


class Resp:
    """Minimal pipelined RESP2 client."""

    def __init__(self, port: int) -> None:
        self.sock = socket.create_connection(("localhost", port))
        self.file = self.sock.makefile("rb")

    def send(self, cmds: list[list]) -> list[Any]:
        out = bytearray()
        for args in cmds:
            out += b"*%d\r\n" % len(args)
            for a in args:
                a = a if isinstance(a, bytes) else str(a).encode()
                out += b"$%d\r\n%s\r\n" % (len(a), a)
        self.sock.sendall(out)
        return [self._read() for _ in cmds]

    def call(self, *args: Any) -> Any:
        return self.send([list(args)])[0]

    def _read(self) -> Any:
        line = self.file.readline()[:-2]
        tag, rest = line[:1], line[1:]
        if tag == b"+":
            return rest.decode()
        if tag == b"-":
            raise RuntimeError(rest.decode())
        if tag == b":":
            return int(rest)
        if tag == b"$":
            n = int(rest)
            return None if n < 0 else self.file.read(n + 2)[:-2]
        if tag == b"*":
            n = int(rest)
            return None if n < 0 else [self._read() for _ in range(n)]
        raise RuntimeError(f"bad RESP line {line!r}")


def valkey(c: Composition) -> Resp:
    return Resp(c.port("valkey", 6379))


def build_guest() -> None:
    # Must exist before `up`, or Docker creates a directory at the mount source.
    subprocess.run(
        [
            "cargo",
            "build",
            "--release",
            "--locked",
            "--target",
            "wasm32-wasip2",
            "--target-dir",
            str(GUEST_DIR / "target"),
        ],
        cwd=GUEST_DIR,
        check=True,
    )


def setup(c: Composition, rows: int, workers: int) -> str:
    """Creates a table, an MV over it, and the swapped sink; returns the sink id."""
    build_guest()
    c.down(destroy_volumes=True)
    c.up("redpanda", "valkey", "materialized")
    c.sql(
        f"CREATE CLUSTER sink_c SIZE 'scale=1,workers={workers}';"
        "CREATE TABLE t (id bigint NOT NULL, v text NOT NULL, n bigint NOT NULL);"
    )
    step = 1_000_000
    for lo in range(1, rows + 1, step):
        hi = min(rows, lo + step - 1)
        c.sql(f"INSERT INTO t SELECT g, 'v' || g, 0 FROM generate_series({lo}, {hi}) g")
    c.sql(
        "CREATE MATERIALIZED VIEW mv AS SELECT id, v, n FROM t;"
        "CREATE CONNECTION rp TO KAFKA (BROKER 'kafka:9092', SECURITY PROTOCOL PLAINTEXT);"
        "CREATE SINK ws IN CLUSTER sink_c FROM mv INTO KAFKA CONNECTION rp (TOPIC 'ws')"
        " KEY (id) NOT ENFORCED FORMAT JSON ENVELOPE UPSERT;"
    )
    return c.sql_query("SELECT id FROM mz_sinks WHERE name = 'ws'")[0][0]


def churn(c: Composition, statements: int, modulus: int = 10) -> None:
    for i in range(statements):
        c.sql(f"UPDATE t SET n = n + 1 WHERE id % {modulus} = {i % modulus}")


def progress(c: Composition, sink_id: str) -> int | None:
    p = valkey(c).call("GET", f"{{{sink_id}}}:progress")
    return None if p is None else int(p)


def wait_caught_up(c: Composition, sink_id: str, timeout: float = 900) -> None:
    """Waits until the sink's committed frontier reaches the MV's write frontier."""
    deadline = time.time() + timeout
    while True:
        upper = int(
            c.sql_query(
                "SELECT write_frontier FROM mz_internal.mz_frontiers f"
                " JOIN mz_materialized_views m ON m.id = f.object_id WHERE m.name = 'mv'"
            )[0][0]
        )
        p = progress(c, sink_id)
        print(f"mv upper {upper}, sink progress {p}")
        if p is not None and p >= upper:
            return
        assert time.time() < deadline, f"sink did not catch up: {p} < {upper}"
        time.sleep(5)


def fingerprint_mz(c: Composition) -> tuple[int, int, int]:
    count, s_id, s_mix = c.sql_query(
        "SELECT count(*), sum(id), sum((id % 1000003) * (n + 1) + length(v)) FROM mv"
    )[0]
    return int(count), int(s_id), int(s_mix)


def fingerprint_valkey(c: Composition, sink_id: str) -> tuple[int, int, int]:
    r = valkey(c)
    prefix = f"{{{sink_id}}}:k:"
    count = s_id = s_mix = 0
    cursor = b"0"
    while True:
        cursor, keys = r.call("SCAN", cursor, "MATCH", prefix + "*", "COUNT", 50000)
        if keys:
            for k, h in zip(keys, r.send([["HGETALL", k] for k in keys])):
                d = {h[j].decode(): h[j + 1].decode() for j in range(0, len(h), 2)}
                id_ = int(k.decode()[len(prefix) :])
                count += 1
                s_id += id_
                s_mix += (id_ % 1000003) * (int(d["n"]) + 1) + len(d["v"])
        if cursor == b"0":
            return count, s_id, s_mix


def verify(c: Composition, sink_id: str) -> None:
    """Asserts Valkey equals the MV. Requires no concurrent writes."""
    wait_caught_up(c, sink_id)
    mz = fingerprint_mz(c)
    vk = fingerprint_valkey(c, sink_id)
    print(f"fingerprint mz={mz} valkey={vk}")
    assert mz == vk, f"Valkey diverged from the MV: mz={mz} valkey={vk}"


def kill_sink_clusterd(c: Composition) -> None:
    cluster_id, replica_id = c.sql_query(
        "SELECT c.id, r.id FROM mz_clusters c JOIN mz_cluster_replicas r"
        " ON r.cluster_id = c.id WHERE c.name = 'sink_c'"
    )[0]
    pattern = f"cluster-{cluster_id}-replica-{replica_id}-"
    c.exec(
        "materialized",
        "bash",
        "-c",
        f"ps -eo pid,args | grep '[c]lusterd' | grep '{pattern}'"
        " | awk '{print $1}' | xargs -r kill -9",
    )


def sink_errors(c: Composition, pattern: str) -> int:
    return int(
        c.sql_query(
            "SELECT count(*) FROM mz_internal.mz_sink_status_history h"
            " JOIN mz_sinks s ON s.id = h.sink_id"
            f" WHERE s.name = 'ws' AND h.error ILIKE '%{pattern}%'"
        )[0][0]
    )


def wait_for_sink_error(c: Composition, pattern: str, timeout: float = 180) -> None:
    deadline = time.time() + timeout
    while sink_errors(c, pattern) == 0:
        assert time.time() < deadline, f"no sink error matching {pattern!r}"
        time.sleep(2)


def workflow_default(c: Composition) -> None:
    for name in c.workflows:
        if name == "default":
            continue
        with c.test_case(name):
            c.workflow(name)
            c.down(destroy_volumes=True)


def workflow_multi_worker(c: Composition, parser: WorkflowArgumentParser) -> None:
    """Snapshot and steady state through a multi-worker replica, with replica kills."""
    parser.add_argument("--rows", type=int, default=10_000_000)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--kills", type=int, default=5)
    args = parser.parse_args()

    sink_id = setup(c, args.rows, args.workers)
    verify(c, sink_id)

    for _ in range(args.kills):
        churn(c, 2)
        kill_sink_clusterd(c)
        churn(c, 2)
    verify(c, sink_id)


def workflow_fencing(c: Composition, parser: WorkflowArgumentParser) -> None:
    """An older guest instance must not write once a newer one has opened."""
    parser.add_argument("--rows", type=int, default=100_000)
    args = parser.parse_args()

    sink_id = setup(c, args.rows, 1)
    verify(c, sink_id)
    epoch_key = f"{{{sink_id}}}:epoch"

    # A foreign epoch bump fences the running instance; the sink restarts with
    # a fresh epoch and catches up.
    valkey(c).call("INCR", epoch_key)
    churn(c, 2)
    wait_for_sink_error(c, "fenced")
    churn(c, 2)
    verify(c, sink_id)

    # A paused target outlives the 30s call deadline. The abandoned guest's
    # in-flight transaction resumes after unpause and must abort.
    epoch_before = int(valkey(c).call("GET", epoch_key))
    c.pause("valkey")
    churn(c, 3)
    time.sleep(45)
    c.unpause("valkey")
    wait_for_sink_error(c, "deadline")
    churn(c, 2)
    verify(c, sink_id)
    epoch_after = int(valkey(c).call("GET", epoch_key))
    assert epoch_after > epoch_before, f"no new epoch: {epoch_before} -> {epoch_after}"
