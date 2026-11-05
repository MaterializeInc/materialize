# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Fresh, sequential idle measurements. Run with the repository's bin/pyactivate."""

import argparse
import gzip
import json
import os
import re
import signal
import socket
import subprocess
import time
import urllib.request
from pathlib import Path
from types import SimpleNamespace

import psutil
import psycopg
from psycopg.rows import dict_row

from materialize.cli.run import _controller_args, _environment_args

ROOT = Path.cwd()
E = Path.home() / ".local/share/aj/m3-evidence/native-main-idle"
NAMES = {"controllerd", "adapterd", "environmentd", "clusterd", "pubsubd"}
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("kind", choices=["main", "native"])
parser.add_argument("--evidence-dir", default=str(E))
parser.add_argument("--postgres-image", default="postgres:18")
parser.add_argument("--warmup", type=int, default=300)
parser.add_argument("--window", type=int, default=300)
parser.add_argument("--windows", type=int, default=2)
parser.add_argument("--profile-seconds", type=int, default=30)
parser.add_argument("--label", default=None)
parser.add_argument("--scheduling-diagnostics", action="store_true")
parser.add_argument("--system-parameter", action="append", default=[])
args = parser.parse_args()
E = Path(args.evidence_dir).resolve()
if args.warmup < 0 or args.window < 4 or args.windows < 1 or args.profile_seconds < 0:
    parser.error("require warmup >= 0, window >= 4, windows >= 1, profile-seconds >= 0")
assert not any(
    p.info["name"] in NAMES | {"rustc", "cargo"} for p in psutil.process_iter(["name"])
), "no runtime/build interference"
os.umask(0o077)
RUN = E / f"{args.label or args.kind}-{int(time.time())}"
RUN.mkdir()
(RUN / "observe.py").write_bytes(Path(__file__).read_bytes())
state = RUN / "state"
state.mkdir()
(state / "scratch").mkdir()
binaries = E / "artifacts" / args.kind
container = "aj-idle-" + RUN.name
process = None
docker_attempted = False
owned = {}
endpoints = {}
result = {
    "status": "running",
    "kind": args.kind,
    "run": str(RUN),
    "arguments": vars(args),
}


def save(name, value):
    (RUN / name).write_text(json.dumps(value, indent=2, default=str))


def port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def get(url):
    with urllib.request.urlopen(url, timeout=15) as response:
        return json.load(response)


pg_port, control_port, relay_port = [port() for _ in range(3)]
pg_url = f"postgres://root@127.0.0.1:{pg_port}/materialize_idle?sslmode=disable"
control = f"http://127.0.0.1:{control_port}"
listeners = json.loads(
    (ROOT / "src/materialized/ci/listener_configs/v26_32_0/no_auth.json").read_text()
)
for protocol in ("sql", "http"):
    for listener in listeners[protocol].values():
        listener["addr"] = f"127.0.0.1:{port()}"
save("listeners.json", listeners)
env = dict(
    os.environ,
    RUSTUP_TOOLCHAIN="1.98.1",
    RUST_MIN_STACK="8388608",
    MZ_LOG_FILTER="info",
    MZ_SOFT_ASSERTIONS="1",
)
env.pop("SYSTEM_PARAMETER_DEFAULT", None)
launch_args = SimpleNamespace(
    postgres=pg_url,
    blob=(state / "persist/blob").as_uri(),
    listeners_config_path=str(RUN / "listeners.json"),
    controller_data_directory=state,
    controller_binaries_directory=binaries,
    pubsub_url=None,
    reset=False,
    adapter_sizes=json.dumps(
        {"1": {"cpu_limit": os.cpu_count(), "memory_limit": "16 GiB"}}
    ),
)
save("database.json", {"url": pg_url, "container": container, "blob": launch_args.blob})


def connect(address, internal=True):
    host, p = address.rsplit(":", 1)
    return psycopg.connect(
        host=host,
        port=p,
        user="mz_system" if internal else "materialize",
        dbname="materialize",
        autocommit=True,
        connect_timeout=5,
        options="-c statement_timeout=60000",
    )


def flag(cmd, name):
    for i, arg in enumerate(cmd):
        if arg.startswith(name + "="):
            return arg.split("=", 1)[1]
        if arg == name:
            return cmd[i + 1]
    return None


def discover():
    for p in psutil.process_iter(["name", "exe", "cmdline"]):
        if p.info["name"] not in NAMES or Path(p.info["exe"] or ".").parent != binaries:
            continue
        if p.pid in owned:
            continue
        cmd = p.info["cmdline"]
        role = p.info["name"]
        cluster = None
        if role == "clusterd":
            cluster = flag(cmd, "--catalog-cluster-id")
            replica = flag(cmd, "--catalog-replica-id")
            match = re.search(
                r"cluster-([su][0-9]+)-replica-([su][0-9]+)", " ".join(cmd)
            )
            if match:
                cluster, replica = match.groups()
            assert cluster and replica, cmd
            role = f"replica-{cluster}-{replica}"
        if p.info["name"] == "controllerd":
            address = f"127.0.0.1:{control_port}"
        elif p.info["name"] == "environmentd":
            address = listeners["http"]["internal"]["addr"]
        elif p.info["name"] == "adapterd":
            items = get(control + "/api/adapters")
            address = next(
                a["endpoints"]["http-internal"][0] for a in items if a["id"] == "a1"
            )
        else:
            address = flag(cmd, "--internal-http-listen-addr")
        assert address, (role, cmd)
        owned[p.pid] = {
            "pid": p.pid,
            "start": p.create_time(),
            "name": p.info["name"],
            "role": role,
            "cluster": cluster,
            "exe": p.info["exe"],
            "cmd": cmd,
            "http": address,
        }
        endpoints[role] = owned[p.pid]
    save("processes.json", list(owned.values()))


def curl(record, path, output, body=None):
    address = record["http"]
    command = ["curl", "--fail", "--silent", "--show-error", "--max-time", "90"]
    if address.startswith("/"):
        command += ["--unix-socket", address]
        url = "http://localhost" + path
    else:
        url = "http://" + address + path
    if body is not None:
        command += ["-H", "Content-Type: application/json", "-d", json.dumps(body)]
    command += ["-o", str(output), url]
    subprocess.run(command, check=True, timeout=100)


def cgroup(pid):
    for line in Path(f"/proc/{pid}/cgroup").read_text().splitlines():
        if line.startswith("0::"):
            return Path("/sys/fs/cgroup") / line[3:].lstrip("/")
    return None


def cgroup_values(path):
    values = {}
    if path:
        for name in [
            "cpu.stat",
            "cpu.max",
            "memory.current",
            "memory.max",
            "memory.events",
            "memory.stat",
            "io.stat",
        ]:
            file = path / name
            if file.exists():
                values[name] = file.read_text()
    return values


def postgres_snapshot():
    queries = {
        "database": "SELECT * FROM pg_stat_database WHERE datname=current_database()",
        "wal": "SELECT * FROM pg_stat_wal",
        "tables": "SELECT * FROM pg_stat_all_tables WHERE schemaname IN ('consensus','tsoracle','storage')",
        "table_io": "SELECT * FROM pg_statio_all_tables WHERE schemaname IN ('consensus','tsoracle','storage')",
        "activity": "SELECT pid,application_name,backend_type,state,wait_event_type,wait_event,client_port FROM pg_stat_activity WHERE datname=current_database()",
    }
    with psycopg.connect(
        pg_url,
        autocommit=True,
        row_factory=dict_row,
        application_name="aj_idle_sampler",
    ) as conn:
        snapshot = {"timing": {}}
        for name, query in queries.items():
            start = time.monotonic()
            snapshot[name] = conn.execute(query).fetchall()
            snapshot["timing"][name] = {"start": start, "end": time.monotonic()}
        return snapshot


def capture(label, pg_group):
    timing = {}
    for role, record in endpoints.items():
        start = time.monotonic()
        dest = RUN / f"{label}-{role}.prom"
        curl(record, "/metrics", dest)
        timing[role] = {"start": start, "end": time.monotonic()}
        with gzip.open(str(dest) + ".gz", "wb") as compressed:
            compressed.write(dest.read_bytes())
        dest.unlink()
    save(label + "-metrics-timing.json", timing)
    save(label + "-postgres.json", postgres_snapshot())
    save(label + "-postgres-cgroup.json", cgroup_values(pg_group))
    save(
        label + "-memory.json",
        [
            {
                "role": r["role"],
                "pid": pid,
                "memory": psutil.Process(pid).memory_full_info()._asdict(),
                "smaps_rollup": Path(f"/proc/{pid}/smaps_rollup").read_text(),
            }
            for pid, r in owned.items()
        ],
    )
    print("captured", label, flush=True)


def sample(duration, phase, pg_group):
    deadline = time.monotonic() + duration
    with (RUN / "usage.jsonl").open("a") as out:
        while time.monotonic() < deadline:
            assert process.poll() is None, "frontend/controller exited"
            assert (
                psutil.virtual_memory().available > 3 * 1024**3
            ), "preserve VM memory headroom"
            rows = []
            for pid, record in owned.items():
                p = psutil.Process(pid)
                assert (
                    p.create_time() == record["start"]
                    and p.status() != psutil.STATUS_ZOMBIE
                ), ("process replaced", record)
                rows.append(
                    {
                        "pid": pid,
                        "role": record["role"],
                        "cpu": p.cpu_times()._asdict(),
                        "memory": p.memory_info()._asdict(),
                        "io": p.io_counters()._asdict(),
                        "threads": p.num_threads(),
                        "cgroup": cgroup_values(cgroup(pid)),
                    }
                )
            out.write(
                json.dumps(
                    {
                        "time": time.monotonic(),
                        "wall_time": time.time(),
                        "phase": phase,
                        "available_memory": psutil.virtual_memory().available,
                        "cpu": psutil.cpu_times()._asdict(),
                        "processes": rows,
                        "postgres_cgroup": cgroup_values(pg_group),
                    }
                )
                + "\n"
            )
            out.flush()
            time.sleep(min(2, max(0, deadline - time.monotonic())))


def stop():
    # Reap our direct child before asking the launcher to wait on its identity.
    if process is not None and process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=30)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()
    launcher_error = None
    try:
        if args.kind == "native" and (state / "environment-id").exists():
            with (RUN / "stop.log").open("w") as log:
                code = subprocess.run(
                    [
                        "bin/controllerd",
                        "--controller-data-directory",
                        str(state),
                        "--stop",
                    ],
                    cwd=ROOT,
                    env=env,
                    stdout=log,
                    stderr=subprocess.STDOUT,
                    timeout=120,
                ).returncode
            result["launcher_stop_code"] = code
    except Exception as error:
        launcher_error = error
        result["launcher_stop_error"] = repr(error)
    # PID/start-qualified cleanup also covers failures during startup discovery.
    for p in psutil.process_iter(["name", "exe"]):
        if p.info["name"] in NAMES and Path(p.info["exe"] or ".").parent == binaries:
            owned.setdefault(p.pid, {"pid": p.pid, "start": p.create_time()})
    remaining = []
    for pid, record in owned.items():
        try:
            p = psutil.Process(pid)
            if (
                p.create_time() == record["start"]
                and p.status() != psutil.STATUS_ZOMBIE
            ):
                p.terminate()
                remaining.append(p)
        except psutil.NoSuchProcess:
            pass

    def alive():
        live = []
        for p in remaining:
            try:
                if p.is_running() and p.status() != psutil.STATUS_ZOMBIE:
                    live.append(p)
            except psutil.NoSuchProcess:
                pass
        return live

    deadline = time.monotonic() + 15
    while alive() and time.monotonic() < deadline:
        time.sleep(0.1)
    for p in alive():
        try:
            p.kill()
        except psutil.NoSuchProcess:
            pass
    deadline = time.monotonic() + 10
    while alive() and time.monotonic() < deadline:
        time.sleep(0.1)
    assert not alive(), "owned processes survived termination"
    # Wait for exited descendants to be reaped and discard process_iter's
    # cached executable identities before checking what still exists now.
    deadline = time.monotonic() + 10
    while True:
        psutil.process_iter.cache_clear()
        survivors = [
            p.info
            for p in psutil.process_iter(["pid", "name", "exe", "status"])
            if p.info["name"] in NAMES and Path(p.info["exe"] or ".").parent == binaries
        ]
        if not survivors:
            break
        if time.monotonic() >= deadline:
            save("cleanup-survivors.json", survivors)
            raise RuntimeError(f"fixture processes remain: {survivors}")
        time.sleep(0.1)

    if launcher_error is not None:
        raise launcher_error


def stop_postgres():
    # A launch may reach Docker even if its CLI never returns successfully.
    # Only our label and exact private name authorize cleanup in that case.
    containers = subprocess.check_output(
        [
            "docker",
            "ps",
            "-aq",
            "--filter",
            f"label=materialize.native-idle.fixture={RUN}",
            "--filter",
            f"name=^/{container}$",
        ],
        text=True,
        timeout=15,
    ).split()
    if containers:
        subprocess.run(
            ["docker", "stop", *containers],
            check=True,
            stdout=subprocess.DEVNULL,
            timeout=40,
        )


def interrupted(signum, frame):
    raise RuntimeError(f"interrupted {signum}")


for sig in [signal.SIGTERM, signal.SIGINT, signal.SIGALRM]:
    signal.signal(sig, interrupted)
signal.alarm(2400)
try:
    docker_attempted = True
    subprocess.run(
        [
            "docker",
            "run",
            "-d",
            "--name",
            container,
            "--label",
            f"materialize.native-idle.fixture={RUN}",
            "--memory=2g",
            "-p",
            f"127.0.0.1:{pg_port}:5432",
            "-e",
            "POSTGRES_HOST_AUTH_METHOD=trust",
            "-e",
            "POSTGRES_USER=root",
            "-e",
            "POSTGRES_DB=materialize_idle",
            args.postgres_image,
            "-c",
            "max_connections=1000",
            "-c",
            "track_io_timing=on",
        ],
        check=True,
        stdout=subprocess.DEVNULL,
        timeout=45,
    )
    for _ in range(30):
        if (
            subprocess.run(
                ["docker", "exec", container, "pg_isready", "-U", "root"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            ).returncode
            == 0
        ):
            break
        time.sleep(1)
    else:
        raise TimeoutError("Postgres readiness")
    pg_pid = int(
        subprocess.check_output(
            ["docker", "inspect", "--format", "{{.State.Pid}}", container], text=True
        )
    )
    pg_group = cgroup(pg_pid)
    if args.kind == "native":
        command = [
            str(binaries / "controllerd"),
            *_controller_args(launch_args, binaries, env),
            f"--controller-http-listen-addr=127.0.0.1:{control_port}",
            "--system-parameter-default=catalog_read_protection_publish_interval=10s",
            f"--pubsub-cpu-limit={os.cpu_count()}",
        ]
    else:
        with psycopg.connect(pg_url, autocommit=True) as pg:
            for schema in ["consensus", "tsoracle", "storage"]:
                pg.execute("CREATE SCHEMA " + schema)
        command = [
            str(binaries / "environmentd"),
            *_environment_args(
                launch_args, state, state / "scratch", proxy_addr="127.0.0.1"
            ),
            f'--clusterd-image={binaries/"clusterd"}',
            f"--persist-pubsub-url=http://127.0.0.1:{relay_port}",
        ]
    command += [
        f"--internal-persist-pubsub-listen-addr=127.0.0.1:{relay_port}",
        "--system-parameter-default=persist_shard_metrics=both",
        "--system-parameter-default=log_filter=info",
    ]
    command += [
        f"--system-parameter-default={value}" for value in args.system_parameter
    ]
    save(
        "launch.json",
        {
            "command": command,
            "listeners": listeners,
            "control": control,
            "environment": {
                k: env.get(k)
                for k in [
                    "RUST_MIN_STACK",
                    "MZ_LOG_FILTER",
                    "MZ_SOFT_ASSERTIONS",
                    "SYSTEM_PARAMETER_DEFAULT",
                ]
            },
        },
    )
    with (RUN / "runtime.log").open("w") as log:
        process = subprocess.Popen(
            command,
            cwd=ROOT,
            env=env,
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
    started = time.monotonic()
    deadline = started + 420
    while time.monotonic() < deadline:
        assert process.poll() is None, "server exited before readiness"
        try:
            if args.kind == "native":
                adapter = get(control + "/api/adapters")[0]
                if not (adapter.get("readiness") or {}).get("ready"):
                    time.sleep(1)
                    continue
                sql_addr = adapter["endpoints"]["sql-internal"][0]
            else:
                sql_addr = listeners["sql"]["internal"]["addr"]
            with connect(sql_addr) as c:
                assert c.execute("SELECT 1").fetchone() == (1,)
            break
        except (OSError, psycopg.Error, IndexError) as error:
            (RUN / "readiness-error.txt").write_text(str(error))
            time.sleep(1)
    else:
        raise TimeoutError("SQL readiness")
    result["readiness_seconds"] = time.monotonic() - started
    with connect(sql_addr) as c:
        c.execute("SET cluster=quickstart")
        c.execute("CREATE TABLE idle_probe (n INT)")
        c.execute("INSERT INTO idle_probe VALUES (1)")
        c.execute("CREATE DEFAULT INDEX ON idle_probe")
        assert c.execute("SELECT * FROM idle_probe").fetchall() == [(1,)]
        settings = {}
        for name in [
            "catalog_read_protection_publish_interval",
            "persist_shard_metrics",
            "log_filter",
            "enable_metric_sink",
            "persist_enable_incremental_compaction",
            "enable_columnar_accumulable_diff",
            "compute_temporal_bucketing_summary",
            "persist_blob_cache_mem_limit_bytes",
            "persist_pg_consensus_read_committed",
        ]:
            if name.startswith("catalog_read") and args.kind == "main":
                continue
            settings[name] = c.execute("SHOW " + name).fetchone()[0]
        for value in args.system_parameter:
            name = value.split("=", 1)[0]
            settings[name] = c.execute("SHOW " + name).fetchone()[0]
        save("effective-settings.json", settings)
        if args.kind == "native":
            assert (
                settings["catalog_read_protection_publish_interval"] == "10 s"
            ), settings
        assert settings["persist_shard_metrics"] == "both", settings
        save(
            "clusters.json",
            c.execute(
                "SELECT c.id,c.name,r.id,r.name,r.size FROM mz_catalog.mz_clusters c LEFT JOIN mz_catalog.mz_cluster_replicas r ON r.cluster_id=c.id ORDER BY c.id,r.id"
            ).fetchall(),
        )
        cursor = c.execute("SELECT * FROM mz_internal.mz_storage_shards")
        save(
            "shards.json",
            {
                "columns": [d.name for d in cursor.description],
                "rows": cursor.fetchall(),
            },
        )
        save(
            "object-names.json",
            c.execute(
                "SELECT g.global_id,o.id,s.name,o.name FROM mz_internal.mz_object_global_ids g JOIN mz_catalog.mz_objects o ON o.id=g.id JOIN mz_catalog.mz_schemas s ON s.id=o.schema_id ORDER BY g.global_id"
            ).fetchall(),
        )
    discover()
    sample(args.warmup, "warmup", pg_group)
    discover()
    capture("steady-0", pg_group)
    for i in range(args.windows):
        sample(args.window, f"steady-{i+1}", pg_group)
        capture(f"steady-{i+1}", pg_group)
    with connect(sql_addr) as c:
        assert c.execute("SELECT * FROM idle_probe").fetchall() == [(1,)]
    result["measurement_status"] = "passed"
    profile_errors = {}
    if args.profile_seconds:
        for role, record in endpoints.items():
            if record["name"] == "pubsubd":
                continue
            route = "/cpu" if record["name"] == "clusterd" else "/prof/cpu"
            try:
                curl(
                    record,
                    route,
                    RUN / f"{role}-cpu.pb.gz",
                    {"seconds": args.profile_seconds, "hz": 99, "merge_threads": False},
                )
            except (OSError, subprocess.SubprocessError) as error:
                profile_errors[role] = repr(error)
                continue
            (RUN / f"{role}-binary.txt").write_text(record["exe"] + "\n")
            print("profiled", role, flush=True)
            heap_route = "/heap" if record["name"] == "clusterd" else "/prof/heap"
            try:
                curl(record, heap_route, RUN / f"{role}-heap.pb.gz")
            except (OSError, subprocess.SubprocessError) as error:
                profile_errors[role + "-heap"] = repr(error)
    result["profile_errors"] = profile_errors
    if profile_errors:
        raise RuntimeError(f"profiling incomplete: {profile_errors}")
    if args.scheduling_diagnostics:
        with connect(sql_addr) as c:
            c.execute("SET cluster=mz_catalog_server")
            c.execute("SET statement_timeout='30s'")
            for index in range(2):
                if index:
                    time.sleep(60)
                started = time.monotonic()
                rows = c.execute(
                    "SELECT d.id, d.name, a.id, e.elapsed_ns "
                    "FROM mz_introspection.mz_dataflows d "
                    "JOIN mz_introspection.mz_dataflow_addresses a "
                    "ON a.address = LIST[d.id]::uint8 LIST "
                    "LEFT JOIN mz_introspection.mz_scheduling_elapsed e ON e.id = a.id "
                    "ORDER BY d.id"
                ).fetchall()
                save(
                    f"scheduling-{index}.json",
                    {
                        "started": started,
                        "finished": time.monotonic(),
                        "rows": rows,
                        "columns": ["dataflow_id", "name", "operator_id", "elapsed_ns"],
                        "timing": "Post-window, post-profile diagnostic. Not an idle measurement window.",
                    },
                )
                assert rows and any(
                    row[3] is not None for row in rows
                ), "scheduling counter snapshot is empty"
    result["status"] = "passed"
except BaseException as error:
    result.update(status="failed", error=repr(error))
    raise
finally:
    signal.alarm(0)
    try:
        try:
            stop()
        finally:
            if docker_attempted:
                stop_postgres()
    except BaseException as error:
        result.update(status="failed", cleanup_error=repr(error))
        raise
    finally:
        save("result.json", result)
        print(json.dumps(result), flush=True)
