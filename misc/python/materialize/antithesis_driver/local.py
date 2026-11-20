# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""The driver's local loop: run it for a while, then list what a run would fail.

`standalone` runs the driver on this machine against a fresh
`materialize/materialized` container: the SQL components, without
Kubernetes. `kind` deploys the same manifests Antithesis runs, with the
release's published environmentd, clusterd and orchestratord images and a
locally built driver image, into a kind cluster, and runs the driver pod
there, rollouts and pod deletions included.

Either way the driver writes its assertions to `sdk.jsonl` in `--output`, runs
its eventually after `--duration` seconds, and exits. Nothing injects faults
locally, so assertions on situations that need faults stay unmet, and in
`standalone` so do the Kubernetes ones.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

import yaml

from materialize import MZ_ROOT, spawn

DRIVER_IMAGE = "antithesis-driver"
STANDALONE_CONTAINER = "mz-antithesis-driver"
UPSTREAM_CONTAINER = "mz-antithesis-driver-pg"
BROKER_CONTAINER = "mz-antithesis-driver-redpanda"
MYSQL_CONTAINER = "mz-antithesis-driver-mysql"
SQL_SERVER_CONTAINER = "mz-antithesis-driver-mssql"
NETWORK = "mz-antithesis-driver"
LICENSE_KEY_FILE = Path.home() / ".config" / "materialize" / "antithesis-license-key"


class Kind:
    """One kind cluster, addressed through its kubectl context."""

    def __init__(self, name: str) -> None:
        self.name = name
        self.context = f"kind-{name}"

    def kubectl(
        self, *args: str, check: bool = True
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ["kubectl", "--context", self.context, *args],
            check=check,
            capture_output=True,
            text=True,
        )

    def ensure(self) -> None:
        if self.name not in spawn.capture(["kind", "get", "clusters"]).split():
            spawn.runv(["kind", "create", "cluster", "--name", self.name])

    def load(self, images: list[str]) -> None:

        for image in images:
            present = (
                subprocess.run(
                    ["docker", "image", "inspect", image], capture_output=True
                ).returncode
                == 0
            )
            if not present:
                pulled = subprocess.run(["docker", "pull", image]).returncode == 0
                if not pulled:
                    # SQL Server publishes only amd64, which runs under
                    # emulation on arm64.
                    spawn.runv(["docker", "pull", "--platform", "linux/amd64", image])
            result = subprocess.run(
                ["kind", "load", "docker-image", image, "--name", self.name],
                capture_output=True,
                text=True,
            )
            if result.returncode == 0:
                continue
            if not re.search(r"content digest .*: not found", result.stderr):
                raise subprocess.CalledProcessError(
                    result.returncode, result.args, result.stdout, result.stderr
                )
            # Docker's containerd store can keep a multi-platform index without
            # its other platforms' blobs. Export the platform pulled.
            platform = subprocess.run(
                [
                    "docker",
                    "image",
                    "inspect",
                    "--format",
                    "{{.Os}}/{{.Architecture}}",
                    image,
                ],
                capture_output=True,
                text=True,
                check=True,
            ).stdout.strip()
            with tempfile.TemporaryDirectory(
                prefix="mz-antithesis-image-"
            ) as directory:
                archive = Path(directory) / "image.tar"
                spawn.runv(
                    [
                        "docker",
                        "image",
                        "save",
                        "--platform",
                        platform,
                        "--output",
                        str(archive),
                        image,
                    ]
                )
                spawn.runv(
                    ["kind", "load", "image-archive", str(archive), "--name", self.name]
                )

    def apply(self, manifests: Path) -> None:
        # Namespaces first: everything else is namespaced.
        spawn.runv(
            [
                "kubectl",
                "--context",
                self.context,
                "apply",
                "-f",
                str(manifests / "00-namespaces.yaml"),
            ]
        )
        spawn.runv(
            ["kubectl", "--context", self.context, "apply", "-f", str(manifests)]
        )


def license_key() -> str:
    key = os.environ.get("MZ_CI_LICENSE_KEY")
    if not key and LICENSE_KEY_FILE.exists():
        key = LICENSE_KEY_FILE.read_text().strip()
    if not key:
        raise SystemExit(
            f"set MZ_CI_LICENSE_KEY or write the key to {LICENSE_KEY_FILE}"
        )
    return key


def would_fail(*jsonl: Path) -> list[str]:
    """Every assertion that would fail in a run with these events."""
    by_message: dict[str, list[dict]] = {}
    lines = [line for path in jsonl for line in path.read_text().splitlines()]
    for line in lines:
        record = json.loads(line).get("antithesis_assert")
        if record is not None:
            by_message.setdefault(record["message"], []).append(record)
    failing = []
    for message, records in sorted(by_message.items()):
        first = records[0]
        if first["assert_type"] == "sometimes":
            bad = not any(r["hit"] and r["condition"] for r in records)
        else:
            bad = any(r["hit"] and not r["condition"] for r in records) or (
                first["must_hit"] and not any(r["hit"] for r in records)
            )
        if bad:
            failing.append(f"{first['display_type']}\t{message}")
    return failing


def report(output: Path) -> int:
    sdk = output / "sdk.jsonl"
    if not sdk.exists():
        print(f"no assertions were written to {sdk}")
        return 1
    # The SDK writes nothing for an assertion the run never reached. The
    # catalog declares every call site, as Antithesis does from the image, so
    # a never-reached assertion is listed too.
    catalog = output / "catalog.jsonl"
    spawn.runv(
        [
            sys.executable,
            "-m",
            "antithesis.catalog",
            str(Path(__file__).parent),
            "-o",
            str(catalog),
        ]
    )
    failing = would_fail(catalog, sdk)
    print(f"assertions that would fail ({len(failing)}):")
    for line in failing:
        print(f"  {line}")
    return 0


def dependency(name: str) -> dict[str, Any]:
    """The container of a dependency's StatefulSet in the manifests, so the
    local loop runs it as Antithesis does."""
    from materialize.antithesis import render

    text = (render.MANIFESTS / "10-dependencies.yaml").read_text()
    for doc in yaml.safe_load_all(text):
        if doc and doc["kind"] == "StatefulSet" and doc["metadata"]["name"] == name:
            return doc["spec"]["template"]["spec"]["containers"][0]
    raise KeyError(name)


def standalone(args: argparse.Namespace) -> int:
    from materialize.antithesis import render

    output: Path = args.output
    output.mkdir(parents=True, exist_ok=True)
    (output / "sdk.jsonl").unlink(missing_ok=True)
    containers = [
        STANDALONE_CONTAINER,
        UPSTREAM_CONTAINER,
        BROKER_CONTAINER,
        MYSQL_CONTAINER,
        SQL_SERVER_CONTAINER,
    ]
    subprocess.run(["docker", "rm", "-f", *containers], capture_output=True)
    subprocess.run(["docker", "network", "rm", NETWORK], capture_output=True)
    spawn.runv(["docker", "network", "create", NETWORK])
    run = ["docker", "run", "-d", "--network", NETWORK, "--name"]
    spawn.runv(
        [
            *run,
            UPSTREAM_CONTAINER,
            "-p",
            "15432:5432",
            "-e",
            "POSTGRES_PASSWORD=postgres",
            "-e",
            "POSTGRES_DB=upstream",
            render.DEFAULT_IMAGES["POSTGRES_IMAGE"],
            "-c",
            "wal_level=logical",
            "-c",
            "max_wal_senders=20",
            "-c",
            "max_replication_slots=20",
        ]
    )
    # Materialize reaches the broker inside the network, the driver from the
    # host: one listener each.
    spawn.runv(
        [
            *run,
            BROKER_CONTAINER,
            "-p",
            "19092:19092",
            render.DEFAULT_IMAGES["REDPANDA_IMAGE"],
            "redpanda",
            "start",
            "--overprovisioned",
            "--smp=1",
            "--memory=512M",
            "--reserve-memory=0M",
            "--node-id=0",
            "--check=false",
            "--kafka-addr=internal://0.0.0.0:9092,external://0.0.0.0:19092",
            f"--advertise-kafka-addr=internal://{BROKER_CONTAINER}:9092,external://localhost:19092",
            "--set",
            "redpanda.enable_transactions=true",
            "--set",
            "redpanda.enable_idempotence=true",
            "--set",
            "redpanda.auto_create_topics_enabled=false",
        ]
    )
    mysql = dependency("mysql-upstream")
    spawn.runv(
        [
            *run,
            MYSQL_CONTAINER,
            "-p",
            "13306:3306",
            *(f"--env={e['name']}={e['value']}" for e in mysql["env"]),
            render.DEFAULT_IMAGES["MYSQL_IMAGE"],
            *mysql["args"],
        ]
    )
    sql_server = dependency("sql-server-upstream")
    spawn.runv(
        [
            *run,
            SQL_SERVER_CONTAINER,
            # The only image there is. It runs under emulation on arm64.
            "--platform",
            "linux/amd64",
            "-p",
            "11433:1433",
            *(f"--env={e['name']}={e['value']}" for e in sql_server["env"]),
            render.DEFAULT_IMAGES["SQL_SERVER_IMAGE"],
        ]
    )
    spawn.runv(
        [
            *run,
            STANDALONE_CONTAINER,
            "-p",
            "6875:6875",
            "-p",
            "6876:6876",
            "-p",
            "6877:6877",
            f"materialize/materialized:{args.release}",
        ]
    )
    active = output / "workload-active"
    active.touch()
    sa_password = next(
        e["value"] for e in sql_server["env"] if e["name"] == "MSSQL_SA_PASSWORD"
    )
    env = {
        **os.environ,
        "PYTHONPATH": str(MZ_ROOT / "misc" / "python"),
        "MZ_ANTITHESIS_SQL_HOST": "localhost",
        "MZ_ANTITHESIS_WORKLOAD_ACTIVE": str(active),
        "MZ_ANTITHESIS_REPLICA_SIZES": "25cc,100cc",
        "MZ_ANTITHESIS_EVENTUALLY_AFTER": str(args.duration),
        "MZ_ANTITHESIS_UPSTREAM_POSTGRES_URL": "postgres://postgres:postgres@localhost:15432/upstream",
        "MZ_ANTITHESIS_UPSTREAM_POSTGRES_MZ_HOST": f"{UPSTREAM_CONTAINER}:5432",
        "MZ_ANTITHESIS_UPSTREAM_MYSQL_URL": "mysql://root:mysqlpass@localhost:13306",
        "MZ_ANTITHESIS_UPSTREAM_MYSQL_MZ_HOST": f"{MYSQL_CONTAINER}:3306",
        "MZ_ANTITHESIS_UPSTREAM_SQL_SERVER_URL": f"mssql://SA:{sa_password}@localhost:11433",
        "MZ_ANTITHESIS_UPSTREAM_SQL_SERVER_MZ_HOST": f"{SQL_SERVER_CONTAINER}:1433",
        "MZ_ANTITHESIS_KAFKA_BROKER": "localhost:19092",
        "MZ_ANTITHESIS_KAFKA_MZ_BROKER": f"{BROKER_CONTAINER}:9092",
        "ANTITHESIS_SDK_LOCAL_OUTPUT": str(output / "sdk.jsonl"),
    }
    try:
        with open(output / "driver.log", "w") as log:
            result = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "materialize.antithesis_driver",
                    "--backend",
                    "default",
                    "--seed",
                    str(args.seed),
                ],
                env=env,
                stdout=log,
                stderr=subprocess.STDOUT,
                timeout=args.duration + args.eventually_timeout,
            )
        print(f"driver exited with {result.returncode}, log in {output / 'driver.log'}")
    finally:
        if not args.keep:
            subprocess.run(["docker", "rm", "-f", *containers], capture_output=True)
            subprocess.run(["docker", "network", "rm", NETWORK], capture_output=True)
    return report(output)


def kind(args: argparse.Namespace) -> int:
    from materialize.antithesis import render
    from materialize.mzbuild import Repository
    from materialize.xcompile import Arch

    harness = Kind(args.cluster_name)
    output: Path = args.output
    output.mkdir(parents=True, exist_ok=True)

    repo = Repository(MZ_ROOT, arch=Arch.host())
    deps = repo.resolve_dependencies([repo.images[DRIVER_IMAGE]])
    deps.acquire()
    driver_image = deps[DRIVER_IMAGE].spec()
    release = {
        name: f"docker.io/materialize/{name}:{args.release}"
        for name in ("environmentd", "clusterd", "orchestratord")
    }
    harness.ensure()
    harness.load([driver_image, *release.values(), *render.DEFAULT_IMAGES.values()])
    with tempfile.TemporaryDirectory(prefix="mz-antithesis-driver-") as directory:
        manifests = Path(directory) / "manifests"
        render.render(
            manifests,
            environmentd_image=release["environmentd"],
            clusterd_image=release["clusterd"],
            orchestratord_image=release["orchestratord"],
            workload_image=driver_image,
            license_key=license_key(),
        )
        # Local-loop settings, which the Antithesis config does not carry.
        pod = manifests / "30-driver.yaml"
        pod.write_text(
            pod.read_text().replace(
                "    env:\n",
                "    env:\n"
                "    - name: ANTITHESIS_SDK_LOCAL_OUTPUT\n"
                "      value: /dev/stdout\n"
                "    - name: MZ_ANTITHESIS_EVENTUALLY_AFTER\n"
                f'      value: "{args.duration}"\n',
                1,
            )
        )
        harness.kubectl(
            "delete",
            "pod",
            "-n",
            "materialize",
            "driver",
            "--ignore-not-found",
            check=False,
        )
        harness.apply(manifests)

    def logs() -> str:
        return harness.kubectl(
            "logs", "-n", "materialize", "driver", check=False
        ).stdout

    deadline = time.monotonic() + args.setup_timeout
    while "setup complete, waiting for" not in logs():
        if time.monotonic() > deadline:
            print(logs()[-4000:])
            raise TimeoutError("the driver did not report setup complete")
        time.sleep(10)
    print("driver reported setup complete, starting the workload")
    harness.kubectl(
        "exec",
        "-n",
        "materialize",
        "driver",
        "--",
        "/opt/antithesis/test/v1/driver/first_start_workload",
    )
    deadline = time.monotonic() + args.duration + args.eventually_timeout
    phase = ""
    while time.monotonic() < deadline:
        phase = harness.kubectl(
            "get",
            "pod",
            "-n",
            "materialize",
            "driver",
            "-o",
            "jsonpath={.status.phase}",
            check=False,
        ).stdout
        if phase in ("Succeeded", "Failed"):
            break
        time.sleep(10)
    # The SDK writes its events to stdout, the one output that outlives the
    # pod: the driver exits after its eventually.
    text = logs()
    (output / "driver.log").write_text(text)
    (output / "sdk.jsonl").write_text(
        "".join(
            line + "\n"
            for line in text.splitlines()
            if line.startswith('{"antithesis_')
        )
    )
    print(f"driver pod phase {phase}, log in {output / 'driver.log'}")
    return report(output)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--release", default="v26.51.0", help="published Materialize release to run"
    )
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--duration",
        type=float,
        default=300,
        help="seconds of workload before the eventually",
    )
    parser.add_argument("--eventually-timeout", type=float, default=1800)
    sub = parser.add_subparsers(dest="command", required=True)
    standalone_parser = sub.add_parser("standalone")
    standalone_parser.add_argument("--seed", type=int, default=1)
    standalone_parser.add_argument(
        "--keep", action="store_true", help="keep the container"
    )
    kind_parser = sub.add_parser("kind")
    kind_parser.add_argument("--cluster-name", default="mz-antithesis-driver")
    kind_parser.add_argument("--setup-timeout", type=float, default=2700)
    sub.add_parser("report")
    args = parser.parse_args()
    if args.command == "standalone":
        return standalone(args)
    if args.command == "kind":
        return kind(args)
    return report(args.output)


if __name__ == "__main__":
    sys.exit(main())
