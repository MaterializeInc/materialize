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
`standalone` so do the Kubernetes ones. A rollout needs more than ten
minutes of stable clusters before it is promoted, see
`with_0dt_caught_up_check_stability_period`.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path

from materialize import MZ_ROOT, spawn

DRIVER_IMAGE = "antithesis-driver"
STANDALONE_CONTAINER = "mz-antithesis-driver"


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


def standalone(args: argparse.Namespace) -> int:
    output: Path = args.output
    output.mkdir(parents=True, exist_ok=True)
    (output / "sdk.jsonl").unlink(missing_ok=True)
    subprocess.run(["docker", "rm", "-f", STANDALONE_CONTAINER], capture_output=True)
    spawn.runv(
        [
            "docker",
            "run",
            "-d",
            "--name",
            STANDALONE_CONTAINER,
            "-p",
            "6875:6875",
            "-p",
            "6877:6877",
            f"materialize/materialized:{args.release}",
        ]
    )
    active = output / "workload-active"
    active.touch()
    env = {
        **os.environ,
        "PYTHONPATH": str(MZ_ROOT / "misc" / "python"),
        "MZ_ANTITHESIS_SQL_HOST": "localhost",
        "MZ_ANTITHESIS_WORKLOAD_ACTIVE": str(active),
        "MZ_ANTITHESIS_REPLICA_SIZES": "25cc,100cc",
        "MZ_ANTITHESIS_EVENTUALLY_AFTER": str(args.duration),
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
            subprocess.run(
                ["docker", "rm", "-f", STANDALONE_CONTAINER], capture_output=True
            )
    return report(output)


def kind(args: argparse.Namespace) -> int:
    from materialize.antithesis import local as harness
    from materialize.antithesis import render
    from materialize.mzbuild import Repository
    from materialize.xcompile import Arch

    harness.CLUSTER = args.cluster_name
    harness.CONTEXT = f"kind-{args.cluster_name}"
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
    harness.ensure_cluster()
    harness.load_images(
        [driver_image, *release.values(), *render.DEFAULT_IMAGES.values()]
    )
    with tempfile.TemporaryDirectory(prefix="mz-antithesis-driver-") as directory:
        manifests = Path(directory) / "manifests"
        render.render(
            manifests,
            environmentd_image=release["environmentd"],
            clusterd_image=release["clusterd"],
            orchestratord_image=release["orchestratord"],
            workload_image=driver_image,
            license_key=harness.license_key(),
            workload="driver",
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
