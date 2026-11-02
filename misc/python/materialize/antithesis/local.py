# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Run the Antithesis Kubernetes harness on a local kind cluster.

`up` builds the images (Antithesis flavor), gives environmentd and clusterd a
shared tag (orchestratord derives the clusterd image from the environmentd
reference), loads everything into kind, renders the manifests, applies them,
and waits for the workload pod to report `setup_complete`. `down` deletes the
cluster. Local success is a strong signal for, not a guarantee of, success in
Antithesis: kind runs on many cores and injects no faults.

Requires `kind`, `kubectl`, `helm`, `docker`, and a license key in
`MZ_CI_LICENSE_KEY` or in `~/.config/materialize/antithesis-license-key`.
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
import uuid
from pathlib import Path

from materialize import MZ_ROOT, spawn
from materialize.antithesis import images as antithesis_images
from materialize.antithesis import render
from materialize.antithesis.local_scheduler import nonnegative_seconds
from materialize.mz_version import MzVersion
from materialize.xcompile import Arch

CLUSTER = "mz-antithesis"
CONTEXT = f"kind-{CLUSTER}"
LOCAL_TAG = antithesis_images.image_tag(MzVersion.parse_cargo(), "antithesis.local")
LOCAL_PREFIX = "localhost/materialize"
CONFIG_DIR = MZ_ROOT / "test" / "antithesis" / "config"
SETUP_TIMEOUT = 45 * 60


def kubectl(*args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["kubectl", "--context", CONTEXT, *args],
        check=check,
        capture_output=True,
        text=True,
    )


def ensure_cluster() -> None:
    clusters = spawn.capture(["kind", "get", "clusters"]).split()
    if CLUSTER not in clusters:
        spawn.runv(["kind", "create", "cluster", "--name", CLUSTER])


def load_images(images: list[str]) -> None:
    for image in images:
        present = (
            subprocess.run(
                ["docker", "image", "inspect", image], capture_output=True
            ).returncode
            == 0
        )
        if not present:
            spawn.runv(["docker", "pull", image])
        result = subprocess.run(
            ["kind", "load", "docker-image", image, "--name", CLUSTER],
            capture_output=True,
            text=True,
        )
        if result.returncode == 0:
            continue
        if not re.search(r"content digest .*: not found", result.stderr):
            raise subprocess.CalledProcessError(
                result.returncode, result.args, result.stdout, result.stderr
            )
        # Docker's containerd store can retain a multi-platform index without
        # its other platform blobs. Export the host platform before importing.
        with tempfile.TemporaryDirectory(prefix="mz-antithesis-image-") as directory:
            archive = Path(directory) / "image.tar"
            spawn.runv(
                [
                    "docker",
                    "image",
                    "save",
                    "--platform",
                    f"linux/{Arch.host().go_str()}",
                    "--output",
                    str(archive),
                    image,
                ]
            )
            spawn.runv(
                ["kind", "load", "image-archive", str(archive), "--name", CLUSTER]
            )


def apply(manifests: Path) -> None:
    # Namespaces first: everything else is namespaced.
    spawn.runv(
        [
            "kubectl",
            "--context",
            CONTEXT,
            "apply",
            "-f",
            str(manifests / "00-namespaces.yaml"),
        ]
    )
    spawn.runv(["kubectl", "--context", CONTEXT, "apply", "-f", str(manifests)])


def wait_for_setup() -> None:
    deadline = time.monotonic() + SETUP_TIMEOUT
    while time.monotonic() < deadline:
        result = kubectl(
            "-n",
            "materialize",
            "get",
            "deployment",
            "workload",
            "-o",
            "jsonpath={.status.readyReplicas}",
            check=False,
        )
        if result.stdout.strip() == "1":
            print("workload reported setup_complete")
            return
        time.sleep(10)
    raise TimeoutError("workload did not reach setup_complete")


LICENSE_KEY_FILE = Path.home() / ".config" / "materialize" / "antithesis-license-key"


def license_key() -> str:
    key = os.environ.get("MZ_CI_LICENSE_KEY")
    if not key and LICENSE_KEY_FILE.exists():
        key = LICENSE_KEY_FILE.read_text().strip()
    if not key:
        raise SystemExit(
            f"set MZ_CI_LICENSE_KEY or write the key to {LICENSE_KEY_FILE}"
        )
    return key


def up(
    build_dir: Path,
    skip_build: bool,
    upgrade_from: tuple[str, str] | None = None,
    manifests_dir: Path | None = None,
) -> None:
    """Bring the harness up. `upgrade_from` is as for `render.render`."""
    key = license_key()
    if skip_build:
        images = {
            name: f"{LOCAL_PREFIX}/{name}:{LOCAL_TAG}"
            for name in antithesis_images.IMAGES
        }
    else:
        images = antithesis_images.publish(
            antithesis_images.acquire(build_dir), LOCAL_PREFIX, LOCAL_TAG, push=False
        )
    ensure_cluster()
    load_images(
        [*images.values(), *render.DEFAULT_IMAGES.values(), *(upgrade_from or ())]
    )
    manifests = manifests_dir or CONFIG_DIR / CLUSTER / "manifests"
    render.render(
        manifests,
        environmentd_image=images["environmentd"],
        clusterd_image=images["clusterd"],
        orchestratord_image=images["orchestratord"],
        workload_image=images["antithesis-workload"],
        license_key=key,
        upgrade_from=upgrade_from,
    )
    apply(manifests)
    wait_for_setup()


def down() -> None:
    spawn.runv(["kind", "delete", "cluster", "--name", CLUSTER])


def run(args: argparse.Namespace) -> int:
    """Run one bounded scheduler in the workload pod and copy its evidence."""
    pod_data = json.loads(
        kubectl(
            "-n", args.namespace, "get", "pods", "-l", "app=workload", "-o", "json"
        ).stdout
    )
    pods = [
        pod
        for pod in pod_data["items"]
        if not pod["metadata"].get("deletionTimestamp")
        and pod.get("status", {}).get("phase") == "Running"
    ]
    if len(pods) != 1:
        raise RuntimeError(
            "expected exactly one running workload pod in the selected cluster"
        )
    pod = pods[0]["metadata"]["name"]
    remote = f"/var/lib/antithesis-workload/local-runs/{uuid.uuid4().hex}"
    args.output.mkdir(parents=True, mode=0o700, exist_ok=False)
    (args.output / "local-run-start.json").write_text(
        json.dumps(
            {
                "cluster": CLUSTER,
                "context": CONTEXT,
                "pod": pod,
                "remote_output": remote,
            },
            indent=2,
        )
        + "\n"
    )
    command = [
        "kubectl",
        "--context",
        CONTEXT,
        "-n",
        args.namespace,
        "exec",
        pod,
        "-c",
        "workload",
        "--",
        "python3",
        "-u",
        "-m",
        "materialize.antithesis.local_runner",
        "--output",
        remote,
        "--profile",
        args.profile,
        "--duration",
        str(args.duration),
        "--concurrency",
        str(args.concurrency),
        "--seed",
        str(args.seed),
        "--command-timeout",
        str(args.command_timeout),
        "--setup-timeout",
        str(args.setup_timeout),
        "--shutdown-grace",
        str(args.shutdown_grace),
        "--drain-timeout",
        str(args.drain_timeout),
        "--check-interval",
        str(args.check_interval),
        "--fault-interval",
        str(args.fault_interval),
    ]
    for name in ("include", "check", "require_witness"):
        for value in getattr(args, name):
            command.extend(("--" + name.replace("_", "-"), value))
    if args.skip_first:
        command.append("--skip-first")
    with (args.output / "scheduler.log").open("w") as log:
        result = subprocess.run(command, stdout=log, stderr=subprocess.STDOUT)
    copied = kubectl(
        "-n",
        args.namespace,
        "cp",
        "-c",
        "workload",
        f"{pod}:{remote}/.",
        str(args.output),
        check=False,
    )
    if copied.returncode:
        raise RuntimeError(
            f"could not copy local run evidence: {copied.stderr[-2000:]}"
        )
    receipt = {
        "cluster": CLUSTER,
        "context": CONTEXT,
        "namespace": args.namespace,
        "pod": pod,
        "remote_output": remote,
        "profile": args.profile,
        "concurrency": args.concurrency,
        "seed": args.seed,
        "fault_interval": args.fault_interval,
        "returncode": result.returncode,
    }
    (args.output / "local-run.json").write_text(json.dumps(receipt, indent=2) + "\n")
    summary = args.output / "summary.json"
    print(summary if summary.exists() else args.output / "setup-failed.json")
    return result.returncode


def main() -> int:
    global CLUSTER, CONTEXT
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--cluster-name", required=True, help="explicit isolated kind cluster name"
    )
    sub = parser.add_subparsers(dest="command", required=True)
    up_parser = sub.add_parser("up")
    up_parser.add_argument(
        "--build-dir",
        type=Path,
        default=MZ_ROOT,
        help="git checkout to build images from (mzbuild needs a git work tree)",
    )
    up_parser.add_argument("--skip-build", action="store_true")
    up_parser.add_argument("--manifests-dir", type=Path)
    up_parser.add_argument(
        "--upgrade-from",
        metavar="RELEASE",
        help="start on this release's published images, such as v26.45.0, and upgrade from it",
    )
    sub.add_parser("down")
    run_parser = sub.add_parser("run")
    run_parser.add_argument("--namespace", default="materialize")
    run_parser.add_argument("--profile", default="transactions")
    run_parser.add_argument("--output", type=Path, required=True)
    run_parser.add_argument("--duration", type=float, default=120)
    run_parser.add_argument("--concurrency", type=int, default=8)
    run_parser.add_argument("--seed", type=int, default=1)
    run_parser.add_argument("--command-timeout", type=float, default=180)
    run_parser.add_argument("--setup-timeout", type=float, default=600)
    run_parser.add_argument("--shutdown-grace", type=float, default=2)
    run_parser.add_argument("--drain-timeout", type=float, default=60)
    run_parser.add_argument("--check-interval", type=float, default=5)
    run_parser.add_argument(
        "--fault-interval",
        type=nonnegative_seconds,
        default=30,
        help="seconds after an ordinary pod-fault command finishes before admitting the next (0 disables pacing)",
    )
    run_parser.add_argument("--include", action="append", default=[])
    run_parser.add_argument("--check", action="append", default=[])
    run_parser.add_argument("--require-witness", action="append", default=[])
    run_parser.add_argument("--skip-first", action="store_true")
    args = parser.parse_args()
    if not re.fullmatch(r"[a-z0-9](?:[a-z0-9-]*[a-z0-9])?", args.cluster_name):
        parser.error(
            "cluster name must be lowercase alphanumeric with internal hyphens"
        )
    CLUSTER = args.cluster_name
    CONTEXT = f"kind-{CLUSTER}"
    if args.command == "up":
        upgrade_from = (
            (
                f"docker.io/materialize/environmentd:{args.upgrade_from}",
                f"docker.io/materialize/clusterd:{args.upgrade_from}",
            )
            if args.upgrade_from
            else None
        )
        up(args.build_dir, args.skip_build, upgrade_from, args.manifests_dir)
    elif args.command == "down":
        down()
    else:
        if (
            args.concurrency < 1
            or min(
                args.duration,
                args.command_timeout,
                args.setup_timeout,
                args.shutdown_grace,
            )
            <= 0
        ):
            parser.error("positive duration/timeouts and concurrency >= 1 are required")
        return run(args)
    return 0


if __name__ == "__main__":
    sys.exit(main())
