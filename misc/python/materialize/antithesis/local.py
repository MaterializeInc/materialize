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
import os
import subprocess
import sys
import time
from pathlib import Path

from materialize import MZ_ROOT, spawn
from materialize.antithesis import images as antithesis_images
from materialize.antithesis import render
from materialize.mz_version import MzVersion

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
        spawn.runv(["kind", "load", "docker-image", image, "--name", CLUSTER])


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
    build_dir: Path, skip_build: bool, upgrade_from: tuple[str, str] | None = None
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
    manifests = CONFIG_DIR / "manifests"
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


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    up_parser = sub.add_parser("up")
    up_parser.add_argument(
        "--build-dir",
        type=Path,
        default=MZ_ROOT,
        help="git checkout to build images from (mzbuild needs a git work tree)",
    )
    up_parser.add_argument("--skip-build", action="store_true")
    up_parser.add_argument(
        "--upgrade-from",
        metavar="RELEASE",
        help="start on this release's published images, such as v26.45.0, and upgrade from it",
    )
    sub.add_parser("down")
    args = parser.parse_args()
    if args.command == "up":
        upgrade_from = (
            (
                f"docker.io/materialize/environmentd:{args.upgrade_from}",
                f"docker.io/materialize/clusterd:{args.upgrade_from}",
            )
            if args.upgrade_from
            else None
        )
        up(args.build_dir, args.skip_build, upgrade_from)
    else:
        down()
    return 0


if __name__ == "__main__":
    sys.exit(main())
