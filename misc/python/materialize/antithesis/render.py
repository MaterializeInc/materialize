# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Render the Antithesis Kubernetes manifests.

Fills the templates in `test/antithesis/manifests/` with image references and
the license key, renders the orchestratord Helm chart with
`test/antithesis/manifests/operator-values.yaml`, and writes plain manifests to
the output directory. That directory is what `snouty validate` checks and what
the Antithesis config image ships under `/manifests`.

The license key comes from `MZ_CI_LICENSE_KEY` and only ever lands in the
rendered output, which must not be committed.
"""

from __future__ import annotations

import argparse
import json
import os
import string
import subprocess
import sys
from pathlib import Path

from materialize import MZ_ROOT

MANIFESTS = MZ_ROOT / "test" / "antithesis" / "manifests"
OPERATOR_CHART = MZ_ROOT / "misc" / "helm-charts" / "operator"
ENVIRONMENT_NAME = "antithesis"

DEFAULT_IMAGES = {
    "POSTGRES_IMAGE": "docker.io/library/postgres:17.6",
    "MINIO_IMAGE": "docker.io/materialize/minio:v26.42.0",
    "REDPANDA_IMAGE": "docker.io/redpandadata/redpanda:v26.1.8",
}

# System parameters every timeline starts from. Per-timeline variation is the
# workload's job (it rewrites the `system-params` ConfigMap); these are only
# the values the harness itself depends on.
BASE_SYSTEM_PARAMETERS: dict[str, str] = {
    # One node: replicas of one cluster must be allowed to share it.
    "cluster_soften_replication_anti_affinity": "true",
    "allowed_cluster_replica_sizes": "'antithesis-1', 'antithesis-2', 'antithesis-4'",
    "max_connections": "1000",
    # Lets the client-history driver run sessions at strong session
    # serializable, the only isolation level with per-session guarantees.
    "enable_session_timelines": "true",
    # Real-time recency is off by default; the canary and session-mix drivers
    # issue real-time-recency reads in every profile.
    "allow_real_time_recency": "true",
    # A read-only generation reboots when it sees DDL it must react to. The
    # production check interval (5 min) and max wait (1 year) would make every
    # rollout outlast a timeline, so both are scaled down here. These two are
    # defaults: `configure` overrides them per timeline.
    "with_0dt_deployment_ddl_check_interval": "30s",
    "with_0dt_deployment_max_wait": "20min",
}
# Keys of `BASE_SYSTEM_PARAMETERS` a timeline's configuration may override.
OVERRIDABLE_BASE_PARAMETERS = frozenset(
    {"with_0dt_deployment_ddl_check_interval", "with_0dt_deployment_max_wait"}
)


def split_image(ref: str) -> tuple[str, str]:
    repository, sep, tag = ref.rpartition(":")
    if not sep or "/" in tag:
        raise ValueError(f"image reference needs an explicit tag: {ref}")
    return repository, tag


def check_image_pair(environmentd_image: str, clusterd_image: str) -> None:
    envd_repo, envd_tag = split_image(environmentd_image)
    clusterd_repo, clusterd_tag = split_image(clusterd_image)
    # orchestratord derives the clusterd image from the environmentd reference
    # by swapping the image name and keeping the repository prefix and tag.
    if (
        envd_repo.rsplit("/", 1)[0] != clusterd_repo.rsplit("/", 1)[0]
        or envd_tag != clusterd_tag
    ):
        raise ValueError(
            "environmentd and clusterd images must share a repository prefix and tag"
        )


def render(
    output: Path,
    environmentd_image: str,
    clusterd_image: str,
    orchestratord_image: str,
    workload_image: str,
    license_key: str,
    upgrade_from: tuple[str, str] | None = None,
) -> None:
    """Write the manifests.

    `upgrade_from` is an (environmentd, clusterd) image pair at an older
    release. When set, the environment starts on it and the rollouts driver
    upgrades it to `environmentd_image`.
    """
    check_image_pair(environmentd_image, clusterd_image)
    if upgrade_from is not None:
        check_image_pair(*upgrade_from)
    initial_environmentd, initial_clusterd = upgrade_from or (
        environmentd_image,
        clusterd_image,
    )
    orchestratord_repo, orchestratord_tag = split_image(orchestratord_image)

    values = {
        **DEFAULT_IMAGES,
        "ENVIRONMENTD_IMAGE": environmentd_image,
        "CLUSTERD_IMAGE": clusterd_image,
        "INITIAL_ENVIRONMENTD_IMAGE": initial_environmentd,
        "INITIAL_CLUSTERD_IMAGE": initial_clusterd,
        "ORCHESTRATORD_REPOSITORY": orchestratord_repo,
        "ORCHESTRATORD_TAG": orchestratord_tag,
        "WORKLOAD_IMAGE": workload_image,
        "LICENSE_KEY": license_key,
        "ENVIRONMENT_NAME": ENVIRONMENT_NAME,
        "SYSTEM_PARAMS_JSON": json.dumps(BASE_SYSTEM_PARAMETERS),
    }

    output.mkdir(parents=True, exist_ok=True)
    for stale in output.glob("*.yaml"):
        stale.unlink()

    for template in sorted(MANIFESTS.glob("*.yaml")):
        text = string.Template(template.read_text()).substitute(values)
        if template.name == "operator-values.yaml":
            values_file = output.parent / "operator-values.rendered.yaml"
            values_file.write_text(text)
            operator = subprocess.run(
                [
                    "helm",
                    "template",
                    "operator",
                    str(OPERATOR_CHART),
                    "--namespace",
                    "materialize",
                    "--values",
                    str(values_file),
                ],
                check=True,
                capture_output=True,
                text=True,
            ).stdout
            values_file.unlink()
            (output / "05-operator.yaml").write_text(operator)
        else:
            (output / template.name).write_text(text)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--environmentd-image", required=True)
    parser.add_argument("--clusterd-image", required=True)
    parser.add_argument("--orchestratord-image", required=True)
    parser.add_argument("--workload-image", required=True)
    parser.add_argument(
        "--upgrade-from-environmentd-image",
        help="start on this older release and upgrade to --environmentd-image",
    )
    parser.add_argument("--upgrade-from-clusterd-image")
    args = parser.parse_args()
    if (args.upgrade_from_environmentd_image is None) != (
        args.upgrade_from_clusterd_image is None
    ):
        parser.error("the --upgrade-from-* images must be given together")

    license_key = os.environ.get("MZ_CI_LICENSE_KEY")
    if not license_key:
        print("MZ_CI_LICENSE_KEY must be set", file=sys.stderr)
        return 1

    render(
        args.output,
        environmentd_image=args.environmentd_image,
        clusterd_image=args.clusterd_image,
        orchestratord_image=args.orchestratord_image,
        workload_image=args.workload_image,
        license_key=license_key,
        upgrade_from=(
            (args.upgrade_from_environmentd_image, args.upgrade_from_clusterd_image)
            if args.upgrade_from_environmentd_image
            else None
        ),
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
