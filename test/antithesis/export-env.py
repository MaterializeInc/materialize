#!/usr/bin/env python3

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Emit the `.env` file consumed by Antithesis's docker-compose.yaml.

The compose YAML (export-compose.py) is committed with `${MATERIALIZED_IMAGE}`
/ `${ANTITHESIS_WORKLOAD_IMAGE}` placeholders so it stays stable across
materialized source changes. This script writes the corresponding `.env`
with the current mzbuild fingerprints so compose can interpolate them.

Run at CI build time (build-antithesis.sh) and at local-dev `make build`.
The `antithesis-config` mzbuild image copies in the .env produced by this
script, so the image's own fingerprint tracks the materialized fingerprint
transitively — same materialized → same .env → same antithesis-config.

With `--registry`, the emitted refs use that registry prefix instead of
the default (whatever `spec()` returns based on `MZ_GHCR`). CI passes the
Antithesis GCP Artifact Registry so the compose Antithesis pulls
references images at the registry Antithesis can actually reach.

Usage:
    bin/pyactivate test/antithesis/export-env.py \\
        > test/antithesis/config/.env
    bin/pyactivate test/antithesis/export-env.py \\
        --registry us-central1-docker.pkg.dev/molten-verve-216720/materialize-repository \\
        > test/antithesis/config/.env
"""

import argparse
import sys
from pathlib import Path

from materialize.mzbuild import Repository
from materialize.xcompile import Arch

# Mapping of `.env` variable name → mzbuild image name. Keep in sync with
# MATERIALIZE_IMAGES in export-compose.py.
ENV_VARS = {
    "MATERIALIZED_IMAGE": "materialized",
    "ANTITHESIS_WORKLOAD_IMAGE": "antithesis-workload",
}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--registry",
        default=None,
        help=(
            "Registry prefix to use for emitted refs. If unset, uses the "
            "default `spec()` (GHCR when MZ_GHCR=1, else Docker Hub)."
        ),
    )
    args = parser.parse_args()

    repo = Repository(Path("."), arch=Arch.X86_64, antithesis=True)
    images = [repo.images[name] for name in ENV_VARS.values()]
    deps = repo.resolve_dependencies(images)

    sys.stdout.write(
        "# GENERATED FILE — do not edit. Regenerate via:\n"
        "#   bin/pyactivate test/antithesis/export-env.py > test/antithesis/config/.env\n"
        "# Consumed by test/antithesis/config/docker-compose.yaml at compose-parse time.\n"
    )
    for var, image_name in ENV_VARS.items():
        if args.registry:
            ref = f"{args.registry}/{image_name}:mzbuild-{deps[image_name].fingerprint()}"
        else:
            ref = deps[image_name].spec()
        sys.stdout.write(f"{var}={ref}\n")


if __name__ == "__main__":
    main()
