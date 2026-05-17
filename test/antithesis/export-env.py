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

The compose YAML (export-compose.py) is committed with
`${MATERIALIZED_IMAGE}` / `${ANTITHESIS_WORKLOAD_IMAGE}` placeholders so
it stays stable across materialized source changes.  This script writes
the corresponding `.env` with the current mzbuild fingerprints so
compose can interpolate them at parse time.

`--group=NAME` is required because each Antithesis workload group has
its own per-group workload image (`antithesis-workload-<group>`).  The
emitted `ANTITHESIS_WORKLOAD_IMAGE` always resolves to the right per-
group image; the placeholder name in the compose YAML stays
group-agnostic.

Run at CI build time (build-antithesis.sh) and at local-dev `make build`.
The `antithesis-config-<group>` mzbuild image copies in the .env produced
by this script, so the config image's own fingerprint tracks the
materialized + per-group workload fingerprints transitively — same
materialized + same workload → same .env → same antithesis-config.

With `--registry`, the emitted refs use that registry prefix instead of
the default (whatever `spec()` returns based on `MZ_GHCR`). CI passes the
Antithesis GCP Artifact Registry so the compose Antithesis pulls
references images at the registry Antithesis can actually reach.

Usage:
    bin/pyactivate test/antithesis/export-env.py --group=kafka \\
        > test/antithesis/configs/kafka/.env
    bin/pyactivate test/antithesis/export-env.py --group=kafka \\
        --registry us-central1-docker.pkg.dev/molten-verve-216720/materialize-repository \\
        > test/antithesis/configs/kafka/.env
"""

import argparse
import sys
from pathlib import Path

from materialize.mzbuild import Repository
from materialize.xcompile import Arch

sys.path.insert(0, str(Path(__file__).resolve().parent))
from groups import load_manifest  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--group",
        required=True,
        help=(
            "Workload group whose per-group `antithesis-workload-<group>` "
            "image fingerprint should be emitted as ANTITHESIS_WORKLOAD_IMAGE."
        ),
    )
    parser.add_argument(
        "--registry",
        default=None,
        help=(
            "Registry prefix to use for emitted refs. If unset, uses the "
            "default `spec()` (GHCR when MZ_GHCR=1, else Docker Hub)."
        ),
    )
    parser.add_argument(
        "--no-antithesis",
        action="store_true",
        help=(
            "Emit non-antithesis-flavored image fingerprints. Used by the "
            "`make build-local` workflow that brings the compose up without "
            "the Antithesis platform — the antithesis flavor needs a "
            "libvoidstar.so we don't have locally, and the antithesis-only "
            "deps (testdrive in particular) aren't published with the "
            "antithesis flavor yet. CI sets neither (antithesis stays on)."
        ),
    )
    args = parser.parse_args()

    # Fail loud if the group isn't in the manifest.
    load_manifest().group(args.group)

    # Mapping of `.env` variable name → mzbuild image name.  Keep in
    # sync with MATERIALIZE_IMAGES in export-compose.py.
    env_vars = {
        "MATERIALIZED_IMAGE": "materialized",
        "ANTITHESIS_WORKLOAD_IMAGE": f"antithesis-workload-{args.group}",
    }

    # Antithesis itself runs amd64-only, so the Antithesis-targeted build
    # (CI default) is always x86_64. For local-dev `--no-antithesis` we use
    # the host arch instead so the compose stack runs natively without
    # rosetta/qemu emulation (which segfaults inside testdrive on Apple
    # Silicon).
    arch = Arch.host() if args.no_antithesis else Arch.X86_64
    repo = Repository(Path("."), arch=arch, antithesis=not args.no_antithesis)
    images = [repo.images[name] for name in env_vars.values()]
    deps = repo.resolve_dependencies(images)

    sys.stdout.write(
        "# GENERATED FILE — do not edit. Regenerate via:\n"
        f"#   bin/pyactivate test/antithesis/export-env.py --group={args.group} > \\\n"
        f"#     test/antithesis/configs/{args.group}/.env\n"
        f"# Consumed by test/antithesis/configs/{args.group}/docker-compose.yaml.\n"
    )
    for var, image_name in env_vars.items():
        if args.registry:
            ref = (
                f"{args.registry}/{image_name}:mzbuild-{deps[image_name].fingerprint()}"
            )
        else:
            ref = deps[image_name].spec()
        sys.stdout.write(f"{var}={ref}\n")


if __name__ == "__main__":
    main()
