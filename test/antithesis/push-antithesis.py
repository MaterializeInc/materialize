#!/usr/bin/env python3

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Retag + push antithesis-flavored images to Antithesis's GCP registry.

Antithesis's sandbox pulls images by reference. Our standard mzbuild flow
publishes to GHCR with `mzbuild-<fp>` tags, but new GHCR packages default
to private visibility — Antithesis hits a 4001 (image-not-reachable) when
trying to pull them. Pushing to a GCP Artifact Registry whose IAM grants
Antithesis read access avoids the visibility dance entirely.

This script presumes `ci.test.build` has already run (so the source images
exist locally) and that `docker login` against the target registry has
already happened (build-antithesis.sh handles that via
GCP_SERVICE_ACCOUNT_JSON).

Usage:
    bin/pyactivate test/antithesis/push-antithesis.py \\
        --registry us-central1-docker.pkg.dev/molten-verve-216720/materialize-repository
"""

import argparse
from pathlib import Path

from materialize import spawn, ui
from materialize.mzbuild import Repository

# Images Antithesis needs to be able to pull:
#   - antithesis-config holds the docker-compose.yaml + .env Antithesis runs.
#   - materialized + antithesis-workload are referenced by that compose.
ANTITHESIS_IMAGES = ["materialized", "antithesis-workload", "antithesis-config"]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--registry",
        required=True,
        help="Antithesis registry prefix, e.g. us-central1-docker.pkg.dev/molten-verve-216720/materialize-repository",
    )
    args = parser.parse_args()

    # Match the Repository configuration used by ci.test.build so that
    # `deps[name].spec()` returns the same local tag that build actually
    # produced (materialize/<name>:mzbuild-<fp>, not the GHCR-prefixed one).
    repo = Repository(
        Path("."),
        arch="x86_64",
        antithesis=True,
        image_registry="materialize",
    )
    deps = repo.resolve_dependencies([repo.images[name] for name in ANTITHESIS_IMAGES])

    # Ensure each image is actually present locally before retag — ci.test.build's
    # `ensure()` path may short-circuit to "already pushed" without leaving a
    # local copy if the fingerprint was already in the cache.
    deps.acquire()

    for name in ANTITHESIS_IMAGES:
        resolved = deps[name]
        source = resolved.spec()
        target = f"{args.registry}/{name}:mzbuild-{resolved.fingerprint()}"
        ui.section(f"Pushing {name}")
        print(f"    source: {source}")
        print(f"    target: {target}")
        spawn.runv(["docker", "tag", source, target])
        spawn.runv(["docker", "push", target])


if __name__ == "__main__":
    main()
