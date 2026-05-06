#!/usr/bin/env python3
"""Export the resolved docker-compose YAML for the Antithesis composition.

Loads the mzcompose composition and dumps the compose dict to stdout as
YAML — without building any images or requiring a running Docker daemon.

mzbuild references are replaced with public images where possible,
or local tags for images that must be built (e.g. the workload).

Usage:
    bin/pyactivate test/antithesis/export-compose.py > antithesis/config/docker-compose.yaml
"""

import sys
from pathlib import Path

import yaml

from materialize.mzbuild import Repository
from materialize.mzcompose.composition import Composition

# Map mzbuild names → image references for the Antithesis compose.
# Public images for infra; local build tag for the workload.
MZBUILD_TO_IMAGE = {
    "materialized": "materialize/materialized:latest",
    "postgres": "postgres:17.7",
    "minio": "minio/minio:latest",
    "antithesis-workload": "materialize-workload:latest",
}

repo = Repository(Path("."), arch="x86_64")
c = Composition(repo, "antithesis", munge_services=False)

for name, svc in c.compose["services"].items():
    svc["platform"] = "linux/amd64"

    if "mzbuild" in svc:
        mzbuild_name = svc.pop("mzbuild")
        if mzbuild_name not in MZBUILD_TO_IMAGE:
            print(
                f"warning: no image mapping for mzbuild {mzbuild_name!r}, "
                f"using {mzbuild_name}:latest",
                file=sys.stderr,
            )
            svc["image"] = f"{mzbuild_name}:latest"
        else:
            svc["image"] = MZBUILD_TO_IMAGE[mzbuild_name]

    # Vanilla postgres needs trust auth to match the mzbuild image behavior
    # (materialized connects as root with no password)
    if svc.get("image", "").startswith("postgres:"):
        svc.setdefault("environment", []).append("POSTGRES_HOST_AUTH_METHOD=trust")

    # Drop mzcompose-only keys that docker/podman compose doesn't understand
    for key in ["propagate_uid_gid", "allow_host_ports", "publish"]:
        svc.pop(key, None)

yaml.dump(c.compose, sys.stdout, default_flow_style=False, sort_keys=False)
