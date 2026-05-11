#!/usr/bin/env python3

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

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
    "materialized": "materialize-materialized:latest",
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

    # Fixups for vanilla postgres (the mzbuild image has eatmydata, custom
    # pg_hba.conf, and baked-in init SQL — none of which exist in the public image).
    if svc.get("image", "").startswith("postgres:"):
        env = svc.get("environment", [])
        # Remove eatmydata — not installed in vanilla postgres
        env[:] = [e for e in env if not e.startswith("LD_PRELOAD=")]
        # Trust auth so materialized can connect as root without a password
        env.append("POSTGRES_HOST_AUTH_METHOD=trust")
        # Remove host bind-mount for setup SQL — won't exist in Antithesis.
        # Instead, inline the init SQL that creates the schemas materialized needs.
        vols = svc.get("volumes", [])
        vols[:] = [v for v in vols if "setup_materialize.sql" not in v]
        if not vols:
            del svc["volumes"]
        # Inline the init SQL as a script volume
        init_sql = (
            "CREATE ROLE root WITH LOGIN PASSWORD 'root';"
            "CREATE DATABASE root;"
            "GRANT ALL PRIVILEGES ON DATABASE root TO root;"
            r"\c root;"
            "CREATE SCHEMA IF NOT EXISTS consensus AUTHORIZATION root;"
            "CREATE SCHEMA IF NOT EXISTS adapter AUTHORIZATION root;"
            "CREATE SCHEMA IF NOT EXISTS storage AUTHORIZATION root;"
            "CREATE SCHEMA IF NOT EXISTS tsoracle AUTHORIZATION root;"
            "GRANT ALL PRIVILEGES ON SCHEMA public TO root;"
        )
        svc.setdefault("entrypoint", [])
        svc["entrypoint"] = ["sh", "-c", f"""
echo "{init_sql}" > /docker-entrypoint-initdb.d/z_setup_materialize.sql
exec docker-entrypoint.sh "$$@"
""".strip(), "--"]

    # Strip host bind-mounts — they won't resolve in Antithesis
    if "volumes" in svc:
        svc["volumes"] = [
            v for v in svc["volumes"]
            if not isinstance(v, str) or ":" not in v or not v.split(":")[0].startswith("/")
        ]
        if not svc["volumes"]:
            del svc["volumes"]

    # Remove env vars that point at host-only paths (the Docker image
    # entrypoint provides sensible defaults when these are unset)
    if "environment" in svc:
        svc["environment"] = [
            e for e in svc["environment"]
            if not e.startswith(("MZ_LISTENERS_CONFIG_PATH=", "MZ_EXTERNAL_LOGIN_PASSWORD_"))
        ]

    # Drop mzcompose-only keys that docker/podman compose doesn't understand
    for key in ["propagate_uid_gid", "allow_host_ports", "publish"]:
        svc.pop(key, None)

yaml.dump(c.compose, sys.stdout, default_flow_style=False, sort_keys=False)
