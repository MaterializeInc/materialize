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

Loads `test/antithesis/mzcompose.py`, resolves every `mzbuild:` reference,
and dumps the resulting docker-compose dict to stdout. Antithesis pulls the
referenced images directly from public GHCR — no separate registry, no
re-tagging.

Image-reference policy:

  * Materialize-built images (`materialized`, `antithesis-workload`) are
    emitted as `ghcr.io/materializeinc/materialize/<name>:mzbuild-<fp>`.
    The fingerprint participates in `antithesis=True` so antithesis builds
    don't collide with regular builds.

  * Third-party `mzbuild` images (`postgres`, `minio`) are replaced with the
    public upstream image. Our mzbuild variants bake in test-friendly
    patches (eatmydata, no_fsync) that defeat Antithesis's fault injection;
    Antithesis runs against vanilla.

The script also strips mzcompose-only keys, host bind-mounts, and host-path
env vars that don't resolve inside the Antithesis sandbox, and inlines the
postgres bootstrap SQL into the entrypoint (the bind-mount path won't
exist).

Usage:
    bin/pyactivate test/antithesis/export-compose.py \\
        > test/antithesis/config/docker-compose.yaml
"""

import sys
from pathlib import Path
from typing import Any

import yaml

from materialize import MZ_ROOT
from materialize.mzbuild import Repository
from materialize.mzcompose.composition import Composition

# mzbuild image names that we publish to GHCR and want Antithesis to pull
# under our fingerprint. Everything else falls back to a public image.
MATERIALIZE_IMAGES = {"materialized", "antithesis-workload"}

# Public-image fallbacks for mzbuild images whose Materialize-specific
# customizations subvert Antithesis (eatmydata, fsync no-ops, etc.).
PUBLIC_FALLBACKS = {
    "postgres": "postgres:17.7",
    "minio": "minio/minio:latest",
}

# Header prepended to the generated YAML so check-copyright passes and
# readers know the file isn't hand-edited.
HEADER = """\
# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# GENERATED FILE — do not edit. Regenerate via:
#   bin/pyactivate test/antithesis/export-compose.py > test/antithesis/config/docker-compose.yaml
# Source of truth: test/antithesis/mzcompose.py.

"""


def resolve_mzbuild(svc: dict[str, Any], deps: Any) -> None:
    """Replace `mzbuild:` with a concrete `image:` ref."""
    name = svc.pop("mzbuild")
    if name in MATERIALIZE_IMAGES:
        svc["image"] = deps[name].spec()
    elif name in PUBLIC_FALLBACKS:
        svc["image"] = PUBLIC_FALLBACKS[name]
    else:
        raise ValueError(
            f"mzbuild image {name!r} has no Antithesis policy — add it to "
            f"MATERIALIZE_IMAGES (use our GHCR build) or PUBLIC_FALLBACKS "
            f"(swap to a public image) in export-compose.py."
        )


def inline_postgres_setup(svc: dict[str, Any]) -> None:
    """Replace the bind-mounted setup SQL with an inline entrypoint write.

    Antithesis has no host filesystem, so we can't mount the SQL file.
    Read it from misc/postgres/setup_materialize.sql (one source of truth)
    and bake it into the service entrypoint.
    """
    if not svc.get("image", "").startswith("postgres:"):
        return

    env = svc.setdefault("environment", [])
    # eatmydata isn't installed in the public postgres image.
    env[:] = [e for e in env if not e.startswith("LD_PRELOAD=")]
    # Trust auth — Antithesis-internal traffic only.
    env.append("POSTGRES_HOST_AUTH_METHOD=trust")

    # Drop the bind-mounted setup SQL; we'll inline it.
    vols = svc.get("volumes", [])
    vols[:] = [v for v in vols if "setup_materialize.sql" not in v]
    if not vols:
        svc.pop("volumes", None)

    setup_sql = (MZ_ROOT / "misc" / "postgres" / "setup_materialize.sql").read_text()
    # Strip comment lines + collapse to one statement per output line so we
    # can safely double-quote it inside the sh -c here.
    setup_sql = "\n".join(
        line for line in setup_sql.splitlines() if line and not line.startswith("--")
    )
    svc["entrypoint"] = [
        "sh",
        "-c",
        # `$$@` survives compose's $-interpolation and arrives as `$@` at the
        # shell, forwarding any args (e.g., the `postgres` CMD) verbatim.
        f"cat <<'SQL' > /docker-entrypoint-initdb.d/z_setup_materialize.sql\n"
        f"{setup_sql}\n"
        f"SQL\n"
        f'exec docker-entrypoint.sh "$$@"',
        "--",
    ]


def strip_host_bindmounts(svc: dict[str, Any]) -> None:
    """Drop volume entries that bind-mount a host path."""
    if "volumes" not in svc:
        return
    svc["volumes"] = [
        v
        for v in svc["volumes"]
        if not isinstance(v, str)
        or ":" not in v
        or not v.split(":", 1)[0].startswith("/")
    ]
    if not svc["volumes"]:
        del svc["volumes"]


def strip_incompatible_env(svc: dict[str, Any]) -> None:
    """Drop env vars that are unsafe or unresolvable under Antithesis.

    - `MZ_EAT_MY_DATA` enables `libeatmydata.so` (fsync no-op) — fatal for
      crash-recovery testing under fault injection.
    - `MZ_LISTENERS_CONFIG_PATH` and `MZ_EXTERNAL_LOGIN_PASSWORD_*` reference
      host paths or host secrets that don't exist in the sandbox.
    - Bare env vars (no `=`) inherit from the host environment, which is
      empty under Antithesis; drop them so materialized's built-in defaults
      apply.
    """
    if "environment" not in svc:
        return
    drop_prefixes = (
        "MZ_EAT_MY_DATA=",
        "MZ_LISTENERS_CONFIG_PATH=",
        "MZ_EXTERNAL_LOGIN_PASSWORD_",
    )
    svc["environment"] = [
        e for e in svc["environment"] if "=" in e and not e.startswith(drop_prefixes)
    ]


def strip_mzcompose_keys(svc: dict[str, Any]) -> None:
    """Drop keys understood by mzcompose but not by docker/podman compose."""
    for key in ("propagate_uid_gid", "allow_host_ports", "publish"):
        svc.pop(key, None)


def main() -> None:
    # munge_services=False keeps ports bare (e.g., `6875` instead of
    # `127.0.0.1::6875`) — Antithesis is container-to-container, no host
    # binding. We do our own mzbuild→image substitution below.
    repo = Repository(Path("."), arch="x86_64", antithesis=True)
    c = Composition(repo, "antithesis", munge_services=False)

    images = [
        repo.images[svc["mzbuild"]]
        for svc in c.compose["services"].values()
        if "mzbuild" in svc
    ]
    deps = repo.resolve_dependencies(images)

    for svc in c.compose["services"].values():
        svc["platform"] = "linux/amd64"
        if "mzbuild" in svc:
            resolve_mzbuild(svc, deps)
        inline_postgres_setup(svc)
        strip_host_bindmounts(svc)
        strip_incompatible_env(svc)
        strip_mzcompose_keys(svc)

    sys.stdout.write(HEADER)
    yaml.dump(c.compose, sys.stdout, default_flow_style=False, sort_keys=False)


if __name__ == "__main__":
    main()
