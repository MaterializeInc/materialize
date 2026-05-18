#!/usr/bin/env python3

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Export a per-workload-group docker-compose YAML for Antithesis.

`test/antithesis/mzcompose.py` defines a kitchen-sink topology — every
service every workload could possibly need. This script picks one
`--group=NAME` from `test/antithesis/groups.yaml`, filters
`compose["services"]` down to that group, prunes any `depends_on`
references that no longer resolve, injects `ANTITHESIS_WORKLOAD_GROUP`
on the workload service, and emits the result.

Materialize-built images become `${MATERIALIZED_IMAGE}` /
`${ANTITHESIS_WORKLOAD_IMAGE}` placeholders so the committed YAML stays
stable across materialized source changes — only `.env` shifts per
fingerprint.

Image-reference policy:

  * Materialize-built images (`materialized`, `antithesis-workload`)
    become `${MATERIALIZED_IMAGE}` / `${ANTITHESIS_WORKLOAD_IMAGE}`.
    Compose interpolates them from `.env` at parse time. The actual specs
    are `ghcr.io/materializeinc/materialize/<name>:mzbuild-<fp>` with
    `antithesis=True` participating in the fingerprint.

  * Third-party `mzbuild` images (`postgres`, `minio`) are replaced with
    the public upstream image. Our mzbuild variants bake in test-friendly
    patches (eatmydata, no_fsync) that defeat Antithesis's fault injection;
    Antithesis runs against vanilla.

The script also strips mzcompose-only keys, host bind-mounts, and host-path
env vars that don't resolve inside the Antithesis sandbox, and inlines the
postgres bootstrap SQL into the entrypoint (the bind-mount path won't
exist).

Usage:
    bin/pyactivate test/antithesis/export-compose.py --group=kafka \\
        > test/antithesis/configs/kafka/docker-compose.yaml
"""

import argparse
import sys
from pathlib import Path
from typing import Any

import yaml

from materialize import MZ_ROOT
from materialize.mzbuild import Repository
from materialize.mzcompose.composition import Composition
from materialize.xcompile import Arch

# `test/antithesis/groups.py` lives next to this script in the source
# tree, not under the `materialize` package, so we add the directory
# to sys.path before importing.
sys.path.insert(0, str(Path(__file__).resolve().parent))
from groups import Group, Manifest, load_manifest  # noqa: E402

# mzbuild image names that we publish under our fingerprint. Each maps
# to the compose env-var placeholder; `.env` (export-env.py) supplies the
# concrete ref at compose-parse time.  Keep in sync with `export-env.py`.
#
# The per-group `antithesis-workload-<group>` names share a single
# placeholder (`${ANTITHESIS_WORKLOAD_IMAGE}`) — each group's .env supplies
# its own concrete value, so the compose YAML stays group-agnostic at
# this layer.  Group selection is encoded by which mzbuild name we
# rewrite the workload service to in `main()` before this map is
# consulted.
MATERIALIZE_IMAGES = {
    "materialized": "${MATERIALIZED_IMAGE}",
    "antithesis-workload-kafka": "${ANTITHESIS_WORKLOAD_IMAGE}",
    "antithesis-workload-pg-cdc": "${ANTITHESIS_WORKLOAD_IMAGE}",
    "antithesis-workload-mysql-cdc": "${ANTITHESIS_WORKLOAD_IMAGE}",
    "antithesis-workload-parallel-workload": "${ANTITHESIS_WORKLOAD_IMAGE}",
    "antithesis-workload-upsert-stress": "${ANTITHESIS_WORKLOAD_IMAGE}",
    "antithesis-workload-combined": "${ANTITHESIS_WORKLOAD_IMAGE}",
    # Standalone hammer image — only referenced by the `upsert-stress`
    # group's compose, but the placeholder is always emitted in `.env`
    # (an unused var costs nothing) so adding a hammer service to
    # another group later is a one-line change.
    "antithesis-upsert-hammer": "${ANTITHESIS_UPSERT_HAMMER_IMAGE}",
}

# Public-image fallbacks for mzbuild images whose Materialize-specific
# customizations subvert Antithesis (eatmydata, fsync no-ops, etc.).
# Antithesis can reach public registries — we just need to make sure the
# compose points at the upstream image, not our patched mzbuild build.
PUBLIC_FALLBACKS = {
    "postgres": "postgres:17.7",
    "minio": "minio/minio:latest",
}

# Header prepended to the generated YAML so check-copyright passes and
# readers know the file isn't hand-edited.
HEADER_TEMPLATE = """\
# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# GENERATED FILE — do not edit. Regenerate via:
#   bin/pyactivate test/antithesis/export-compose.py --group={group} > \\
#     test/antithesis/configs/{group}/docker-compose.yaml
# Source of truth: test/antithesis/mzcompose.py + test/antithesis/groups.yaml.
# Group: {group}

"""


def resolve_mzbuild(svc: dict[str, Any]) -> None:
    """Replace `mzbuild:` with a concrete or templated `image:` ref.

    For Materialize-built images we also set `pull_policy: never` so the
    `make up-local` flow doesn't attempt a registry probe at compose
    startup. The fingerprint tags only exist locally on the dev machine
    that ran `make build-local` — they're never pushed to GHCR by that
    flow, so the standard "check remote for newer digest" probe fails
    with `unauthorized` and aborts the bring-up. Third-party images
    (PUBLIC_FALLBACKS) genuinely come from upstream registries; for
    those we leave the default pull policy alone.

    The Antithesis platform itself uses a separate registry (Antithesis's
    GCP Artifact Registry) that it does have credentials for, so the
    pull_policy never field doesn't affect a real Antithesis run.
    """
    name = svc.pop("mzbuild")
    if name in MATERIALIZE_IMAGES:
        svc["image"] = MATERIALIZE_IMAGES[name]
        svc["pull_policy"] = "never"
    elif name in PUBLIC_FALLBACKS:
        svc["image"] = PUBLIC_FALLBACKS[name]
    else:
        raise ValueError(
            f"mzbuild image {name!r} has no Antithesis policy — add it to "
            f"MATERIALIZE_IMAGES (use a `.env` placeholder) or "
            f"PUBLIC_FALLBACKS (swap to a public image) in export-compose.py."
        )


def inline_postgres_setup(svc: dict[str, Any]) -> None:
    """Replace the bind-mounted setup SQL with an inline entrypoint write.

    Antithesis has no host filesystem, so we can't mount the SQL file.
    Read it from misc/postgres/setup_materialize.sql (one source of truth)
    and bake it into the service entrypoint.

    The inline-setup transform only fires when the service originally
    requested it (Postgres-ctor `setup_materialize=True`, which appears
    here as a bind-mounted setup_materialize.sql). Plain postgres-image
    services — e.g. a vanilla PG used as a CDC upstream — get the
    common env-fixup (drop LD_PRELOAD, add HOST_AUTH_METHOD=trust) and
    nothing else.
    """
    if not svc.get("image", "").startswith("postgres:"):
        return

    vols = svc.get("volumes", []) or []
    has_setup = any(isinstance(v, str) and "setup_materialize.sql" in v for v in vols)

    env = svc.setdefault("environment", [])
    # eatmydata isn't installed in the public postgres image.
    env[:] = [e for e in env if not e.startswith("LD_PRELOAD=")]
    # Trust auth — Antithesis-internal traffic only.
    env.append("POSTGRES_HOST_AUTH_METHOD=trust")

    if not has_setup:
        return

    # Drop the bind-mounted setup SQL; we'll inline it.
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


def strip_ports(svc: dict[str, Any]) -> None:
    """Drop the `ports:` block — Antithesis is container-to-container.

    The upstream Service classes (Materialized, Postgres, Minio, …) list
    `ports: [6875, 6876, …]` so that local mzcompose runs can reach
    services from the host.  Inside the Antithesis sandbox there is no
    host to publish to: containers reach each other by service name on
    the antithesis-net bridge, and podman tries to map each bare port
    to an ephemeral host port anyway.  When two services' random
    allocations collide on the same ephemeral, the second one to start
    fails with "bind: address already in use" — observed for
    materialized in the pg-cdc group, which has the most ports across
    services.  Stripping ports avoids the ephemeral roulette entirely.
    """
    svc.pop("ports", None)


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


# Single user-defined bridge network every service joins. Defining the
# network explicitly (rather than relying on docker-compose's auto-
# generated `default`) gives us deterministic container-DNS regardless
# of how the Antithesis platform's surrounding orchestration parses the
# compose file. Antithesis support flagged the auto-network as a likely
# cause of a kafka -> zookeeper UnknownHostException during setup; the
# fix is to make the network explicit.
#
# Must NOT set `internal: true` per Antithesis docker best practices —
# that would cut us off from the Antithesis-side network used for
# instrumentation. Plain bridge is the recommended shape.
ANTITHESIS_NETWORK = "antithesis-net"


# Extra docker-network aliases per service. The repository's
# `test/pg-cdc/*.td` files hard-code `@postgres` as the upstream hostname;
# the testdrive-runner drivers run those files unmodified by aliasing
# `postgres` to our `postgres-source` container at the network-DNS layer.
EXTRA_NETWORK_ALIASES: dict[str, list[str]] = {
    "postgres-source": ["postgres"],
}


def assign_network(name: str, svc: dict[str, Any]) -> None:
    """Place the service on the single named bridge network so docker-DNS
    is deterministic. Overwrites any pre-existing `networks` entry — some
    upstream Service classes set a vestigial `default: aliases: []` block
    that we don't want carried through.

    Services that need additional names on the same network (see
    `EXTRA_NETWORK_ALIASES`) use the long-form mapping syntax so we can
    declare `aliases:` for them. Plain services keep the short list form.
    """
    aliases = EXTRA_NETWORK_ALIASES.get(name)
    if aliases:
        svc["networks"] = {ANTITHESIS_NETWORK: {"aliases": aliases}}
    else:
        svc["networks"] = [ANTITHESIS_NETWORK]


def declare_top_level_network(compose: dict[str, Any]) -> None:
    """Declare the bridge network at the compose top level. Overwrites any
    pre-existing top-level `networks:` entry (mzcompose currently emits
    an empty dict).
    """
    compose["networks"] = {
        ANTITHESIS_NETWORK: {"driver": "bridge"},
    }


def set_explicit_names(name: str, svc: dict[str, Any]) -> None:
    """Set `container_name` and `hostname` to the service key.

    Per Antithesis docker best practices (https://antithesis.com/docs/
    best_practices/docker_best_practices/), every service should declare
    its container_name and hostname explicitly and use the same value
    for both. Triage reports attribute log lines and assertions by
    `hostname`; if it isn't set, Antithesis falls back to an inferred
    value (possibly the container id) that's harder to recognize.

    Set here at export time rather than per-service in mzcompose.py so
    that local mzcompose runs aren't constrained to one global
    container_name namespace.

    Asserts the service key is DNS-safe (no underscores, RFC-1123).
    Docker Compose itself rejects underscored service keys, so this is
    a sanity check, not a transform.
    """
    if "_" in name:
        raise ValueError(
            f"service {name!r}: underscores in hostnames break DNS resolution "
            f"under Antithesis (RFC-1123). Rename the service to use hyphens."
        )
    svc["container_name"] = name
    svc["hostname"] = name


def upgrade_started_to_healthy(compose: dict[str, Any]) -> None:
    """For every `depends_on` entry that uses `condition: service_started`
    against a dependency that declares a `healthcheck`, upgrade the
    condition to `service_healthy`.

    Under the Antithesis platform, `service_started` proved unreliable as
    a readiness gate during initial container startup: docker fires it as
    soon as the dependency's container *process* starts, before the
    dependency's DNS entry is reliably resolvable. The first run on the
    fault-isolated topology saw kafka hit `UnknownHostException: zookeeper`
    148+ times in a row before its retry loop landed on a successful
    lookup, with the same cascade downstream (schema-registry ↔ kafka).
    Gating on the healthcheck (which probes the actual listen port)
    eliminates that race.

    Dependencies without a healthcheck (e.g. clusterd, which has no
    readiness signal we currently expose) are left as `service_started`
    — there's nothing to wait on.
    """
    services = compose.get("services", {})
    has_healthcheck = {name for name, svc in services.items() if "healthcheck" in svc}
    for svc in services.values():
        deps = svc.get("depends_on")
        if not isinstance(deps, dict):
            continue
        for dep_name, dep_spec in deps.items():
            if (
                isinstance(dep_spec, dict)
                and dep_spec.get("condition") == "service_started"
                and dep_name in has_healthcheck
            ):
                dep_spec["condition"] = "service_healthy"


def prune_unreferenced_volumes(compose: dict[str, Any]) -> None:
    """Drop top-level `volumes:` entries no remaining service references.

    mzcompose's `Composition` declares the full `DEFAULT_MZ_VOLUMES` set
    at the top level regardless of which services we keep. After
    filtering to a group, many of those volumes have no consumer; leave
    them in and the YAML is cluttered with phantom volumes Docker
    silently ignores. Drop them.
    """
    top_level = compose.get("volumes") or {}
    referenced: set[str] = set()
    for svc in compose.get("services", {}).values():
        for entry in svc.get("volumes", []) or []:
            if not isinstance(entry, str) or entry.startswith("/"):
                continue
            name = entry.split(":", 1)[0]
            if name:
                referenced.add(name)
    compose["volumes"] = {k: v for k, v in top_level.items() if k in referenced}
    if not compose["volumes"]:
        compose.pop("volumes", None)


def register_referenced_named_volumes(compose: dict[str, Any]) -> None:
    """Declare any named volume referenced by a service that isn't already
    declared at the top level. Docker Compose rejects the file otherwise.

    mzcompose's `Composition` only auto-declares the fixed `DEFAULT_MZ_VOLUMES`
    set; per-service custom named volumes (e.g. `clusterd1_scratch`) reference
    names that have no top-level entry and fail `docker compose config`.
    """
    top_level: dict[str, Any] = compose.setdefault("volumes", {}) or {}
    compose["volumes"] = top_level

    for svc in compose.get("services", {}).values():
        for entry in svc.get("volumes", []) or []:
            if not isinstance(entry, str):
                continue
            # Bind mounts (`/host:/container`) start with `/`; named volumes
            # are bare identifiers. We only auto-declare the latter.
            if entry.startswith("/"):
                continue
            name = entry.split(":", 1)[0]
            if not name or name in top_level:
                continue
            top_level[name] = None


def filter_to_group(
    compose: dict[str, Any], manifest: Manifest, group: Group
) -> None:
    """Drop services not in `group`, then prune dangling depends_on refs.

    Mutates `compose` in place. Validates that every service the group
    asks for actually exists in mzcompose's output — a typo in
    groups.yaml fails loud here rather than producing a YAML missing a
    silently-dropped service.
    """
    kept = set(group.all_services(manifest.universal_services))
    available = set(compose["services"])

    missing = kept - available
    if missing:
        raise ValueError(
            f"group {group.name!r} references services not in mzcompose.py: "
            f"{sorted(missing)}. Either add them to mzcompose.SERVICES or "
            f"remove from groups.yaml."
        )

    compose["services"] = {
        name: svc for name, svc in compose["services"].items() if name in kept
    }

    # Prune depends_on entries that point at services we just dropped.
    # Compose treats an unresolved depends_on as a hard error at parse
    # time, so leaving them would break the per-group YAML.
    for svc in compose["services"].values():
        deps = svc.get("depends_on")
        if isinstance(deps, dict):
            for dep_name in list(deps):
                if dep_name not in kept:
                    deps.pop(dep_name)
            if not deps:
                svc.pop("depends_on", None)
        elif isinstance(deps, list):
            svc["depends_on"] = [d for d in deps if d in kept]
            if not svc["depends_on"]:
                svc.pop("depends_on", None)


def inject_workload_group_env(compose: dict[str, Any], group_name: str) -> None:
    """Append `ANTITHESIS_WORKLOAD_GROUP=<name>` to the workload service.

    The workload entrypoint reads this env at startup to decide which
    setup/driver/anytime scripts to surface to Test Composer and which
    cluster bootstrap branches to run.
    """
    workload = compose.get("services", {}).get("workload")
    if workload is None:
        raise ValueError(
            "compose has no `workload` service — cannot inject "
            "ANTITHESIS_WORKLOAD_GROUP. Did mzcompose.py drift?"
        )
    env = workload.setdefault("environment", [])
    env.append(f"ANTITHESIS_WORKLOAD_GROUP={group_name}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--group",
        required=True,
        help=(
            "Workload group to emit. Must be a key in test/antithesis/groups.yaml."
        ),
    )
    parser.add_argument(
        "--no-antithesis",
        action="store_true",
        help=(
            "Emit a compose YAML for local-dev (host arch) rather than the "
            "Antithesis x86_64 platform. Mirrors `export-env.py --no-antithesis` "
            "— together they let `make build-local` + `make up-local` run "
            "the stack natively on Apple Silicon (the Antithesis-flavored "
            "x86_64 testdrive binary segfaults inside Docker's rosetta/qemu "
            "emulation)."
        ),
    )
    args = parser.parse_args()

    manifest = load_manifest()
    group = manifest.group(args.group)

    arch = Arch.host() if args.no_antithesis else Arch.X86_64
    platform = "linux/amd64" if arch == Arch.X86_64 else "linux/arm64"

    # munge_services=False keeps ports bare (e.g., `6875` instead of
    # `127.0.0.1::6875`) — Antithesis is container-to-container, no host
    # binding. We do our own mzbuild→image substitution below and don't
    # need fingerprint resolution since Materialize-built images become
    # `${...}` placeholders.
    repo = Repository(Path("."), arch=arch, antithesis=not args.no_antithesis)
    c = Composition(repo, "antithesis", munge_services=False)

    # Filter first so per-service transforms (depends_on prune, mzbuild
    # resolution) only touch the services we're keeping.
    filter_to_group(c.compose, manifest, group)

    # Each group consumes its own per-group workload image
    # (antithesis-workload-<group>) rather than the shared
    # antithesis-workload.  The image's filename is decided at compose
    # generation time so the .env placeholder
    # `${ANTITHESIS_WORKLOAD_IMAGE}` resolves to the right per-group
    # ref at compose-parse time.  This must happen *before*
    # `resolve_mzbuild` swaps the mzbuild key for an image ref, since
    # `resolve_mzbuild` keys off the mzbuild name.
    workload_svc = c.compose["services"].get("workload")
    if workload_svc is not None:
        current = workload_svc.get("mzbuild", "")
        if current.startswith("antithesis-workload"):
            workload_svc["mzbuild"] = f"antithesis-workload-{group.name}"

    for name, svc in c.compose["services"].items():
        svc["platform"] = platform
        if "mzbuild" in svc:
            resolve_mzbuild(svc)
        inline_postgres_setup(svc)
        strip_host_bindmounts(svc)
        strip_ports(svc)
        strip_incompatible_env(svc)
        strip_mzcompose_keys(svc)
        set_explicit_names(name, svc)
        assign_network(name, svc)

    inject_workload_group_env(c.compose, group.name)
    declare_top_level_network(c.compose)
    upgrade_started_to_healthy(c.compose)
    register_referenced_named_volumes(c.compose)
    prune_unreferenced_volumes(c.compose)

    sys.stdout.write(HEADER_TEMPLATE.format(group=group.name))
    yaml.dump(c.compose, sys.stdout, default_flow_style=False, sort_keys=False)


if __name__ == "__main__":
    main()
