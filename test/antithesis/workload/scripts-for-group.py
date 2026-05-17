#!/usr/bin/env python3
# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Print one `<template>/<file>` path per line for the active group.

Standalone helper used at workload-image *build* time by
`bake-test-templates.sh`.  The image's pre-image:copy step lands the
manifest at `/opt/antithesis-config/groups.yaml`, and the bake script
shells out to this helper to know which source files to copy into
which `/opt/antithesis/test/v1/<template>/` directories.

Output format: one `<template>/<file>.{py,sh}` per line.  Source files
live at `/opt/antithesis-stage/<template>/<file>.{py,sh}` (the test/
subdirectory staged into the build context).  The `defaults/` pseudo-
template's contents are physically duplicated by the bake script into
every real template directory; everything else copies 1:1.

Intentionally has no dependency on the materialize Python tree — the
workload image is a stripped-down runtime, not a full repo, so this
helper only needs `yaml` (already pinned in the image's pip install).
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import yaml

DEFAULT_MANIFEST = "/opt/antithesis-config/groups.yaml"
DEFAULT_GROUP = "combined"


def script_path(entry: str) -> str:
    # Manifest entries are `<template>/<basename>`; bash scripts
    # (anytime_health_check) are .sh, everything else .py.  Keep the
    # extension special-case here so the manifest stays brief.
    template, basename = entry.split("/", 1)
    ext = ".sh" if basename == "anytime_health_check" else ".py"
    return f"{template}/{basename}{ext}"


def main() -> int:
    manifest_path = Path(os.environ.get("ANTITHESIS_GROUPS_MANIFEST", DEFAULT_MANIFEST))
    group_name = os.environ.get("ANTITHESIS_WORKLOAD_GROUP", DEFAULT_GROUP)

    raw = yaml.safe_load(manifest_path.read_text())
    groups = raw.get("groups", {})
    if group_name not in groups:
        valid = ", ".join(sorted(groups))
        print(
            f"scripts-for-group: unknown group {group_name!r}; "
            f"valid groups: {valid}",
            file=sys.stderr,
        )
        return 2

    g = groups[group_name]
    default_anytime = raw.get("default_anytime", []) or []
    default_drivers = raw.get("default_drivers", []) or []
    ordered = (
        list(g.get("setup") or [])
        + list(g.get("drivers") or [])
        + list(default_drivers)
        + list(g.get("anytime") or [])
        + list(default_anytime)
    )

    seen: set[str] = set()
    for entry in ordered:
        if entry in seen:
            continue
        seen.add(entry)
        print(script_path(entry))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
