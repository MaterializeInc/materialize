#!/bin/sh

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# bake-test-templates.sh — image-build-time test-template populator.
#
# Called once from the workload Dockerfile after the source tree has
# been staged at /opt/antithesis-stage/ (the contents of
# `test/antithesis/workload/test/`, with per-template subdirs
# `kafka/`, `pg-cdc/`, `mysql-cdc/`, `parallel-workload/`, plus
# `defaults/` for scripts that should land in every template).  This
# script reads the manifest via `scripts-for-group` and copies files
# from the staging tree into the runtime template directories at
# /opt/antithesis/test/v1/<template>/.
#
# Routing:
#   - Manifest entries are paths of the form `<template>/<file>`.
#   - `<focused-template>/<file>`  → /opt/antithesis/test/v1/<focused-template>/<file>
#   - `defaults/<file>`            → /opt/antithesis/test/v1/<template>/<file>  for every
#                                    real template the active group uses, so the catalog
#                                    /persist drivers in `default_drivers:` get exercised
#                                    regardless of which template Antithesis picks per
#                                    execution history.
#
# Antithesis selects exactly one template per execution history, so
# the focused groups (kafka, pg-cdc, mysql-cdc, parallel-workload)
# end up with one template; the kitchen-sink `combined` group ends up
# with four — one per focused group's drivers.

set -eux

GROUP=${1:?usage: bake-test-templates.sh <group>}
STAGE=/opt/antithesis-stage
ROOT=/opt/antithesis/test/v1

# Enumerate the runtime templates the active group will land scripts in.
# scripts-for-group emits paths like `kafka/parallel_driver_kafka_*.py`
# and `defaults/anytime_health_check.sh`; the prefix before the first
# `/` identifies the source template.  For routing we treat every
# non-defaults prefix as a real runtime template.
templates=$(
    ANTITHESIS_WORKLOAD_GROUP="$GROUP" scripts-for-group \
        | awk -F/ '$1 != "defaults" {print $1}' \
        | sort -u
)
if [ -z "$templates" ]; then
    echo "bake-test-templates.sh: group '$GROUP' has no runtime templates" >&2
    exit 1
fi

# Pre-create the runtime template directories.
for t in $templates; do
    mkdir -p "$ROOT/$t"
done

# Per-template local helpers (`helper_*` files in the source
# `<template>/` subdir).  Antithesis's command discovery skips files
# with a `helper_` prefix, but they can be `import`-ed by test
# commands in the same template — e.g. parallel-workload's
# `first_setup.py` chains three `helper_*_setup.py` modules.  The
# manifest doesn't list helpers, so copy them based on filename.
for t in $templates; do
    for f in "$STAGE/$t"/helper_*.py "$STAGE/$t"/helper_*.sh; do
        [ -f "$f" ] || continue
        cp "$f" "$ROOT/$t/"
    done
done

# Per-template data subdirectories.  Templates can ship arbitrary
# bundled data alongside their scripts (e.g. workload-replay's
# `captured/` subdir holds a multi-megabyte captured-workload YAML
# the driver replays).  Antithesis's command discovery ignores
# directories, so any subdir is safe to copy verbatim — only files
# at the top level can become test commands.  Manifests don't list
# data dirs, so copy them based on `is a directory`.
#
# Skip `__pycache__`: it's noise if anyone py_compiled in the source
# tree before the build context was staged, never anything we'd want
# in the runtime image.
for t in $templates; do
    for d in "$STAGE/$t"/*/; do
        [ -d "$d" ] || continue
        case "$d" in
            */__pycache__/) continue ;;
        esac
        cp -R "$d" "$ROOT/$t/"
    done
done

# Copy each manifest entry to its destination(s).
ANTITHESIS_WORKLOAD_GROUP="$GROUP" scripts-for-group | while read -r path; do
    template=${path%%/*}
    file=${path#*/}
    src="$STAGE/$path"
    if [ ! -f "$src" ]; then
        echo "bake-test-templates.sh: manifest references '$path' but " \
             "'$src' doesn't exist; check test/antithesis/groups.yaml " \
             "vs. test/antithesis/workload/test/ layout" >&2
        exit 1
    fi
    if [ "$template" = "defaults" ]; then
        # Defaults are merged into every real runtime template so the
        # catalog/persist drivers in `default_drivers:` fire regardless
        # of which template Antithesis picks per execution history.
        for t in $templates; do
            cp "$src" "$ROOT/$t/$file"
        done
    else
        cp "$src" "$ROOT/$template/$file"
    fi
done

# Mark every script executable.  Antithesis's test-command discovery
# requires the executable bit to be set on the container's default user.
for t in $templates; do
    chmod +x "$ROOT/$t"/*
done
