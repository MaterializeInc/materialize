#!/usr/bin/env bash

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Runs probe.py on the host scratch volume, host /tmp, and inside a container
# on its overlay root and on a bind mount of the scratch volume (the emulator's
# two possible scratch-directory placements).
set -uo pipefail
here=$(cd "$(dirname "$0")" && pwd)
python3 "$here/probe.py" /scratch /tmp
docker run --rm -v "$here":/p:ro -v /scratch:/bind python:3-slim \
    sh -c 'mkdir -p /ovl && python3 /p/probe.py /ovl /bind' 2>/dev/null
