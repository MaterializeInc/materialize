# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Standalone continuous-load image for the INC-936 upsert-stress group.
#
# Built deliberately small and separate from `antithesis-workload-*`:
# the workload images bake Test Composer command directories at
# `/opt/antithesis/test/v1/<template>/` at build time, and Antithesis
# scans every container in the topology for that path at run start.
# If the hammer reused a workload image, Antithesis would also run
# Test Composer drivers (catalog/persist invariants, etc.) inside the
# hammer container — duplicating assertion traffic and executing
# Materialize-pgwire-side logic from a container that has none of the
# env wiring for it.  Stripping the directory at the container
# entrypoint isn't enough: discovery happens off the image layout.
#
# This image therefore carries:
#   - python3.12 + the two pip deps the hammer actually needs
#   - the hammer script + the handful of helper modules it imports
#   - nothing under `/opt/antithesis/` at all
#
# Matches `python:3.12-slim-trixie` in the workload image so the
# Antithesis platform (Debian 13 baseline) sees a consistent libc; for
# a pure-Python image this is taste rather than requirement.

FROM python:3.12-slim-trixie

# `confluent-kafka` (no `[avro]` — the hammer produces TEXT key+value)
# is the librdkafka binding. `antithesis` is the SDK; helper_random
# routes per-call entropy through it under Antithesis, falling back to
# the OS RNG outside.
RUN pip install --no-cache-dir \
    "confluent-kafka==2.8.0" \
    antithesis==0.2.0

# Helpers and constants land alongside the hammer script under
# `/opt/upsert-hammer/`. PYTHONPATH-pointing at `helpers/` keeps
# `import helper_kafka` working without exposing the helpers as
# importable modules to anything else (no module-resolution surprises
# if this image were ever extended).
COPY helpers/ /opt/upsert-hammer/helpers/
COPY upsert_hammer_loop.py /opt/upsert-hammer/upsert_hammer_loop.py

ENV PYTHONPATH=/opt/upsert-hammer/helpers

ENTRYPOINT ["python3", "/opt/upsert-hammer/upsert_hammer_loop.py"]
