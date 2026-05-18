# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Single source of truth for the INC-936 upsert-stress topic + source
identifiers and partition count.

Split out from `helper_upsert_stress` so the standalone hammer image
(antithesis-upsert-hammer) can import these without dragging the
source-creation path's transitive `helper_pg` → `psycopg` chain into
its build. The hammer image is intentionally minimal — no Materialize
client surface, no Test Composer scripts, no `/opt/antithesis/`
directory at all — and pulling psycopg in just to read three module-
level constants would defeat that.

`helper_upsert_stress` re-exports the same names so the workload-side
setup driver can keep using its existing import.
"""

from __future__ import annotations

# Multi-partition topic. The bug doc calls out GM's many-partition
# topic as a likely amplifier (each partition has its own capability
# and the operator consolidates across them). 8 is enough to exercise
# multi-partition routing across CLUSTERD_WORKERS=4 timely workers
# without saturating the single-broker harness.
NUM_PARTITIONS = 8

TOPIC_UPSERT_STRESS = "antithesis-upsert-stress"
SOURCE_UPSERT_STRESS = "upsert_stress_src"
