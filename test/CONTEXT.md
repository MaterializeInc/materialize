# test

**Purpose:** System-level test corpus for Materialize. Contains ~100 named suites,
each a self-contained directory. Total ~53K LOC; only `test/cluster` exceeds 5K LOC.

**Framework taxonomy:**

| Framework | Count | Location | Scope |
|---|---|---|---|
| testdrive (`.td`) | ~1,185 files | `test/testdrive/`, embedded in mzcompose | End-to-end SQL + Kafka/sources, direct wire-protocol assertions |
| sqllogictest (`.slt`) | ~481 files | `test/sqllogictest/` | SQL correctness, planner regression |
| mzcompose (Python) | ~98 `mzcompose.py` | One per named suite | Multi-service orchestration; wraps the above |
| pytest | `conftest.py` + `test_*.py` | `test/cloudtest/`, `test/playwright/` | Kubernetes-level and browser E2E |

**Depth:** All suites are depth-1 leaves. `test/cluster` (depth-2) is the only
sub-suite with its own subdirectory structure.

**Key subdirs:**
- `cluster/` — external-clusterd topology tests; see cluster/CONTEXT.md.
- `testdrive/` — largest single-framework corpus; ~211 files, direct testdrive runner.
- `sqllogictest/` — SQL planner/executor correctness.
- `platform-checks/` — mzcompose suite validating upgrade/downgrade compatibility.
- `cloudtest/` — pytest/Kubernetes suite for cloud-env scenarios.
- `feature-benchmark/`, `parallel-workload/`, `scalability/` — performance/stress.

**Seam:** `materialize/mzcompose/composition.py` is the orchestration backbone;
every `mzcompose.py` imports `Composition` from there.

**Bubbles up to root:** test/ is a pure test corpus; no production code. Root
CONTEXT.md should note the four-framework split (testdrive / sqllogictest /
mzcompose / pytest) and that `test/cluster` is the external-clusterd topology fixture.
