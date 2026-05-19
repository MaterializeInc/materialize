---
commit: e7ac38b338
updated: 2026-05-06
scope: antithesis-sdk-search
---

# Existing Antithesis Assertions

Before the upsert-source prototype, no existing Antithesis SDK assertions or
lifecycle calls were found in this worktree.

Search performed:

```text
rg -n "antithesis_sdk|antithesis\.assertions|antithesis\.lifecycle|assert_always!|assert_sometimes!|assert_reachable!|assert_unreachable!|ANTITHESIS_STOP_FAULTS|ANTITHESIS_SDK" --glob '!target/**' --glob '!Cargo.lock' .
```

The upsert-source prototype now adds workload-side SDK calls through
`misc/python/materialize/antithesis/upsert_sources.py`:

| Location | Assertion type | Message |
| --- | --- | --- |
| `upsert_sources.py` | `reachable` | `upsert source command saw tolerated testdrive failure` |
| `upsert_sources.py` | `reachable` | `upsert source setup completed` |
| `upsert_sources.py` | `sometimes` | `upsert source write driver completed at least once` |
| `upsert_sources.py` | `sometimes` | `upsert source stale-safe read completed at least once` |
| `upsert_sources.py` | `sometimes` | `upsert source health check completed at least once` |
| `upsert_sources.py` | `reachable` | `upsert source expected rows verified` |
| `upsert_sources.py` | `always` | `upsert source catches up after quiet period` |

There are still no SUT-side Rust assertions. Add those only where a property
needs internal visibility that the workload cannot observe.

The shared runner module `materialize.antithesis.td_runner` adds two more
workload-side reachable buckets used by all Tier 1 .td wrappers:

| Location | Assertion type | Message |
| --- | --- | --- |
| `td_runner.py` | `reachable` | `td_runner completed .td file` |
| `td_runner.py` | `reachable` | `td_runner saw tolerated failure` |

The four `testdrive_*` area templates (sql / kafka / load_generator /
recovery) each have a single `singleton_driver_random_*` command and
intentionally **no** `eventually_*` recovery check. We tried per-area
`eventually_*` checks first (initially trivial `count(*) >= 0` queries,
then a real CREATE/INSERT/SELECT/DROP round-trip), but the round-trip
ended up being a generic "MZ still works" probe whose pass/fail signal
was only weakly correlated with the chosen .td's actual semantics — when
the singleton picks a random file, you can't write a useful recovery
property without knowing what state was created. That correlation
belongs in SUT-side Rust assertions or in scenario-specific
`eventually_*` commands (like `upsert_sources/eventually_*` which has
its own expected-state model). Add those when there's something
concrete to verify.

The Antithesis Python SDK is now available locally too: `antithesis==0.2.0`
is pinned in `ci/builder/requirements.txt` (the real source of
`misc/python/requirements.txt`, which is a symlink). Both the test-driver
Docker image and the local uv venv resolve `from antithesis.assertions
import reachable` to the same real SDK. Our scaffolding directory
`antithesis/` at the repo root coexists peacefully with the installed
package because antithesis 0.2.0 is itself a namespace package — Python's
namespace-package merge picks up submodules from site-packages while our
directory contributes nothing conflicting.
