---
name: mz-platform-checks
description: >
  This skill should be used when the user wants to create, modify, or debug a
  platform check. Trigger when the user mentions "platform check", "platform-checks",
  "upgrade check", "restart check", or wants to write a Check class that tests
  feature survival across restarts/upgrades. Also trigger when the user edits files
  in misc/python/materialize/checks/all_checks/.
---

# Platform Checks

The Platform Checks framework is "write once, run everywhere" - a single test validates a feature across multiple scenarios (restarts, upgrades, etc.).

## Key Concepts

- **Check**: A Python class that tests one feature via testdrive fragments.
- **Scenario**: The context (e.g., restart, upgrade) in which checks run.
- **Action**: An individual step within a scenario (e.g., stop a container).

## Where Checks Live

All checks are in `misc/python/materialize/checks/all_checks/`. The `__init__.py` auto-discovers every `.py` file in that directory, so adding a new file is all that's needed for registration.

## Anatomy of a Check

A check is a class that extends `Check` and implements three methods. Each returns a `Testdrive` object wrapping a testdrive fragment string.

```python
from textwrap import dedent
from materialize.checks.actions import Testdrive
from materialize.checks.checks import Check

class MyFeature(Check):
    def initialize(self) -> Testdrive:
        """Run once at the start. Create tables, insert seed data, set up objects."""
        return Testdrive(dedent("""
            > CREATE TABLE my_feature_table (f1 INTEGER);
            > INSERT INTO my_feature_table VALUES (1), (2), (3);
        """))

    def manipulate(self) -> list[Testdrive]:
        """MUST return a list of exactly 2 Testdrive fragments.
        Each runs after a restart/upgrade phase. Insert more data, ALTER objects,
        or create derived objects (e.g., materialized views)."""
        return [
            Testdrive(dedent("""
                > INSERT INTO my_feature_table VALUES (4), (5);
                > CREATE MATERIALIZED VIEW my_feature_view1 AS
                  SELECT COUNT(*) AS c FROM my_feature_table;
            """)),
            Testdrive(dedent("""
                > INSERT INTO my_feature_table VALUES (6), (7);
                > CREATE MATERIALIZED VIEW my_feature_view2 AS
                  SELECT SUM(f1) AS s FROM my_feature_table;
            """)),
        ]

    def validate(self) -> Testdrive:
        """Run after all initialize + manipulate steps. Verify everything survived.
        MAY BE CALLED MORE THAN ONCE - must be idempotent. Any objects created
        here must be TEMPORARY or explicitly dropped."""
        return Testdrive(dedent("""
            > SELECT * FROM my_feature_view1;
            7
            > SELECT * FROM my_feature_view2;
            28
        """))
```

## Execution Order in a Scenario

```
StartMz → initialize() → [restart/upgrade] → manipulate(phase=0)
  → [restart/upgrade] → manipulate(phase=1) → [restart/upgrade] → validate()
```

Both manipulate phases always run. validate() may run multiple times.

## Decorators and Version Gating

### Disable a check
```python
from materialize.checks.checks import disabled

@disabled(ignore_reason="due to database-issues#12345")
class MyBrokenCheck(Check):
    ...
```

### Non-idempotent external interactions
If a check ingests non-UPSERT data into Kafka/Postgres (i.e., replaying manipulate would duplicate data), annotate it:
```python
from materialize.checks.checks import externally_idempotent

@externally_idempotent(False)
class MyKafkaCheck(Check):
    ...
```

### Version-gated checks
For features that only exist from a certain version, override `_can_run`:
```python
from materialize.checks.executors import Executor
from materialize.mz_version import MzVersion

class MyNewFeature(Check):
    def _can_run(self, e: Executor) -> bool:
        return self.base_version >= MzVersion.parse_mz("v0.80.0-dev")
```

### Version-conditional testdrive lines
Use `[version>=VVMMPP]` or `[version<VVMMPP]` prefixes for syntax that changed between versions (e.g., v0.92.0 → `9200`):
```
>[version<9200] CREATE SOURCE s FROM LOAD GENERATOR COUNTER (SCALE FACTOR 0.01)
>[version>=9200] CREATE SOURCE s FROM LOAD GENERATOR COUNTER
```

## Tips for Good Checks

1. **Naming**: Prefix ALL object names with a unique string (e.g., `my_feature_`) to avoid collisions with other checks running in the same Mz instance.
2. **manipulate() returns exactly 2 elements**: The framework asserts this.
3. **validate() is idempotent**: It may run multiple times. Use TEMPORARY objects or DROP at end.
4. **Create objects in manipulate, not just initialize**: This tests that DDL works after restarts/upgrades.
5. **Create derived objects on both old and new base objects**: e.g., a materialized view on a table from initialize AND on a table from manipulate.
6. **Exercise full SQL syntax breadth**: Test every WITH option, every variant of the feature.
7. **Ingest data in every phase**: Ensure data flows during restarts/upgrades.
8. **Use `dedent()` on all testdrive strings**: Required for proper testdrive parsing.

## Helper Methods on Check

- `self.base_version`: The MzVersion the test started from (useful for upgrade scenarios).
- `self.current_version`: The MzVersion currently running.
- `self._kafka_broker()`: Returns the broker connection string for testdrive.
- `self._default_cluster()`: Returns `"quickstart"`.
- `self.rng`: A seeded `Random` instance for deterministic randomness.

## Running

```bash
# Run a specific check against a specific scenario
bin/mzcompose --find platform-checks run default --scenario=RestartEntireMz --check=MyFeature

# Run with no restarts (useful for debugging the check itself)
bin/mzcompose --find platform-checks run default --scenario=NoRestartNoUpgrade --check=MyFeature

# Run all checks (default)
bin/mzcompose --find platform-checks run default --scenario=RestartEntireMz
```

## Debugging CI Failures

1. Find the scenario name near the top of the log: `Running ... --scenario=RestartEnvironmentdClusterdStorage`
2. Find the failing check just before the error: `Running validate() from <materialize.checks.threshold.Threshold ...>`
3. Reproduce locally: `bin/mzcompose --find platform-checks run default --scenario=RestartEnvironmentdClusterdStorage --check=Threshold`
4. Try `--scenario=NoRestartNoUpgrade` to isolate whether the failure is restart/upgrade-related.
