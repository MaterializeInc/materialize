---
name: limits-test
description: >
  This skill should be used when the user wants to add, modify, or debug a limits
  test. Trigger when the user mentions "limits test", "Generator subclass",
  "many objects", "scaling test", or wants to stress-test Materialize with large
  numbers of objects (tables, views, sources, indexes, etc.). Also trigger when
  the user edits test/limits/mzcompose.py.
---

# Limits Test Framework

The limits test stresses Materialize with large numbers of objects, complex queries, and large ingestions. It catches regressions via fixpoint panics, stack overflows, CI timeouts (O(N^M) with N~1000), and OOM panics (2GB memory limit).

## Where It Lives

- **Main file**: `test/limits/mzcompose.py`
- All Generator subclasses live in that single file
- **Auto-discovery**: Subclasses of `Generator` are found automatically via `all_subclasses(Generator)` from `materialize.util` - no registration needed

## Generator Base Class

```python
class Generator:
    COUNT: int = 1000      # Number of objects to create (override per test)
    VERSION: str = "1.0.0" # Bump when test logic changes
    MAX_COUNT: int | None = None  # Upper limit for --find-limit mode

    @classmethod
    def body(cls) -> None:
        """Override this. Print testdrive commands to stdout."""
        raise NotImplementedError

    # Convenience iterators
    @classmethod
    def all(cls) -> range:       # range(1, COUNT+1)
    @classmethod
    def no_first(cls) -> range:  # range(2, COUNT+1) - skip first
    @classmethod
    def no_last(cls) -> range:   # range(1, COUNT) - skip last

    @classmethod
    def store_explain_and_run(cls, query: str) -> str | None:
        """Records EXPLAIN query for timing and prints `> {query}`."""
```

`header()` is inherited - it resets the `public` schema and grants privileges. `footer()` prints a blank line. `generate()` calls `header()` → `body()` → `footer()`.

## How to Add a New Limits Test

1. Add a new class in `test/limits/mzcompose.py` that extends `Generator`.
2. Implement the `body()` classmethod. It must `print()` testdrive commands.
3. Optionally override `COUNT` and `MAX_COUNT`, only if it's causing issues in CI.

### Minimal Template

```python
class MyNewFeature(Generator):
    COUNT = min(Generator.COUNT, 100)  # Lower if operation is slow
    MAX_COUNT = 800  # Optional: cap for --find-limit mode

    @classmethod
    def body(cls) -> None:
        # Bump system limits if needed
        print("$ postgres-execute connection=mz_system")
        print(f"ALTER SYSTEM SET max_tables = {cls.COUNT * 10};")
        print("$ postgres-execute connection=mz_system")
        print(f"ALTER SYSTEM SET max_objects_per_schema = {cls.COUNT * 10};")

        # Create objects
        for i in cls.all():
            print(f"> CREATE TABLE t{i} (f1 INTEGER);")
            print(f"> INSERT INTO t{i} VALUES ({i});")

        # Query and verify
        for i in cls.all():
            cls.store_explain_and_run(f"SELECT * FROM t{i}")
            print(f"{i}")
```

You can run it locally with `bin/mzcompose --find limits down && bin/mzcompose --find limits run main --scenario=MyNewFeature`, add `--find-limit` for the Release Qualification mode.
