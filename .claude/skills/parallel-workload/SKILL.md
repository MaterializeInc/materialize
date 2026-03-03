---
name: parallel-workload
description: >
  This skill should be used when the user wants to extend the parallel-workload
  test framework, add a new action, modify existing actions, or add SQL coverage
  to parallel workload. Trigger when the user mentions "parallel workload",
  "parallel-workload", "action.py" in the context of parallel workload,
  or wants to test for panics or unexpected query errors under concurrency.
  Use this skill even if the user just says "add this to parallel workload" or
  references a bug that panics under concurrent DDL/DML.
---

# Extending Parallel Workload

The parallel-workload framework stress-tests Materialize by running random SQL actions concurrently across multiple threads. It catches **panics and unexpected query errors** - it does not verify result correctness.

## When to Use This Framework

Add coverage here when a bug manifests as:
- A panic under concurrent operations
- An unexpected error from a query that should succeed (or fail gracefully)
- A crash or connection loss triggered by specific DDL/DML combinations

## How It Works

1. The orchestrator spawns N worker threads, each assigned an **action list** (read, fetch, write, dml_nontrans, or ddl) based on the configured `Complexity`.
2. Each worker randomly selects actions from its list using **weighted random choice** and executes them in a loop until the runtime expires.
3. If an action raises a `QueryError`, the worker checks `action.errors_to_ignore(exe)`. If the error matches, it's logged and ignored. If not, the **test fails**.
4. All threads share a `Database` object that tracks created objects (tables, views, clusters, etc.) with thread-safe locking.

## Adding a New Action

### Step 1: Define the Action Class

Add your class in `action.py`. Subclass `Action` and implement `run()`:

```python
class MyNewAction(Action):
    def run(self, exe: Executor) -> bool:
        # Return False to skip (e.g., precondition not met)
        # Return True after successful execution
        exe.execute("SELECT my_new_feature()", http=Http.RANDOM)
        return True
```

### Step 2: Override `errors_to_ignore()` If Needed

If your action can produce expected errors (e.g., object was concurrently dropped), extend the base list:

```python
    def errors_to_ignore(self, exe: Executor) -> list[str]:
        result = super().errors_to_ignore(exe)
        result.extend([
            "my expected error message",
        ])
        # Conditionally ignore errors based on complexity or scenario
        if exe.db.complexity == Complexity.DDL:
            result.extend(["does not exist"])
        return result
```

### Step 3: Register in an Action List

Add your action class and weight to the appropriate list at the bottom of `action.py`:

```python
# In ddl_action_list, read_action_list, write_action_list, etc.
ddl_action_list = ActionList(
    [
        ...
        (MyNewAction, 5),  # weight controls selection frequency
        ...
    ],
    autocommit=True,  # True for DDL, False for transactional actions
)
```

**Which list to use:**
- `read_action_list` - SELECT, SUBSCRIBE, COPY TO (autocommit=False)
- `fetch_action_list` - DECLARE CURSOR / FETCH (autocommit=False)
- `write_action_list` - INSERT, COPY FROM (autocommit=False)
- `dml_nontrans_action_list` - DELETE, UPDATE, INSERT RETURNING (autocommit=True)
- `ddl_action_list` - CREATE/DROP/ALTER/RENAME (autocommit=True)

## Common Patterns

### Picking a Random Database Object

```python
def run(self, exe: Executor) -> bool:
    with exe.db.lock:
        if not exe.db.tables:
            return False
        table = self.rng.choice(exe.db.tables)
    # Use table outside db.lock but inside table.lock for mutations
    with table.lock:
        if table not in exe.db.tables:
            return False  # Was dropped while we waited
        exe.execute(f"ALTER TABLE {table} ...", http=Http.RANDOM)
    return True
```

### Respecting Object Limits

```python
def run(self, exe: Executor) -> bool:
    if len(exe.db.tables) >= MAX_TABLES:
        return False  # Skip if at capacity
    ...
```

Limits are defined in `database.py`: `MAX_TABLES=5`, `MAX_VIEWS=15`, `MAX_CLUSTERS`, `MAX_SCHEMAS`, etc.

### Creating a New Database Object

```python
def run(self, exe: Executor) -> bool:
    if len(exe.db.views) >= MAX_VIEWS:
        return False
    view_id = exe.db.view_id
    exe.db.view_id += 1
    try:
        schema = self.rng.choice(exe.db.schemas)
    except IndexError:
        return False
    with schema.lock:
        if schema not in exe.db.schemas:
            return False
        view = View(self.rng, view_id, base_object, schema, ...)
        view.create(exe)
    exe.db.views.append(view)
    return True
```

### Dropping a Database Object

```python
def run(self, exe: Executor) -> bool:
    with exe.db.lock:
        if len(exe.db.views) <= 2:
            return False  # Keep a minimum
        view = self.rng.choice(exe.db.views)
    with view.lock:
        if view not in exe.db.views:
            return False
        exe.execute(f"DROP VIEW {view}", http=Http.RANDOM)
        exe.db.views.remove(view)
    return True
```

### Scenario-Specific Actions

Some actions only run in specific scenarios:

```python
def run(self, exe: Executor) -> bool:
    if exe.db.scenario != Scenario.Rename:
        return False
    ...
```

### Using Prepared Statements

```python
def run(self, exe: Executor) -> bool:
    query = "SELECT ..."
    if self.rng.choice([True, False]):
        self.stmt_id += 1
        self.exe_prepared(query, f"mystmt{self.stmt_id}", exe)
    else:
        exe.execute(query, http=Http.RANDOM)
    return True
```

## Extending an Existing Action

Often the simplest approach is to add new SQL variants to an existing action's `run()` method. For example, to test a new expression type, add it to `expression.py`. To test a new system flag, add it to `FlipFlagsAction.flags`.

### Adding a System Flag to FlipFlagsAction

In the `FlipFlagsAction` class, add an entry to the `flags` dictionary:

```python
flags: dict[str, list[str]] = {
    ...
    "my_new_flag": ["true", "false"],
    ...
}
```

## Running

```bash
# Default: DDL complexity, regression scenario, 600s runtime
bin/mzcompose --find parallel-workload run default

# Custom options
bin/mzcompose --find parallel-workload run default \
    --complexity=ddl \
    --scenario=regression \
    --runtime=300 \
    --seed=42

# Available complexities: read, dml, ddl, ddl-only, random
# Available scenarios: regression, cancel, kill, rename, backup-restore, 0dt-deploy, random
```

## Important Design Rules

1. **Minimize locking** - Use `exe.db.lock` briefly to pick objects, then use per-object locks. Excessive locking reduces the chance of finding real race conditions. You can instead add an expected error in `errors_to_ignore`
2. **Return False, don't raise** - If a precondition isn't met (empty list, at capacity), return `False` to skip the action.
3. **Handle IndexError from `rng.choice`** - Objects can be concurrently removed. Either check length first under lock, or catch `IndexError` and return `False`.
4. **Track state changes** - If you create an object, append it to the appropriate `exe.db` list. If you drop one, remove it.
5. **Use `self.rng`** - Always use the action's seeded `random.Random` instance, never `random.choice()` directly. This makes failures reproducible via `--seed`.
