---
name: mz-benchmark
description: >
  Add, modify, or debug benchmark scenarios for measuring Materialize
  performance. Covers three frameworks: Feature Benchmark (single-operation
  micro-benchmarks), Scalability Test (SQL throughput under concurrency), and
  Parallel Benchmark (sustained latency over time via scenarios.py). Trigger on
  "benchmark", "feature benchmark", "scalability test", "parallel benchmark",
  "performance regression", "micro-benchmark", "TPS", "latency test", or when
  editing files in feature_benchmark/scenarios/, scalability/workload/workloads/,
  or parallel_benchmark/scenarios.py. Note: this is about benchmark measurement
  frameworks, not the parallel-workload stress-testing framework (which tests for
  panics under concurrency, not performance).
---

# Benchmark Frameworks

Materialize has some benchmark frameworks targetting local Docker, each suited to different performance concerns. Choose the right one based on what you're measuring.

## Decision Guide

| Concern | Framework | Best For |
|---------|-----------|----------|
| Single-operation latency | **Feature Benchmark** | "How fast is this SELECT / INSERT / CREATE INDEX?" |
| SQL throughput under concurrency | **Scalability Test** | "How many QPS can we sustain at 1/4/16/64/256 concurrent clients?" |
| Sustained performance over time | **Parallel Benchmark** | "Does latency degrade over a 2-minute mixed workload?" |

---

## 1. Feature Benchmark (Micro-benchmarks)

Measures wall-clock time of individual SQL operations without concurrency. Uses testdrive fragments. Great for regression-testing specific code paths.

### How to Add a Scenario

1. Add a class in one of the `scenarios/*.py` files. Extend `Scenario` (default, runs in CI), `ScenarioBig` (longer, opt-in), or `ScenarioDisabled`.
2. Implement `benchmark()` returning a `Td(...)` with timing markers (`/* A */` and `/* B */`).
3. Optionally implement `shared()`, `init()`, `before()` if setup is required.

### Scenario Template

```python
from materialize.feature_benchmark.action import TdAction
from materialize.feature_benchmark.measurement_source import MeasurementSource, Td
from materialize.feature_benchmark.scenario import Scenario

class MyCategory(Scenario):
    """Group parent - won't run (not a leaf)."""
    pass

class MySpecificBenchmark(MyCategory):
    """Leaf class - this runs."""

    def init(self) -> list[TdAction]:
        return [
            self.table_ten(),  # Creates table 'ten' with values 0-9
            TdAction("""
                > CREATE TABLE t1 (f1 INTEGER);
                > INSERT INTO t1 SELECT {unique_values} FROM {join};
                > SELECT COUNT(*) FROM t1;
                {n}
            """.format(
                unique_values=self.unique_values(),
                join=self.join(),
                n=self.n(),
            )),
        ]

    def benchmark(self) -> MeasurementSource:
        return Td("""
> SELECT 1;
  /* A */
1

> SELECT COUNT(*) FROM t1 WHERE f1 % 2 = 0;
  /* B */
{half_n}
""".format(half_n=self.n() // 2))
```

### Timing Markers

- `/* A */` - Start marker. Measurement begins AFTER this completes.
- `/* B */` - End marker. Measurement ends when this completes.
- Measures: time from end of A to end of B.
- For a single query, use `SELECT 1; /* A */` as a dummy start to not measure the overhead of Testdrive starting up.

### Scenario Phases (execution order)

1. **`shared()`** - Runs once for BOTH instances (e.g., create Kafka topics)
2. **`init()`** - Runs once per instance (create tables, populate data)
3. **`before()`** - Runs before EACH iteration (restart, cleanup)
4. **`benchmark()`** - Runs repeatedly (10-50 times), measured

### Helpers

- `self.n()` - Returns 10^SCALE (default SCALE=6 → 1,000,000)
- `self.table_ten()` / `self.view_ten()` - Creates table/view with values 0-9
- `self.unique_values()` - SQL expression generating N unique values via cartesian product
- `self.join()` - SQL FROM clause for cartesian product joins
- `self.keyschema()` / `self.schema()` - Kafka schema helpers

### Tips

- **Aim for ~1 second** query time. Too fast = noise; too slow = Unbearably slow CI.
- **Test td fragments locally** with standalone testdrive before running the full benchmark.
- **Use `SCALE`** to control data volume: `SCALE = 5` → 100K rows.
- **Use `FIXED_SCALE = True`** if `--scale` should not affect your scenario. This is usually done when a scenario doesn't scale well, add an explanation.
- **Use `RELATIVE_THRESHOLD`** to customize regression sensitivity per scenario.

### Running

```bash
bin/mzcompose --find feature-benchmark run default --root-scenario=MyCategory
bin/mzcompose --find feature-benchmark run default --root-scenario=MySpecificBenchmark
```

---

## 2. Scalability Test (SQL Throughput Under Concurrency)

Measures TPS (transactions per second) at increasing concurrency levels (1, 2, 4, 8, ..., 256 clients). Open-loop benchmark that identifies scaling bottlenecks. Compares Materialize against Postgres.

### How to Add a Workload

1. Create a new file or add to an existing file in `workload/workloads/`.
2. Extend a marker class (`DmlDqlWorkload`, `DdlWorkload`, or `ConnectionWorkload`).
3. Implement `operations()` returning a list of `Operation` objects.

### Workload Template (Simple DML/DQL)

```python
from materialize.scalability.operation.operations.operations import (
    SelectOne,
    InsertDefaultValues,
    SelectCountInMv,
)
from materialize.scalability.workload.workload_markers import DmlDqlWorkload

class MyWorkload(DmlDqlWorkload):
    def operations(self) -> list["Operation"]:
        return [InsertDefaultValues(), SelectCountInMv()]
```

### Workload Template (DDL with Data Exchange)

```python
from materialize.scalability.operation.operation_data import OperationData
from materialize.scalability.operation.operations.operations import (
    CreateTableX, PopulateTableX, SelectStarFromTableX, DropTableX,
)
from materialize.scalability.operation.scalability_operation import OperationChainWithDataExchange
from materialize.scalability.workload.workload import WorkloadWithContext
from materialize.scalability.workload.workload_markers import DdlWorkload

class MyDdlWorkload(WorkloadWithContext, DdlWorkload):
    def amend_data_before_execution(self, data: OperationData) -> None:
        data.push("table_seed", data.get("worker_id"))

    def operations(self) -> list["Operation"]:
        return [
            OperationChainWithDataExchange([
                CreateTableX(), PopulateTableX(), SelectStarFromTableX(), DropTableX(),
            ])
        ]
```

### Custom Operations

```python
from materialize.scalability.operation.scalability_operation import SimpleSqlOperation

class MyCustomSelect(SimpleSqlOperation):
    def sql_statement(self) -> str:
        return "SELECT COUNT(*) FROM t1 WHERE f1 > 500;"
```

### Built-in Operations

`SelectOne`, `SelectStar`, `SelectLimit`, `SelectCount`, `SelectCountInMv`, `SelectUnionAll`, `Update`, `InsertDefaultValues`, `CreateTableX`, `PopulateTableX`, `DropTableX`, `CreateMvOnTableX`, `DropMvOfTableX`, `Connect`, `Disconnect`.

### Database Schema (auto-created)

For each workload, the framework creates: `CREATE TABLE t1 (f1 INTEGER DEFAULT 1)`, inserts a row, and creates `CREATE MATERIALIZED VIEW mv1 AS SELECT count(*) AS count FROM t1`.

### Running

```bash
bin/mzcompose --find scalability run default --workload MyWorkload
bin/mzcompose --find scalability run default --workload-group-marker DmlDqlWorkload
bin/mzcompose --find scalability run default --min-concurrency 1 --max-concurrency 128
```

---

## 3. Parallel Benchmark (Sustained Performance)

Measures latency distributions (p50/p95/p99) and QPS over a sustained load period (default 120s). Supports mixed workloads with both open-loop and closed-loop concurrency. Detects latency degradation over time via slope analysis.

### How to Add a Scenario

1. Add a class in `scenarios.py` extending `Scenario`.
2. In `__init__`, call `self.init(phases=[...])` with `TdPhase` (setup) and `LoadPhase` (benchmark).

### Scenario Template

```python
from materialize.parallel_benchmark.framework import (
    ClosedLoop,
    LoadPhase,
    OpenLoop,
    Periodic,
    ReuseConnQuery,
    Scenario,
    StandaloneQuery,
    TdPhase,
)

class MyScenario(Scenario):
    def __init__(self, c, conn_infos):
        self.init(
            phases=[
                # Setup phase (testdrive)
                TdPhase("""
                    > CREATE TABLE bench_t (f1 INTEGER);
                    > INSERT INTO bench_t SELECT generate_series(1, 10000);
                    > CREATE MATERIALIZED VIEW bench_mv AS
                      SELECT COUNT(*) AS c FROM bench_t;
                    > SELECT c FROM bench_mv;
                    10000
                """),
                # Benchmark phase
                LoadPhase(
                    duration=120,  # seconds
                    actions=[
                        OpenLoop(
                            action=StandaloneQuery(
                                "SELECT c FROM bench_mv",
                                conn_infos["materialized"],
                            ),
                            dist=Periodic(per_second=100),
                            report_regressions=True,
                        ),
                        ClosedLoop(
                            action=ReuseConnQuery(
                                "INSERT INTO bench_t VALUES (1)",
                                conn_infos["materialized"],
                            ),
                            report_regressions=True,
                        ),
                    ],
                ),
            ],
            guarantees={
                "SELECT c FROM bench_mv (standalone)": {
                    "qps": 10,
                    "p99": 500,
                },
            },
        )
```

### Action Types

| Class | Behavior | Use When |
|-------|----------|----------|
| `StandaloneQuery` | New connection per operation | Testing connection overhead + query |
| `ReuseConnQuery` | Persistent single connection | Testing pure query latency |
| `PooledQuery` | Draws from connection pool | Testing pooled throughput (set `conn_pool_size > 0`) |

### Load Patterns

| Class | Behavior |
|-------|----------|
| `OpenLoop(dist=Periodic(per_second=N))` | Fixed-rate: N operations/second regardless of latency |
| `OpenLoop(dist=Gaussian(mean, stddev))` | Variable-rate: sleep drawn from gaussian distribution |
| `ClosedLoop(...)` | Sequential: next operation starts when previous completes |

### Guarantees and Regression Thresholds

```python
# Absolute guarantees (fail if not met)
guarantees={
    "SELECT 1 (standalone)": {"qps": 15, "p99": 400, "max": 500},
}

# Relative thresholds (fail if regression vs baseline)
regression_thresholds={
    "SELECT 1 (pooled)": {"qps": 0.8, "p99": 1.3},  # QPS must be >=80% of baseline
}
```

Action names are auto-generated as `"{query} ({connection_type})"`.

### Available Metrics

`queries`, `qps`, `min`, `max`, `avg`, `p50`, `p95`, `p99`, `p99.9`, ..., `p99.999999`, `std`, `slope`.

### Disabling a Scenario

```python
from materialize.parallel_benchmark.framework import disabled

@disabled("Reason for disabling")
class MyScenario(Scenario):
    ...
```

### Running

```bash
bin/mzcompose --find parallel-benchmark run default --scenario MyScenario
bin/mzcompose --find parallel-benchmark run default --scenario MyScenario --load-phase-duration 60
```
