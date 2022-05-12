# Inroduction

The Feature Benchmark provides facilities and ready-made benchmarks that measure the execution time of individual operations and
SQL statements in Materialize. It is meant to sit somewhere between a microbenchmark one would write in Rust and database benchmarks
such as TPC-H.

# Running

The `--help` option can be used to show supported options:

```
./mzcompose run feature-benchmark --help
```

To run the default benchmark scenarios:
```
cd test/feature-benchmark
./mzcompose run default --other-tag unstable
```

Or, from the root of the repository:

```
bin/mzcompose --find feature-benchmark run default --other-tag unstable
```

This is going to benchmark the current source against the `materialize/materialized:unstable` container from DockerHub.

To run one benchmark scenario or a named subset:

```
./mzcompose run default --root-scenario=FastPath
```

To use specific Mz command-line options:

```
 ./mzcompose run default --this-options "--workers 16" --other-options "--workers 1"
```

To compare specific Mz versions:

```
./mzcompose run default --this-tag v1.2.3 --other-tag v2.3.4 ...
```

To compare specific Mz git revisions:
```
./mzcompose run default --this-tag unstable-42ad7432657d3e5c1a3492fa76985cd6b79fcab6 ...
```

To start a 2-node computed cluster for `THIS`:

```
./mzcompose run default --this-nodes 2 ...
```

# Output

The output of the benchmark is a table such as this:

```
NAME                      |        THIS |       OTHER | Verdict: THIS is
--------------------------------------------------------------------------------
Sleep                     |       0.101 |       0.101 | about the same -0.1%
FastPathFilterNoIndex     |       0.309 |       0.292 | about the same +5.5%
FastPathFilterIndex       |       0.002 |       0.002 | about the same +0.4%
FastPathOrderByLimit      |       0.169 |       0.170 | about the same -0.7%
Insert                    |       1.414 |       1.403 | about the same +0.7%
Update                    |       0.781 |       0.803 | about the same -2.7%
```

The table is printed periodically as new rows arrive so that the information is not lost in case of failure.

# Available scenarios

The `test/feature-benchmark/scenarios.py` file contains the definitive list of scenarios. Scenarios follow a hierarchical structure.

The default scenarios derive from the `Scenario` class, those that take longer to execute derive from the `ScenarioBig` class.

# Features

This framework has the following features:

* A focus on comparison runs as opposed to storing and charting data longer-term. The benchmark is run against two instances of Mz and their numbers are compared

* A focus on the performance of individual operations in a single-user-connection context (single-user latency), as opposed to other dimensions such as transactions/second,
  throughput or latency under concurrency.

* The framework automatically decides how many iterations to run

* The operations to benchmark are expressed as a testdrive scripts, which provides access to facilities such as kafka ingestion, retrying of a query until it returns the desired result, etc.

* It is runnable by developers on development machines and provides quick feedback on regressions and performance gains.

# Measurement methodology

The framework is organized around the idea of repeatedly executing the feature under test until a termination condition is met. After measurements have been accumulated, a definitive performance value is chosen.

All the rules and constants governing the process can be seen at the top of `test/feature-benchmark/mzcompose.py`.

## Executor

An Executor runs the desired query and provides back the measurement value. Currently, testdrive is used to execute and the wallclock time elapsed between two points in the testdrive script is measured. Other methods for execution will be provided in the future.

## Termination criteria

The purpose of the termination criteria is to allow for the benchmark to decide the number of needed iterations without operator input.

Not doing an excessive number of iterations is very important as it is what makes the benchmark runnable locally and as part of the development process rather than be delegated to dedicated hardware, teams and nightly schedules.

The framework will keep running the benchmark until one of the predefined termination criteria is met.

Currently, two criteria are used:
* The shape of the normal distribution of the values measured has been established with enough precision so that newer values are unlikely to modify the picture.
* The probability that a value will arrive that is even lower than any of the values already observed goes below a certain threshold.

The code currently assumes that the data follows a normal distribution, which is decidedly not the case, especially since extreme outliers are frequently observed. More advanced statistical
techniques for massaging the numbers may be required in the future. In particular, there is some evidence that the distribution of values is multi-modal with: A) one peak for all operations that completed
within some "tick" (either the kernel rescheduling some thread, or e.g. the timestamper thread kicking in): B) one peak for operations that completed immediately after the "tick" and C) extreme outliers that were particularily unlucky.

To limit the maximum number of measurements at the expense of accuracy, use the `--max-measurements N` option.

## Aggregation policy

The aggregation policy decides how to compute the definitive performance value out of the measurements.

Currently, the `Min` strategy is used, that is, the minimum value measured is used. This means that the benchmark will de-facto measure the performance of the critical path of the feature under test.

## Failure criteria

The framework will report failure in case a particular measurement indicates a performance regression between the two Mz instances that exceeds a particular threshold. Otherwise the number will
be reported.

# Troubleshooting

## Benchmark terminated prematurely

If the feature benchmark terminates prematurely with an error such as
```
Failed! giving up: testdrive_this --no-reset --seed=1639555967 --initial-backoff=0ms --backoff-factor=0 tmp/tmp-gq4qxj88.td
```

This indicates that testdrive could not run the queries from the benchmark definition. To understand what went wrong, use `docker ps -a` to see the ID
of the `feature-benchmark_testdrive_*` container that exited most recently and then `docker logs <id_of_the_container>` to obtain the output from testdrive.

## Dealing with false positives

The feature benchmark will retry any regressed scenarios up to `--max-cycles=3` times in order to weed out false positives. mzcompose.py will exit with a non-zero
exit code only if there are scenarios that reported a regression every time they were tried.

# Writing benchmark scenarios

## Benchmark definition format

Benchmark scenarios live in `test/feature-benchmark/scenarios.py` and follow the following format:

```
class NewBenchmark(Scenario):
    def shared(self) -> Action:
        return TdAction("""
#
# Place here the testdrive commands that need to be run only once for both Mz instances under test
# and that prepare resources that will be common for both Mz instances.
#
# Usually, those are commands around creating and populating Kafka topics.
#
""")

    def init(self) -> Action:
        return TdAction("""
#
# Place here any testdrive commands that need to be run once for each Mz instance before
# starting the respective benchmark.
#
# This usually includes creating and possibly populating database objects of various kinds
#
# Before continuing, place queries that check that all the dataflows are fully hydrated
#
""")

    def before(self) -> Action:
        return ...

# The before() section is reserved for actions that need to happen immediately prior to the
# queries being benchmarked, for example, restarting Mz in order to force a recovery

    def benchmark(self) -> MeasurementSource:
        return Td(f"""
#
# Place here all queries that need to be repeated at each iteration of the benchmark, including
# the queries that will be benchmarked themselves. The execution time between the *end* of the
# query marked with /* A */ and the end of the query marked with /* B */ will be measured
#
>  SELECT 1
   /* A */
1

>  SELECT COUNT(*) FROM t1
   /* B */
{self.n()}

#
# Make sure you delete any database objects created in this section
#

> DROP this_and_that

""")
```

## Scale and `N`

The benchmarks are parameterized on their scale, that is, one can create scenarios that run with some default size which can then be overwritten:

```
class ReallyReallyLargeBenchmark(ScenarioBig):
    SCALE = 10
```

From `SCALE`, the framework calculates `N`, which is `10^SCALE` and makes them available within the scenario as `self.scale()` and `self.n()` respectively.

The default `SCALE` is 6 , that is, any benchmark scenario that is properly parameterized will run benchmark 1M of the thing being benchmarked.

To force the framework to run with a specific scale, use the `--scale N` option. If `N` is prefixed by `+` or `-`, it is interpreted as a relative adjustment to apply to the default scale.

## Actions vs. Measurements

The `shared()`, `init()`, and `before()` sections of the benchmark must return an `Action` object or a list of `Action` objects (usually `TdAction`) while the `benchmark()` section
must return a `MeasurementSource` object (currently, only `Td` is supported).

Available `Action`s:

* `TdAction(...)` - executes the testdrive fragment provided
* `Kgen(...)` - executes the `Kgen` to ingest data into a Kafka topic (see the KafkaRecoveryBig scenario for an example)
* `Lambda(lambda e: e.RestartMz())` - restarts the Mz service


## Running the same scenario with multiple times within the same run

It is possible to use the python `parameterized` module to cause the same scenario to be executed multiple times. For example, with different scale factors:

```
@parameterized_class(
    [{"SCALE": i} for i in [5,6,7,8]], class_name_func=Scenario.name_with_scale
)
class ScalingScenario(ScenarioBig):
    def benchmark(self) -> MeasurementSource:
        ...
```

## Caveats and considerations

* Aim for the query or queries being benchmarked to take around 1 second to execute. This allows the framework to perform several iterations without blowing up
the total execution time. Avoid queries that take milliseconds to run, as the noise from various sources will overwhelm any signal.

* The benchmarks will run with `--timestamp-frequency` and `--introspection-interval` set to `100ms`. This places a lower bound on the granularity and the
information value obtained when benchmarking anything impacted by those intervals. Ingest enough data into Kafka so that operations take many times the
timestamp frequency to complete.

* Operations in Materialize are more asynchronous than a person familiar with typical databases would expect. For example, `CREATE INDEX` returns before the
index has been fully populated; DML operations may return to the user before all of their work is done. In both of those cases, the extra latency would be observed
on a follow-up `SELECT` statement. Therefore carefully curate the list of queries in the individual blocks to avoid measuring less or more operations than desired.

* Note that the framework will capture the time elapsed between the *end* of the query marked with `/* A */` and the *end* of the query marked with `/* B */`.
If you are benchmarking a single query, put the `/* A */` marker on a dummy `SELECT 1` query as shown in the example above.

* Before attempting to run your testdrive fragments, put them in a stand-alone .td file and run them manually through `testdrive` so as to avoid repeated debugging
cycles with the benchmark running.

# Bisection

It is possible to use `git bisect` to determine the specific revision when a performance regression occurred

## Running

The steps are pretty straightforward

```
git bisect start
git bisect good vGOOD.MZ.VERSION
git bisect bad HEAD
git bisect run /path/to/bisect.sh
```

The `bisect.sh` can be something along the following lines:

```
#!/bin/bash
THIS_SHA=$(git rev-parse HEAD)
GOOD_MZ_VERSION="vX.Y.Z"

pushd /path/to/test/feature-benchmark
./mzcompose down -v
./mzcompose run feature-benchmark --this-tag unstable-$THIS_SHA --other-tag $GOOD_MZ_VERSION --root-scenario=...
```
