# Microbenchmarking framework

## Summary

In order to characterize the performance of the product and guard against regressions, we need to be able to benchmarks the invidiual operations Mz performs when it comes to ingesting and processing data and materializing views.

Microbenchmarking will be in addition to any end-to-end benchmarks that exist or will be developed. It aims to provide a more focused spotlight on regressions: instead of a 2% regression in an entire industry benchmark between product versions X and Y, we will be looking at a 20% regression in a specific named operation between specific commits M and N.

## Goals

Measurements:
- measure the performance of the query optimization stage, to avoid the introduction of O(N^M) algorithms in the optimization pipeline
- measure the performance of ingestion of various sources, envelope types and data shapes
- measure the performance of ad-hoc SELECT queries, both those that construct a dataflow and those do not
- measure the ingestion performance of materialized operators, that is, how fast a materialized query returns an up-to-date result

Output:
- provide a report that compares the branch under development/test to some benchmark, such as `main` or `materialized/latest`.

Usability goals:
- allow for individual microbenchmarks to be specified intuitively
- be runnable on a laptop while remaining compatible with the CI
- do not require setting any knobs, thresholds, etc.

## Non-Goals

- produce graphs or any other collateral for marketing purposes
- benchmark end-to-end customer workloads or run industry-standard benchmarks
- benchmark scaling, concurrency (except possibly concurrent ingestion of a single stream)
- provide any sort of "transactions-per-second"(and "per-dollar") values of any kind

## Description

### Specifying individual tests

An individual tests is a python class that specifies the actions that need to be taken prior to executing the operation under test, the thing to be tested, and any cleanup.

For example, this microbenchmark will create a Kafka topic, ingest 100000 rows into it and measure the time it takes for the source to catch up.

```
class PartitionedKafkaIngestion(WallclockUntilSucces):
   COUNT = 100000
   def benchmark(c: Context):
       return Benchmark(
           setup = [
               KafkaCreateTopic(partitions=5),
               KafkaIngest('{"f1": 1} {"f2": 1}'),
               CreateSource()
           ],
           benchmark = Sql("SELECT COUNT(*) = {COUNT} FROM {SOURCE}")
       )
```

Internally, each of the helper classes, such as `KafkaCreateTopic` will produce a testdrive fragment such as `$ kafka-create-topic partitions=5` that will be passed on to testdrive for execution.

### Running

The execution will be via a `mzcompose`-based python script, something along the following lines:

```
cd test/microbenchmark
./mzcompose run microbenchmark --compare-to materialized/latest
```

### Output

The output will be something along those lines:

```
Microbenchmark            Iterations | Unit        | This:                 | Other:              | Outcome
                                     |             | d45bbc0+modifications | materialized/latest |
-----------------------------------------------------------------------------------------------------------------------------
PartitionedKafkaIngestion         25 | records/sec | 11000                 | 10000               | This is 10% faster than Other
...
CountStar                            | ...         | ...                   | ...
CountDistinct                        |             |
OrderByLimitMaterialized             |             |
OrderByLimitFinish                   |             |
...
```

### Measurement strategies

The measurement strategy describes exactly what will be measured and how.

#### `Wallclock` strategy

This strategy will measure the wallclock time between the last operation of the setup (the Kafka ingestion from the example), and the time the query under observation (the `SELECT COUNT(*)`) begins to return the expected result.

For some queries, the result is available immediately without retrying, for example:

```
EXPLAIN SELECT * FROM t1 AS a1 LEFT JOIN t1 AS a2
```

or

```
SELECT COUNT(DISTINCT f1) FROM t1;
```

against a fully-ingested source.

In our example above, the `SELECT` query will be retried until it returns the expected response. As `testdrive` is being used to execute the query, it will be instructed to 1) retry the query without an exponential back-off until it succeeds and 2) report wallclock execution times.

#### `Counter` strategy

This will be used in case the measurement needs to be obtained by querying a particular counter or parsing information in the `EXPLAIN`.

### Benchmarking strategies

We are going to measure and report the **minimum** time it took to execute a particular operation. This has the following advantages:
- measures the performance of the critical path for a given operation. Any code that is added to the critical path will hopefully cause the minimum measurement to regress and will be detected and reported;
- it is a bit more robust against outliers, as more outliers are to be expected on the long tail of slower measurements

Ideally, we would want the user to not have to specify anything about the number of iterations to run, etc. Instead, the algorithm will self-tune and run the benchmark enough times until some terminal condition is met, which would be some combination of:
- the standard deviation between the measurements stabilizing or dropping below a certain value
- the probability of a better measurement arriving drops below a certain threshold

### Language choice

Rather than re-implementing topic ingestion, s3 bucket creation and all of that in a different language, we will use `testdrive`s facilities for the purpose.  Testdrive will be modified to provide any output or behavior needed.

On the other hand the glue code that aggregates measurements and produces reports will be written in python.


## Alternatives

The alternative is to proceed directly with an end-to-end benchmark using customer data and workloads as described in the Snowflake paper:
https://www.researchgate.net/publication/325586461_Snowtrail_Testing_with_Production_Queries_on_a_Cloud_Database


## Open questions

- should this be added as a pass/fail test to CI? If so, we are back to the question of picking appropriate thresholds.
