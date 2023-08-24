# Introduction

The scalability test framework attempts to determine how the product scales in terms of concurrent SQL connections

# Goals

The goals of this framework are to:

- Identify bottlenecks in the product, that is, workloads that do not scale well for some reason.

- Evaluate the performance impact of changes to the code and detect regressions


# Running


1. Collect the data:

```
cd test/scalability
./mzcompose run default
```

2. Start Jupyter lab

```
./mzcompose run lab
```

This is going to display a URL that you can open in your browser

3. Open `scalability.ipynb`

4. From the Run menu, select `Run All Cells`

5. Select the workload you want charted from the drop-down

6. Use drag+drop to preserve any charts

## Selecting a target

The framework can be directed to execute its queries against various targets. Multiple targets can be specified within a single command line.

### Against your current HEAD in a container

```
./mzcompose run default --target HEAD ...
```

### Against a specific DockerHub tag:

```
./mzcompose run default --target v1.2.4 ...
```

In both of those cases, Materialize, CRDB and Python will run within the same machine, possibly interfering with each other

### Against a local containerized Postgres instance


```
./mzcompose run default --target postgres ...
```

### Against a remote Materialize instance

```
./mzcompose run default --target=remote \--materialize-url="postgres://user:password@host:6875/materialize?sslmode=require" --cluster-name= ...
```

## Specifying the number of datapoints

The framework uses an exponential function to determine what concurrencies to test. By default, exponent base of 2 is used, with a default
minimum value of 1 and maximum of 256. To get more data points, you can modify the exponent base to be less than 2:

```
./mzcompose run default --exponent-base=1.5 --min-concurrency=N --max-concurrency=M
```

# Accuracy

The following considerations can potentially impact the accuracy/realism of the reported results:

## Calculation method

The framework is an open-loop benchmark, so it just pumps SQL statements to the database as fast as it can execute them.

The transactions-per-second is calculated in the following ways:

- the total wallclock time needed for all Python threads to complete their work divided by the total number of operations performed
- the sum of the execution times of the individual operations divided by the number of threads and the total number of operations

## Python threading

The framework uses `concurrent.futures.ThreadPoolExecutor` . We measured the time it takes to run a no-op in Python , as well as the time it takes
to run a `time.sleep()` , and , while both produced unwelcome outliers, it does not seem that Python would be obscuring any major trends in the charts.

## Colocation of Python, Materialize and CRDB

By default, all participants run on a single machine, which may influence the results. In particular, CRDB exhibits high CPU usage, which may
be crowding out the remaining components.

Consider using a Materialize instance that is not colocated with the rest of the components.

## End-to-end Latency

To reduce end-to-end latency, consider using a bin/scratch instance that is in the same AWS region as the Materialize instance you are benchmarking.
