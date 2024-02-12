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

### Against the common ancestor

This resolves to the commit of the merge base of the current branch.
* In Buildkite, this is the last shared commit of the current branch and the merge target. When not in a pull request,
`latest` will be used.
* When running locally, this is the lasat shared commit of the current branch and the `main` branch.

```
./mzcompose run default --target common-ancestor ...
```

## Detecting regressions
A regression is defined as a deterioration in performance (transactions per seconds) of more than a configurable
threshold (default: 20%) for a given workload and count factor compared to the baseline.

To detect a regression, add the parameter `--regression-against` and specify a target. The specified target will be
added to the `--target`s if it is not already present.

## Specifying the concurrencies to be benchmarked

The framework uses an exponential function to determine what concurrencies to test. By default, exponent base of 2 is used, with a default
minimum value of 1 and maximum of 256. To get more data points, you can modify the exponent base to be less than 2:

```
./mzcompose run default --exponent-base=1.5 --min-concurrency=N --max-concurrency=M
```

## Specifying the number of operations

The framework will run `--count=256` operations for concurrency=1 and then multiply the count by sqrt(concurrency) for higher concurrencies.

This way, a larger number of operations will be performed for the higher concurrencies, leading to more stable results. If `--count`
operations was used when benchmarking concurrency 256, the test would complete in a second, leading to unstable results.

# Interpreting the diagrams

## Transactions per second (tps)

This diagram show the transactions per second per concurrency. Higher values are better.

## Duration per transaction

These plots show the duration of the individual statements per concurrency. They provide information about the mean
duration of an operation and their timing reliability. Lower values are better.
Violin plots are used by default, boxplots are available as alternative.

### How to interpret the violin plots
The violin plots show the distribution of the data.
The dark blue bar shows the interquartile range, which contains 75% of the measurements.
The horizontal dark blue line shows the median.

See also: https://en.wikipedia.org/wiki/Violin_plot

### How to interpret a boxplot
The most important things in a nutshell:
* 50% of the measurements are within the box.
* The colored line in the box represents the median value.
* The whiskers range until the last data point within the 1.5 times of the interquartile range (size of the box).
* Dots are outliers.

See also: https://en.wikipedia.org/wiki/Box_plot

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
