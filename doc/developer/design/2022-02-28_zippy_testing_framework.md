# Prior Experience

Some recent experience from our CI has shown that even more testing needs to happen along the Source -> Persistence -> Dataflow axis. In particular, the following has been observed:
- bugs were only observed sporadically, and not on the initial CI run of the PR that introduced them;
- bugs/regressions have been observed on unrelated tests, e.g. non-persistence-related issues were observed on the persistence test; Issues not requiring a Mz restart were observed on tests that restart Mz repeatedly;
- some issues are dependent on the total number of kafka sources in flight. At the same time, tests do not really target a particular number of sources/dataflows running concurrently. Instead, that number is largely dependent on the chance organization of a particular testing framework, e.g. how often it happens to issue `DROP DATABASE` or restart Mz.
- the overwhelming majority of our CI tests do not operate on data of meaningful size -- very frequently just a couple of records are ingested and processed through the system. This has caused unrelated issues to be spotted by the few tests that deal with record numbers in the tens of thousands.
- As a corollary to the small data sizes currently in use, the number of unique timestamps being processed within a given dataflow is very low, often just a couple. This means the system is not exercising for the cases where thousands of distinct timestamps may need to be processed, persisted or inserted into SQLite very often.

# Summary

Ideally, we would want to be running very large scale, very long running stress tests that are also subjected to repeated Chaos-monkey-style disruptions. Undoubtedly, applying a truly random workload to the database in the face of random concurrent failures would cause great stress to the product and has great potential of shaking out many bugs.

Unfortunately, such a testing approach makes for bugs that are difficult to reproduce, interiorize and understand. This in turn severely impacts the probability that such bugs will be taken seriously enough to be fixed. Companies have struggled with large-scale testing, expending obscene amounts of money on EC2 instances and man-hours on test frameworks that run for days before failing and remain in permanently-failing state forever.

Therefore, we will try to expand our testing while still aiming for human-scale, that is, run stressful tests but make sure they can be executed on a single laptop or a single EC2 instance before venturing onto very large deployments. To that end, we define the following goals:

## Goals of the Project

We need a framework that:

- can exercise multiple aspects of the Platform (sources, sinks, persistence, dataflowd, coord) together in a single run

- run randomized scenarios while allowing repeatability and reproducibility

- check the correctness of Mz operation across larger datasets by maintaining longer-running dataflows and validating them periodically

- compatible with the Antithesis environment (which requires the ability to run varied tests of bounded duration based on a random seed value)

- provide a framework that will provide more testing value as more execution time is given to it. This way it can be run for extended periods of time, e.g. in a Weekly CI, while knowing that it is not repeatedly beating the same code path over and over again

# High-level Description

## Execution

The tests will run in the existing mzcompose framework and allow for single-command-line execution:

```
./bin/mzcompose --find zippy run default [--dump] --starting-seed=K --max-tests=L --max-action-count=M
```

If `dump` is specified, the framework will also dump the entire test scenario in a format that allows it to be added as a stand-alone `mzcompose.py` test case in the regression test suite.

## Operation

The command line above will perform the following steps `L` times:

1. Initialize a PRNG based on `N`;
2. Generate a complete `Test` that is a sequence of `Action`s at most `N` steps long, with each `Action` being a different thing that would happen to the system; Available `Action`s are described below;
3. For each `Action` that requires parameters, use the PRNG to pick values for those parameters;
4. Once the `Test` is fully constructed, execute it or dump it to a readable/usable format

## Output

The output will be the usual `mzcompose` output of actions being taken and `testdrive` executions. Failures will appear the way they currently do for `mzcompose` executions.

# An Example

We start with a seed of `1234` and a built-in list of all possible `Action`s, e.g. [`RestartMaterialize`, `Ingest`, `CreateMaterializedSource`] and so on. Based on that, we pre-generate the entire sequence of actions that will be executed:

```
StartMaterialize(),
Ingest(topic_name='topic1', count=10000),
CreateMaterializedSource(),
Ingest(topic name='topic1', count=20000),
Validate()
```

Pre-generating the sequence allows the entire sequence to be displayed at the start of the test, allowing the operator to easily see what will be tested.

The framework then proceeds to convert each of those `Action`s into a piece of a `mzworkflow` and execute it.

# Low-level Design

## Sequential Execution

It helps a lot that each `Test` will be a sequence of `Actions`. This provides for the following benefits:

- we can validate in advance that the entire sequence is correct, is not self-contradictory, and can be expected to complete to the end on a bug-free system;
- in case of bugs, apply an automatic simplification algorithm over the list of actions in order to arrive at the smallest list that still reproduces the problem
- dump the test in python or other readable formats, so that it can be reasoned about, as it is a sequence of steps rather than some jumble of random actions happening at random times

Sequential execution has the drawback that there will be no truly guaranteed concurrency happening between the events in the Cluster. We can alleviate this problem somewhat by:
  - ingesting records asynchronously in Kafka before immediately moving to some other Action, either validation or failure injection
  - when injecting failures, inject multiple of them against multiple components at once as part of a single `Action`

## Actions

A test consists of a set of discrete `Action`s, where each `Action` belongs to one of the following classes:
- starting/restarting Mz/coord/dataflowd/kafka
- creating new topics/ingesting data in Kafka
- other failure-ingestion steps (pausing processes, network disruptions)
- creating and dropping sources
- creating and dropping materialized views
- creating and dropping materializations over unmaterialized sources
- data validation

## Selection of Actions

All the `Action`s participating in a given `Test` are selected before the start using a PRNG. The following considerations apply:

### Workload realism

The probability of each action being picked (with the exception of failures, discussed below) should have some grounding in the workload distribution of production systems. For example, we want dataflows that are not being dropped and recreated constantly so that they live longer than a few seconds each. Few, but long-lived Kafka topics should be the norm vs. many short-lived ones.

### Probability of disruption

We want to introduce our random failures in a way that would cause the cluster to reach a variety of states and be subjected to a variety of situations. Simple randomness would not achieve that goal as it is likely that no sufficient state would have accumulated on disk or in memory between failures/restarts to stress any recovery code.

Therefore, we will carefully control the probability of the failure-injecting `Action`s . If necessary, a `PreventFailures` action will also be used to provide calm periods for sufficient state to accumulate.

### Allowed actions

At any given time, a given `Action` may not make sense, such as attempting to issue a SQL query while `coord` is down. To avoid those situations, while generating the list of `Actions` that will participate in a `Test`, the framework will maintain a data structure that describes the current state of the system and use that do decide what actions are allowed at that state.

### Action Selection Algorithm

There are two ways to achieve the desired probabilities or some approximation thereof:
1. Assign each `Action` a probability and only pick it in proportion to its probability
2. List the `Actions` in the order of desired frequency and then apply a skewed picking algorithm that will favor the front of the list. Within such a list, Kafka ingestion would be placed first while dropping an existing dataflow would be down the list, resulting in a much smaller probability of occurrence.

## Test Case Reproducibility

A `Test` is entirely defined by the tuple `(random_seed,git_sha)` so that all the `Action`s happening can be reproduced in their entirety and re-executed verbatim from just those two parameters.

## Metadata Tracking

In order for `Actions` to operate only on objects that we know have been created by some prior `Action`, we keep various pieces of metadata as we generate the `Test`:

- the ID of the latest Kafka topic created from each envelope type. We create the Kafka topics with sequential names, `upsert1`, `upsert2`, etc.
- a map of the names of the sources that we have created
- a map of the names of the views we have created

## Validation

The `Validation` `Action`'s  purpose is to detect that Mz has stopped running a particular dataflow for any reason and/or is returning wrong results. So far, the following types of failures have been seen:
1. Mz crashes
2. Mz returns an error when querying a source that should have been error-free
3. Mz stops processing data on some boundary, that is, after a complete set of messages have been processed but before another complete set of messages
4. Mz stops processing in the middle of ingesting some set of Kafka messages

1, 2 are trivial to handle with a simple `SELECT` query. We tackle 3 and 4 using a mechanism that independently tracks the data that has been inserted/ingested so far:

### Data Tracking

For each topic/source/dataflow/sink we maintain enough data to allow us to create a validating `SELECT` query that will fail if the Mz database object does not contain the expected data.

We will maintain:

* for each topic, the ID of the first (`IDFirst`) and the last (`IDLast`) record that has been ingested. This allows us to perform the following types of ingestions and still validate the correctness of the Mz processing:
  - On Kafka ingestions, we increment `IDLast`.
  - On upsert deletions from the tail, we increment `IDFirst`
  - On upsert deletions from the head, we decrease `IDLast`
  - For updates of the key, we delete a key from the start of the topic and insert it at the end of he topic, adjusting `IDFist` and `IDLast` accordingly;
  - for updates to the values, we update all records to a new value and expect that no instances of the old value will ever be returned by a future `SELECT`

# Available Actions

We define the following `Actions`:

## `RestartMz` /`StopMz` / `PauseMz`

## `StartKafka` / `StopKafka`

This will allow periodic Kafka downtime to be introduced. That downtime must be bounded to the maximum value that Mz will support without erroring out the source.

## `CreateSource` / `CreateTable`

## `CreateIndex` /  `CreateView`

We will also be generating views that join data from multiple sources while still allowing validation to be performed against them.

## `Ingest`

We will ingest data in a manner that will give us a "long-tail" distribution of topics and their sizes -- most of the data will be ingested in a couple of larger topics, but the framework will also have the opportunity to probabilistically create a lot of shorter topics and tickle amounts of data in them

## `Validate`

As described above.

## `CompoundFailure`

A compound failure will inject multiple `Stop`, `Restart`, `Pause` failures in the cluster in very rapid succession. This vastly increases the probability that cluster-wide disruptions such as split-brain, failure of all processes of a given type, etc. will happen as opposed to relying on the chance arrangement of individual failure `Action`s to bring about the same situation.
