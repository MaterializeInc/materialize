# Introduction

The current upgrade test framework has only the notion of a BEFORE state and an AFTER state, running a set of .td files at each state to confirm correct operation.

This is not sufficient for testing stuff in the Platform world, where multiple components may need to be upgraded in sequence over time, the so-called rolling upgrade.

Therefore we need to treat upgrade as a process, both in terms of the actions that happen during the upgrade and the evolution of any participating database objects. So "before" and "after" are no longer sufficient concepts.

## Other issues with the existing framework

The following additional issues have been identified with the existing framework.

- the method to decide which tests are suitable to run against a particular version is not flexible enough, being based on test file names and globs. A more flexible approach is needed.
- the two parts that together test a particular database functionality sit in different .td files
- Some td files had to contain unreadable `skip-if` directives so as to not run at the wrong time.

# Basic Concepts

The new proposed framework revolves around the following concepts

## Initial State and Final State

* **Initial State** is a working Platform cluster, usually associated with a starting version number.

* **Final State** is a working Platform cluster, usually associated with some final version number

Each **State** has the following properties:
* **Version** Even though theoretically we could elect to start or finish with a multi-version cluster, this is not assumed to be the default starting point or end goal;
* **Configuration** that completely describes the **State** so that the cluster can be completely constructed from scratch given that information. A Configuration may also need to include not just parameters, but also a set of SQL statements to execute to instantiate the cluster.

## Scenario

A **Scenario** is a sequence of steps by which the **Initial State** will get converted into a **Final State**

## Steps

A **Step** in a scenario is a single action that is being undertaken with an end goal of performing the complete upgrade once all **Step**s have been completed. A **Step** can be one of the following:

- a non-Kubernetes **Action** that operates on the cluster, e.g. starting or stopping a Mz process in a non-Kuberenetes environment;
- a **KubernetesCommand** that is sent to Kubernetes API which results in some operation being carried by K8s against the cluster. For example, instructing K8s to tear down a Pod would cause Kubernetes to kill a particular set of running containers;
- a Kubernetes action that Kubernetes performs on its own, such as restarting containers that K8s is instructed to keep up at all times
- a **MaterializeCommand** that is sent to Mz directly, most often via the SQL interface, to manage Materialize. This would usually be a testdrive fragment

# Checks

In addition to the steps above, the framework will perform the following during the process:
- **ImplicitCheck**s will be automatically executed at every stage of the test without the need to be explicitly scripted in and without relying on specific database objects to have been created or present;
- **ExplicitCheck**s will run at the pre-defined important times defined in the scenario

# Explicit Checks

Explicit checks refers to creating and populating custom database objects with the desire to explicitly validate their contents later in the upgrade scenario.

Explicit checks have the ability to decide by themselves if they can run successfully in a particular upgrade context.

## Defining an Explicit Check

Each Check is a Python class that has the following methods:

```
class ExplicitCheck:
    def populate(self): -> List[Td]:
       ...

    def validate(self): -> Td:
       ...
```

`populate()` returns a list of testdrive fragments to run. Depending on how many opportunities there are to run the `populate` fragments within a **Scenario**, the framework may run all of them together, or space them apart to run individually as the upgrade proceeds.

`validate()` returns a testdrive fragment that validates that Mz is operating correctly with respect to any stuff that happened during population. The framework will start running this fragment at every opportunity after all the parts of the populating have completed.

## An Example Explicit Check

```
@requires(initial_version > 001900)
class KafkaSource(ExplicitCheck):
    def populate(self) -> List[Td]:
        return [
            Td("$ kafka-create-topic + ... CREATE SOURCE ..."),
            Td("$ kafka-ingest # Ingest first row"),
            Td("$ kafka-ingest # Ingest second row"),
        ]
    def validate(self) -> Td:
        return Td("> SELECT MIN(f1) = 1, MAX(f1) = 2, COUNT(*) = 2 FROM s1")
```

# Implicit Checks

The implicit checks will run as frequently as possible during the test, as long as the Mz instance is expected to be operational, without having to be explicitly scripted in. This provides the following benefits:

- automatic checks that Mz is up and responsive at all times during any sort of rolling upgrade, without the developer having to explicitly think about checking stuff at each individual point of time in each individual scenario being tested;
- automatic checks that basic functionality such as creating tables, sources and materialized views on top of them works at all times even if the developer has not added such tests as **ExplicitCheck**s

Examples of such checks are given at the bottom of the document. Implicit checks make no assumptions as to the contents of the Mz instance when it runs.

# A Complete Scenario

Here is an example of a complete scenario that ties all the concepts above together:

```
class SimplestUpgrade(Scenario):
    def scenario(self) -> List[Actions]:
        return [
            StartMaterialize(config = self.initial_config),
            Populate(),
            StopMaterialize(),
            StartMaterialize(config = self.final_config),
            Populate(),
            ExplicitChecks()
        ]
```

The framework will perform the following preparations:

1. Determine `initial_config` and `final_config`. In the absence of overrides, the config will be a simple cluster of some default shape and size. If we are testing upgrade to version `X`, the initial version will be set to `X-1` and the final version to `X`. A separate run may test `X-2` to `X`;
2. The list of available `Check`s will be compiled. Some Checks may elect to skip themselves at this stage if they are not compatible with both version `X-1` or version `X`;
3. Given that the `KafkaSource` check is fully populated over 3 testdrive fragments but the scenario above only has two `Populate` points, the first two fragments will be scheduled to run at the first population point.

And then run the scenario as follows:

1. Start an old-version Mz
2. Create a kafka topic, create a source, ingest 1 row into source
3. As we have a running Mz instance, perform the implicit checks
4. Stop the old-version Mz
5. Start a new-version Mz
6. Perform another round of implicit schecks as we again have a running Mz instance
7. ingest 1 more row into source
8. Perform another round of implicit checks
9. As we have now completed all the populate steps for the `KafkaSource`, perform the `validate()` part for it
10. Perform any in-framework hard-coded final checks, such as whether the final shape of the cluster matches `final_config` and has not been silently degraded in any dimension (that is, all expected clusters and their replicas and all individual processes are all running)

A more realistic scenario, of course, will stop and upgrade the individual parts of Mz separately. Therefore, the separate processes will transition from `initial_config` and version to `final_config` and version as a consequence of a set of individual steps explicitly specified in the scenario.

# Potential Checks

## Explicit Checks

The .td files in the existing upgrade framework can be used to source a list of the things one would want to validate during an upgrade for example:
- creating table columns, materialized views and avro schemas referencing all possible data types; populating said objects with data

Platform has some additional requirements:

- creating a materialized view containing all possible SQL functions, LIR constructs and such (to test the operation of the Protobuf serialization/deserialization)
- creating clusters, replica sets, secrets and other such.

## Implicit Checks

The full list of things that are amenable to implicit checking is:
- connecting to the Mz instance
- creating (uniquely-named, or placed in a uniquely-named schema) tables and sources
- inserting and selecting from a base table
- creating a materialized view, ingesting data, selecting from a materialized view
- creating simple sinks
- confirming that the various introspection tables are queryable and are not empty
- dropping the temporary objects created above
- (potentially) creation of new clusters
- (potentially) creation of new replicas
