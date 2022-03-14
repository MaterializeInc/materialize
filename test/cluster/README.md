# Materialize clustering demo

## Overview

This is a demo of running Materialize in clustered mode, where the dataflow
computation is sharded across an arbitrary number of machines.

Clustering is an **enterprise feature.** Per the terms of our
[LICENSE](/LICENSE), you may not run Materialize in clustered mode without
an enterprise license.

⚠️ **WARNING!** ⚠️ Materialize clustering is experimental! Do not use this in
production.

## Getting started

To run this demo, you'll need three separate machines. These machines need to
be accessible via DNS at the following names:

  * `coord`, which will run the SQL coordinator
  * `dataflow1` and `dataflow2`, which will each run one dataflow worker

You can, of course, use different names, but you'll need to update the commands
below accordingly. Note that we use Docker as a convenient method of
distributing the `dataflowd` and `coordd` binaries. You can also build these
binaries yourself from the repository (e.g., `cargo build --release --bin
dataflow`).

On `dataflow1`, run:

```
docker run -p 2101:2101 -p 6876:6876 materialize/dataflowd:latest --workers 2 --processes 2 --process 0 0.0.0.0:2101 dataflow2:2101
```

On `dataflow2`, run:

```
docker run -p 2101:2101 -p 6876:6876 materialize/dataflowd:latest --workers 2 --processes 2 --process 1 dataflow1:2101 0.0.0.0:2101
```

On `coord`, run:

```
docker run -v /share/mzdata -p 6875:6875 materialize/coordd:latest -D /share/mzdata dataflow1:6876 dataflow2:6876
```

Then connect to the coordinator via psql:

```
psql -h coord -p 6875 -U materialize materialize
```

## Tweaking parameters

You can run dataflow clusters of arbitrary size. Suppose you want to run a
cluster with *N* dataflow nodes and *W* worker threads per node. To launch
the *I*th dataflow node, run:

```
docker run -p 2101:2101 -p 6876:6876 materialize/dataflowd:latest \
    --workers <W> \
    --processes <N> --process <I> \
    --hosts dataflow1:2101 ... 0.0.0.0:2101 ... dataflow<N>:2101
```

To launch the coordinator:

```
docker run -p 6875:6875 materialize/coordd:latest \
    --dataflowd-addr dataflow1:2101 dataflow2:2101 ... dataflow<N>:2101
```

You should generally choose *W* to match the number of cores on each dataflow
node.
