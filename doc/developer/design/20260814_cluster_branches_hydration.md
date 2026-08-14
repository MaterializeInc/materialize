# Cluster Branches: Hydration

- Parent: [`20260814_cluster_branches.md`](./20260814_cluster_branches.md)

This document details how a **cluster-resident object** on a branch cluster is hydrated. Every index and materialized view on the branched cluster re-renders on the branch replicas, so each carries in-memory arrangements that must be warmed, whether its definition changed or not. Everything else the branch reads (sources, tables, and objects on other clusters) already exists as a persist shard, so the branch reads it directly, with no arrangement to hydrate.

The in-memory state to warm is an **arrangement**: an incrementally maintained trace of `(key, value, time, diff)` updates. An arrangement is derived, so the branch replicas can always rebuild it by reading the inputs from persist starting at `branch_ts`, and the branch's read holds already retain those inputs. That lazy rebuild is the always-correct floor, together with the shared-cluster shortcut when the branch is homed on production's own cluster (both covered in the parent doc). It is correct but pays a full recompute on first use: read the inputs, sort, and re-run every operator.

**Checkpointing** is an optimization: copy production's already-built arrangements onto the branch replicas so the branch resumes a dataflow instead of rebuilding one. It is a general primitive, the same mechanism giving fast replica bring-up, fast restart, and fast blue/green cutover, with branches as its first consumer.

## What a checkpoint contains: the whole dataflow graph

A checkpoint is **every arrangement in the dataflow**, not just the exported one. Arrangements *are* the operator state, e.g., a join's persistent state is its two input arrangements. Checkpointing only the exported arrangement cannot seed the operators that maintain it, so the branch would recompute the expensive part anyway.

Each arrangement is captured **as of `branch_ts`**, not as a trace with history. Reading a trace as-of `branch_ts` yields the consolidated state at that time, stamped `branch_ts`, which restores into a single batch that lines up exactly with the live batches that follow it.

Note: we collect both the `oks` and the `errs` arrangements.

## Maintained state is arrangements, except under recursion

The "every arrangement" claim rests on arrangements being the whole of an operator's maintained state. This generally holds at a settled frontier.

However, recursion is the exception. `WITH MUTUALLY RECURSIVE` renders its body in an iterative scope, so every arrangement inside the loop is timestamped by a product of the data time and an iteration coordinate, not by the data time alone. `branch_ts` names a point on the data time only, so an as-of-`branch_ts` read does not pick out a single consistent state of a loop arrangement, and restoring the converged output as one batch discards the per-iteration state the loop needs to resume incrementally.

For simplicity we can exclude such dataflows from checkpointing and fall back to lazy rebuild, which is always correct. A loop-aware checkpoint would capture and restore across the product timestamp.

## Capture

Capture runs on **production's workers**, where the arrangements live. It adds a new compute command, sent to those workers to capture a dataflow's arrangements as of a given time, and a matching response reporting when each is captured, so the coordinator can tell a branch its checkpoint is ready. Each arrangement is read through an ordinary cursor as-of `branch_ts`, emitting consolidated updates.

No quiescence is needed for the checkpoint as differential times are logical. Each trace read as-of `branch_ts` is the true state at `branch_ts` independently of every other trace.

The condition per arrangement is `since <= branch_ts`. The upper bound `branch_ts < upper` is automatic: a cursor read as-of `branch_ts` waits for the arrangement to advance past it. `since <= branch_ts` is held by a read hold taken at capture, which prevents the arrangement from compacting past `branch_ts` while it is read.

## The checkpoint format: keyed shards

An arrangement is sorted by its key, and the checkpoint stores it the same way: a persist shard whose key is the arrangement's key. Persist returns that shard's parts already in key order, so restoring becomes a merge of already-sorted pieces and avoids a re-sort.

Building an arrangement from scratch has to sort its input by key, so a checkpoint that already comes back in key order skips it. The obvious alternative, storing the checkpoint the way an ordinary table shard is stored with the whole row as the key, comes back ordered by the row and not by the arrangement's key, so restoring it would have to re-sort into key order, which costs as much as arranging the input from scratch and saves nothing.

## Restore: inject pre-populated traces

Restore reinstates each captured arrangement as an operator's starting state. Every trace in a dataflow is constructed through `Trace::new` and wrapping them in a spine whose `Trace::new` consults a **per-worker restore registry** lets a pre-populated trace stand in wherever the registry holds state under that arrangement's name. Because differential builds even its internal arrangements (a reduce's output trace) through the same spine constructor, this reaches the arrangements differential creates as well as ours. The restored arrangement is handed into the dataflow as an ordinary imported `Arranged`, indistinguishable downstream from a shared one.

Loading onto a different worker count exchanges the sorted runs by key before the merge. A matching worker count and partitioning is a fast path that skips the exchange.

## From the checkpoint to live

Hydration is snapshot-plus-listen: the checkpoint is the snapshot at `branch_ts`, restored as one contiguous batch, and the branch's own input reads supply the diffs strictly above `branch_ts`, so updates at `branch_ts` are not double-applied. The object is not queryable until its arrangements are restored, then it serves as of its frontier and tracks live. This is the same serve versus block policy that we have today.

## Identity and validity: names and fingerprint

Capture and restore must agree on a name for every arrangement. As opposed to a positional name, we could instead derive the name from the **LIR plan** (node path plus the arrangement's key) and register it during render.

A checkpoint is valid only for the same plan. The branch's plan differs from production's in `GlobalId`s by construction. So any checks would need account for a branches object-identity.

## What to checkpoint: the cost model

Checkpointing is not always a win: it skips recomputation but pays to write and read the state instead. So if there is little computation to skip it costs more than it saves. We can therefore decide whether to rebuild or checkpoint per dataflow, and have the same end state.

    rebuild:     read(inputs) + sort + recompute(every operator)
    checkpoint:  write(state)  + read(state) + merge(no sort)

Checkpointing removes the sort and the operator recomputation, and adds a state write and read.

- **A plain index on a table: do not checkpoint.** There are no operators to skip, since the dataflow only arranges its input, and the state is the same size as that input. So the checkpoint adds a full input-sized write and removes almost nothing.
- **An index over a join or aggregate: checkpoint.** The operator work it skips, matching a join or folding an aggregate, can be the expensive part. For an aggregate the state is also much smaller than the input, so the write and read are proportionally cheap too. For a join the state is input-sized.

## Readiness

A branch object reports hydrated through the existing hydration-status signal, and `SHOW BRANCHES` distinguishes `warming` from `ready`. No new readiness mechanism is needed.

## Related work: out-of-core

The out-of-core [work](https://app.notion.com/p/materialize/Out-of-core-implementation-strategy-39813f48d37b80d8a068eb23603d8f03) is related but solves a different problem: capacity, a running arrangement exceeding RAM, rather than bring-up latency, a fresh arrangement starting warm. It pages continuously to ephemeral local storage. Checkpointing captures and restores at discrete moments to durable persist. There could be overlap in the out-of-core work's **chunk** primitive, a serializable arrangement representation shared across wire, batcher, and spine: if it lands, a checkpoint could reference chunks rather than cursor-read and re-encode.

## Alternatives

### What a checkpoint contains

- **Every arrangement, as-of `branch_ts` (chosen).** Restores a resumable dataflow, and the as-of form is the smallest thing that restores correctly.
- **Only the exported arrangement.** Cannot seed the operators that maintain it, so the branch recomputes the expensive part anyway.
- **Trace with history.** Faithful, but the branch never reads below `branch_ts`, so history is dead weight that costs more to write.

### Checkpoint format

- **Keyed persist shard (chosen).** Key order makes restore a merge, not a sort, the only version whose load is cheaper than arranging the input. Generic persist brings batch management and GC.
- **MV persist sink over `SourceData`.** The obvious reuse, but it writes the unkeyed shape and cannot bound itself from above (`up_to` is unimplemented for persist sinks), so it restores no cheaper and needs an external watcher to terminate.

### Capture timing

- **On demand at branch create (chosen).** Captures exactly at `branch_ts`, with nothing maintained when no branch exists, at the cost of a state write on the critical path.
- **Continuous background checkpoints.** The natural fit for replica bring-up and restart, and they take the write off the branch's critical path, but out of scope (but could be useful for hydration etc)
