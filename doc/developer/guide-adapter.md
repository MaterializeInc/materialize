# Adapter Guide

General guidance for working on the adapter layer (`src/adapter/`), the
coordinator, pgwire frontend, and related crates. This is a living document -
add to it as you discover invariants, pitfalls, or non-obvious design
decisions.

## Architecture & Key Concepts

<!-- TODO: fill in -->

## Correctness Invariants

### Timestamp selection must respect real-time bounds

For strict serializability, the timestamp assigned to a query must fall within
the query's real-time interval --- between the moment it arrives and the moment
its response is sent. The design doc
(`doc/developer/design/20220516_transactional_consistency.md`) states this as:

> Each timestamp is assigned to an event at some real-time moment within the
> event's bounds (its start and stop).

This means:

- **Never select a timestamp from before the query arrived.** Doing so would
  allow a query to observe a snapshot that predates its own start, violating
  the real-time ordering constraint of strict serializability.

- **Selecting a later timestamp is always safe**, as long as it is still within
  the query's real-time bounds (i.e. the query hasn't returned yet). Pushing a
  timestamp forward only makes the query appear to have executed later, which
  is fine --- the query was indeed still in-flight at that moment.

#### Why the batching oracle is correct

`BatchingTimestampOracle` collects multiple `read_ts` requests that arrive
concurrently and serves them all with a single call to the backing oracle.
Because the backing oracle is called *during* all of their real-time intervals
(after all requests arrived, before any have returned), the returned timestamp
is within bounds for every request. Batching can only push timestamps later,
never earlier. See the comment on the `BatchingTimestampOracle` struct.

#### Why caching an oracle result is not correct

It is tempting to cache or snapshot the most recent `read_ts` result (e.g. in a
shared atomic) and reuse it for subsequent queries without another oracle call.
This is wrong: the cached value was determined at a real-time moment *before*
the new query arrived, so assigning it to the new query places the query's
timestamp outside its real-time bounds. A query arriving after the cached value
was last updated would receive a stale timestamp --- one that predates its own
start --- violating strict serializability.

**Common incorrect argument to watch for:** "Using a slightly older timestamp
just reads an earlier consistent snapshot, which is valid for linearization."
This confuses serializability (some valid ordering exists) with *strict*
serializability (the ordering must respect real-time). Under strict
serializability, the linearization point must fall within the operation's
real-time interval. A timestamp from before the query arrived places the
linearization point before the query started --- outside its real-time bounds ---
regardless of whether the snapshot itself is internally consistent. The
consistency of the snapshot is not the issue; the issue is *when* it was
determined relative to the query's arrival.

This also interacts with the distributed-system invariant below: the cached
value is local-only state, so it cannot reflect writes applied by other
`environmentd` nodes to the shared backing oracle.

### No local-only assumptions in a distributed system

Materialize is designed to run as a distributed system: multiple `environmentd`
instances may share the same backing store (for example CRDB) concurrently. Any
optimization that relies on local-only state --- such as tracking writes with
an in-process counter and assuming no other writer exists --- is incorrect
unless the backing store is also consulted or the invariant is otherwise
guaranteed system-wide. Always ask: "does this still work if another node is
running the same code against the same backing store?"

### Checklist for timestamp-related changes

Before modifying timestamp selection or oracle interaction, verify:

1. **Real-time bounds**: Is the timestamp determined by an oracle call (or
   equivalent) that happens *during* the query's lifetime (after arrival, before
   response)? If the value could have been determined before the query arrived,
   it violates strict serializability.

2. **Distributed correctness**: Does this work when multiple `environmentd`
   nodes share the same backing oracle? Any in-process cache, atomic, or counter
   that is not synchronized through the backing store is suspect.

3. **Monotonicity**: Can a caller ever observe a timestamp go backwards? Even
   with concurrent batches or out-of-order completions?

4. **Write visibility**: After `apply_write(t)` returns, will all subsequent
   `read_ts` calls (including the fast path, if any) return `>= t`? This must
   hold across nodes, not just within the local process.

## Rejected Optimizations

This section records specific optimizations that have been attempted and found
incorrect. If you find yourself re-proposing one of these, treat it as a strong
signal that the approach is wrong.

### Shared-atomic `read_ts` cache (`peek_read_ts_fast`)

**What:** Add an `Arc<AtomicU64>` to `BatchingTimestampOracle` holding the most
recently observed `read_ts`. Return it from a new `peek_read_ts_fast()` trait
method, bypassing the ticket/watch/CRDB round-trip for 99%+ of reads.

**Why it's wrong:**
- Violates real-time bounds (§ "Why caching an oracle result is not correct"):
  the atomic holds a value from a *previous* oracle call. A query arriving after
  that call completed receives a timestamp from before its own start.
- Violates distributed correctness (§ "No local-only assumptions"): another
  `environmentd` node can `apply_write` to CRDB, advancing the global read
  timestamp. The local atomic has no way to learn about this, so it returns a
  stale timestamp that predates a write already visible to other nodes.
- The safety argument ("reading an earlier snapshot is valid for linearization")
  confuses serializability with strict serializability. See the detailed rebuttal
  in § "Why caching an oracle result is not correct."

**Performance context:** This optimization delivered ~5x throughput improvement
at low concurrency by eliminating oracle round-trips. The performance need is
real, but the solution must maintain strict serializability. Correct alternatives
might include: reducing oracle round-trip latency, colocating the oracle,
using the batching oracle's existing mechanism to serve more callers per batch,
or relaxing the isolation level for queries that opt in.
