We have been chatting with Kyle Kingsbury about the design of a transaction
protocol in a world where.

# Calvin-inspired

## Background

Calvin is a distributed database transaction protocol that relies on
deterministic execution to avoid coordination as much as possible. In the
prototypical Calvin implementation global coordination occurs at most once per
epoch (e.g. every 10 ms), where each client-request-receiving service (known as
the sequencer) shares the order of execution it determined with other all other
the services that depend on that order (known as scheulders). Because the
execution is deterministic, ensuring each scheduler receives the same set of
inputs means they will eventually converge to the same final states.

## Materialize design

Materialize propagates all write operations to a durable WAL. (Note that unlike
Calvin, we only propagate writes to the WAL and not the entire transactional
input; we are not running full stack of Materialize services vertically.)

Storage nodes are then responsible for taking updates to the WAL and attempting
to apply them to their local state--note that this, in essence, pushes down
transaction logic to the storage layer. Storage nodes then need some means to
communicate with one another to either all accept or all reject the update. In
the case of rejection, the storage node needs some means to discard the
provisional writes.

Note that in this design, some external source is responsible for determining
the order in which writes get appended to the WAL, i.e. the WAL itself is truly
just a log that offers to real invariants itself.

This design is inspired by Calvin in as much as the storage nodes get to behave
akin t

### Example architecture

```
  client
  |    ^
 txnA  |
  |   res
  v    |                 storageA    storageB
[ADAPTER]-wA->[WAL]<-get---?
^              ^-----------get-----------?
|
|              if a?.is_ok() && b?.is_ok() {
|                  return OK to client
|              } else {
|                  rollback(A);
|                  rollback(B);
|                  return ABORT to client
|______________}
```

### Questions

- How do writes invalidate outstanding reads in e.g. `UPDATE`? I realize this
  design says that sequencing has to happen elsewhere, but just curious where we
  envision that potentially being.
- How do e.g. column constraints get handled by the txn layer? Seems like this
  moves "business logic" into storage, which I don't think will make the storage
  people happy.
- How do we rollback if we're already in the storage layer?

# CAS

## Background

This is the design Kyle has been moving forward with fleshing out, which relies
on a the WAL having a timestamp only ever written to atomically.

## Materialize design

Each writable TVC will have a WAL that acts as a buffer for writes before being
committed to storage. Each WAL will have a timestamp indicating the last time at
which an operation occurred; we'l call this the WAL ts for simplicity.

When users want to write to a TVC, they'll use `append(updates, from ts, new
frontier)`. This implementation of `append` will succeed iff it's able to update
the WAL's ts using an atomic CAS, i.e. the WAL's current ts is `from ts`.

When reading, the ADAPTER consults the WAL and gets its current timestamp
(vaguely representing upper--); knowledge of this time provides access to an
optimistic lock for writes to the table. This fits very narrowly in MZ's usecase
of read-then-write for UPDATE and DELETE; generalized RW transactions would take
more thought.

The session performs all reads at the WAL's timestamp.

When MZ goes to write, if the write fails, it indicates some other write
occurred to the TVC after the read. Because we're only offering table-level
granularity in this design, that means the reads at the last-seen-WAL ts are
invalid, and the transaction cannot commit.

For writes that don't have a WAL ts (i.e. they never read), they can just write
at the current WAL ts++ because they could be scheduled as late as possible
(i.e. the write frontier).

Storage nodes can then follow along and apply updates from the WAL at their
leisure.

### Questions

- Can you check my math? We should be able to get the WAL ts "lock" lazily
  whenever we perform a read. And writes that never read can just be scheduled
  at the latest ts.
- (_mansplaining voice_) This is more of a comment than a question. If we
  correlated each read "lock" (more like a latch) with a set of predicates, we
  could get row-level concurrency in the WAL; when you try to propagate a write
  to the WAL, you could check the predicates of all reads at WAL timestamps
  greater than since to ensure you haven't invalidated anyone's reads. Don't
  think this is necessary, but is a means by which we could improve our
  contention handling.
