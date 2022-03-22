# Multi-table Transactions

## Summary

Upcoming persist changes will remove the persist API allowing writing to multiple collections in an atomic operation.
In order to allow the persist API to be written, we must do one of:

1. Disallow multi-table transactions.
2. Use an existing (or upcoming) materialize API to record transactions.
3. Use a third-party product to record transactions.

We need a log to record transactions that can write the entire transaction atomically, which will allow the rest of the system to know that the transaction committed.
The controller writes transactions to the WAL, then if committed, each collection can apply their portion of the writes asynchronously.
There are two general methods to determine "if committed" are:
- Writing to the log is controlled by a consensus operation such that any transaction successfully written to the log has committed.
  Here the log needs a CAS operation during write.
  [diagrams](https://materializeinc.slack.com/archives/C035TU9QF5W/p1647452686144389),
  [description](https://materializeinc.slack.com/archives/C035TU9QF5W/p1647362440541409)

- Readers of the log must use data contained in a log entry (and possibly previous entries) to determine whether a log entry committed.
  Here the log does not need a CAS operation, but does need a total order.
  [diagrams](https://materializeinc.slack.com/archives/C035TU9QF5W/p1647454329216509)

Below are proposed implementation options for 2 and 3.

## Existing or Upcoming Materialize APIs

The APIs here all share the benefit that they do not require us to manage or interface with more software, at the expense of possibly increasing difficulty for our own work.

2a. Use our current SQLite catalog on EBS.
  EBS mounts are exclusive, but come at occasional availability and durability costs.
  However, this is the least amount of work today.
  [description](https://materializeinc.slack.com/archives/C035TU9QF5W/p1647522459313959)

2b. Use upcoming persist plan as the WAL.
  It is unclear to me if this API will be with-CAS or without-CAS, but in either case it should suffice.
  [description](https://materializeinc.slack.com/archives/C035TU9QF5W/p1647866145149579)

## Third-Party Products

3a. Etcd, a WAL with CAS.

3b. Kafka, a WAL without CAS.

3c. RDS, a WAL with CAS.
  This has an additional benefit that we may end up using this anyway.
  It requires having an epoch on controller requests.
  [description](https://materializeinc.slack.com/archives/C035TU9QF5W/p1647523498432819)
