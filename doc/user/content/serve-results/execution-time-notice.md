---
title: "Execution time notice"
description: "Get Materialize's server-side execution time for each statement as a notice."
menu:
  main:
    parent: "serve-results"
    weight: 8
---

With the [`emit_execution_time_notice`](/sql/set/#other-configuration-parameters)
configuration parameter on, Materialize sends a notice with code `MZ012` that
reports how long it took for a successful statement. Every successful statement
gets one, except `SUBSCRIBE` and the PostgreSQL wire protocol cases listed
[below](#postgresql-wire-protocol). The
[SQL Shell](/developer-tools/console/sql-shell/#query-timing) shows it below
each result.

## Format

The notice's `detail` is a string that holds a JSON object:

```
{
    "kind": <"first_row"|"empty_result"|"completed"|"staged"|"committed">,
    "duration_us": <microseconds>
}
```

Its message is meant for people, for example `execution time: 0.412 ms`, with
` (before commit)` for a `staged` write.

New kinds and fields may be added, and the message text may change, so clients
should parse `detail` and ignore kinds and fields they do not recognize.

## What the duration covers

The duration starts when Materialize begins executing the statement, after
parsing and binding it. For a SQL `EXECUTE`, it includes binding the prepared
statement that it runs. On this page, a query is any statement that returns
rows, such as `SELECT`, `SHOW` or `EXPLAIN`, except a write with `RETURNING`.
Where the duration ends depends on `kind`:

- `first_row`: a query's first row is ready. Materialize computes the whole
  result before it sends the first row, so the duration covers computing the
  result but not sending the rows to the client.
- `empty_result`: a query finished without returning rows.
- `completed`: a statement other than a query or a write completed. For
  `COMMIT`, this includes the commit and any DDL that the transaction applies at
  its commit.
- `staged`: a write was added to a transaction that commits later.
- `committed`: the write was durable by the time its result was sent, and the
  duration includes making it durable. For a write with `RETURNING`, the
  duration is the time to its first row, or to the end of an empty result, plus
  the time its result waited for the commit.

When an implicit transaction spans several statements, only the statement that
ends it waits for the commit. That statement's duration includes the commit,
unless it is a query, whose duration ends at its first row.

## PostgreSQL wire protocol

- The notice precedes `CommandComplete` or `PortalSuspended`.
- `COPY ... TO STDOUT` and `COPY ... FROM STDIN` get none.
- A simple query commits its implicit transaction before the last statement's
  `CommandComplete`, as the HTTP and WebSocket APIs do.
- The extended protocol commits a write's implicit transaction at `Sync`, after
  its `CommandComplete`, so the write reports `staged` unless it was already
  durable.
- For a portal executed in steps, or a cursor, the duration is reported once, by
  the step or `FETCH` that starts executing it, if that step sees a row or
  learns that the result is empty. A cursor over `SUBSCRIBE` reports nothing.

## WebSocket API

The notice is a [`Notice`](/serve-results/websocket-api/#notice) message that
precedes the statement's `CommandComplete`. A Simple request's statements share
one implicit transaction, and each query of an Extended request has its own (see
[Transactional semantics](/serve-results/websocket-api/#transactional-semantics)).

## HTTP API

The notice is among the statement's
[`notices`](/serve-results/http-api/#output-format). A Simple request's
statements share one implicit transaction, and each query of an Extended request
has its own (see
[Transactional semantics](/serve-results/http-api/#transactional-semantics)).
