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
    "kind": <"first_row"|"empty_result"|"completed"|"staged">,
    "duration_us": <microseconds>
}
```

Its message is meant for people, for example `execution time: 0.412 ms`, with
` (staged)` for `staged`.

New kinds and fields may be added, and the message text may change, so clients
should parse `detail` and ignore kinds and fields they do not recognize.

## What the duration covers

The duration starts when Materialize begins executing the statement, after
parsing and binding it. For a SQL `EXECUTE`, it includes binding the prepared
statement that it runs. It ends when the statement's result is ready, and
`kind` says when that was:

- `first_row`: the first row of a statement that returns rows, such as `SELECT`,
  `SHOW`, `EXPLAIN` or a write with `RETURNING`, is ready. Materialize computes
  the whole result before it sends the first row, so the duration covers
  computing the result but not sending the rows to the client.
- `empty_result`: a statement that returns rows finished without any.
- `completed`: a statement that returns no rows completed.
- `staged`: the statement's result is ready, but its transaction still holds
  work for a later commit, such as a write or DDL that the transaction applies
  when it commits. The duration does not include that commit. A statement that
  follows such work in the same transaction reports `staged` too.

The duration never includes the commit of an implicit transaction. In an
explicit transaction, `COMMIT` reports the commit.

## PostgreSQL wire protocol

- The notice precedes `CommandComplete` or `PortalSuspended`.
- `COPY ... TO STDOUT` and `COPY ... FROM STDIN` get none.
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
