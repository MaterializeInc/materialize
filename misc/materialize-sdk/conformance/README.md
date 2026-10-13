# Conformance vectors

These files define how every Materialize SDK package behaves. Each package runs
them through its binding to the protocol core, and the core runs them in
`core/tests/conformance.rs`. A change in behavior changes a vector, so the
change is reviewed as data and every package sees it.

The expected outputs are written by hand from the design, not recorded from an
implementation.

Each file holds the cases for one function:

```json
{
  "function": "decode",
  "cases": [
    { "name": "...", "input": { ... }, "expect": ... }
  ]
}
```

A runner reads each `input` as a typed value and refuses unknown keys, so a
typo cannot change what a case tests. It turns the result into JSON and
compares it with `expect` as JSON values.

`input` and `expect` use the protocol core's JSON form:

- Every 64-bit integer, such as a timestamp, frontier, diff, epoch, row count,
  or byte count, is a decimal string with no sign for positive values and no
  leading zeros. A frontier can be 2^64, one past the largest timestamp. A
  runner converts its language's integers, such as a Python `int` or a
  JavaScript `BigInt`, to and from that string. Smaller integers, such as a
  format version or a type OID, are JSON numbers.
- A row is an array of cells. A cell is the column's text format as a string,
  or `null` for SQL `NULL`.
- An error is `{"error": kind}`. Each function below lists its kinds. Messages
  are not part of the contract.

### `decode.json`

Input: `envelope`, `columns`, `rows`. Build a decoder from the envelope and
columns. If that fails, the output is `{"error": "protocol"}`. Otherwise decode
each row on its own, and the output is `{"messages": [...]}` with one entry per
row: the decoded message, or `{"error": "protocol"}`.

### `batch.json`

Input: `start`, optional `up_to`, `max_pending_bytes`, and
`max_timestamp_bytes`, and `steps`. Build a batcher, then run each step: a
message is pushed, and `"finish"` reports the end of the stream. The output is
`{"outputs": [...], "finished": bool}`, with one entry per step: the list of
events it released, or `{"error": kind}` with kind `protocol` or
`timestamp_too_large`. After an error the batcher fails every later step.

### `latest_by_key.json`

Input: `updates`, in timestamp order at timestamps of one or more. Release them
as one batch and return the latest change per key, as a list of updates.

### `subscribe_sql.json`

Input: `subscribe`, `start`. The output is `{"sql": ...}`, or
`{"error": "usage"}`.

### `fetch_sql.json`

Input: `cursor`, `max_rows`, optional `timeout_ms`. The output is
`{"sql": ...}`, or `{"error": "usage"}`.

### `subscription.json`

Input: `subscribe`, `start`, optional `max_pending_bytes` and
`max_timestamp_bytes`, and `steps`. Create the subscription, or output
`{"error": "usage"}`. The output is a list that starts with `{"declare": ...}`,
followed by one entry per step:

- `{"fetch": {"max_rows", "timeout_ms"}}` asks for the next `FETCH` and yields
  `{"sql", "timeout_ms"}`, or `{"error": "usage"}`.
- `{"push": {"columns", "rows"}}` passes the latest `FETCH` and its result to
  `push` and yields `{"events", "finished"}`, or `{"error": kind}` with kind
  `protocol` or `timestamp_too_large`.
- `{"classify": {"sqlstate", "message"}}` classifies an error at this point of
  the stream, which decides its phase, and yields the classification.

### `classify.json`

Input: `sqlstate` (or `null`), `message`, `phase`. The output is the
classification.

### `resolve_object_check.json`

Input: `otherwise`, `change`. The output is the outcome.

### `resolve_index_check.json`

Input: `indexes`, the names of the indexes on the subscribing cluster that
serve the object. The output is `{"kind": ...}`.

### `compare_identity.json`

Input: `stored`, `current` (or `null`). The output is the change.

### `checkpoint.json`

Input: `stored`, a checkpoint in its JSON form, and `operation`. Decode the
stored checkpoint, or output `{"error": "checkpoint"}`, then apply the
operation:

- `"encode"`, `"take_over"`, `{"restart": {"subscriptions", "as_of"}}`,
  `{"with_subscriptions": {"subscriptions"}}`, and `{"committed": {"upper"}}`
  yield the new checkpoint as `{"checkpoint": ..., "text": ...}`, where `text`
  is its exact encoding, or `{"error": kind}` with kind `checkpoint`, or
  `checkpoint_too_large` when the text would be longer than `MAX_BYTES`.
- `{"check_commit": {"expected", "upper"}}` yields `{"commit": ...}` or a
  conflict, and `{"check_write": {"expected"}}` yields `{"write": "ok"}` or a
  conflict. `expected` is the checkpoint the write was conditional on, in its
  JSON form, and decoding it fails as `stored` does.
- `{"encode_within": {"limit"}}` yields the checkpoint as `"encode"` does, with
  `limit` in place of `MAX_BYTES` when it is lower.
- `{"settle": {"expected", "new"}}` settles a write from `expected` to `new`
  that failed or whose result is unknown, with `stored` as the checkpoint read
  back after it. It yields `{"settlement": ...}`, with a `conflict` for
  `"conflict"`.

### `catalog.json`

Input: one operation:

- `{"lookup_sql": {"name"}}`, `{"indexes_sql": {"object_id", "cluster"}}`, and
  `{"read_frontier_sql": {"object_id"}}` yield `{"sql": ...}`.
- `{"parse_lookup": {"columns", "rows"}}` yields `{"object": ...}`, with
  `null` when the name resolves to nothing.
- `{"parse_indexes": {"columns", "rows"}}` yields `{"indexes": [...]}`, and
  `{"parse_read_frontier": {"columns", "rows"}}` yields
  `{"read_frontier": ...}`.
- `{"resume_margin": {"frontier", "read_frontier"}}` yields
  `{"margin_ms": ...}`, with `null` when history is lost.
- `{"fingerprint": {"object", "envelope"}}` yields `{"fingerprint": ...}` for
  an object a durable subscription can read, or `{"error": "usage"}`.

A query's result that the query cannot return yields `{"error": "protocol"}`.
