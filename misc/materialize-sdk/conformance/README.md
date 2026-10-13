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
