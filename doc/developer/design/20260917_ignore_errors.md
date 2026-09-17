# Ignoring errors in ad-hoc reads

* Associated: [#22430](https://github.com/MaterializeInc/database-issues/issues/6771)
* Revisits: [20240609_error_handling.md](20240609_error_handling.md), which proposed a
  per-relation `IGNORE ERRORS` and rejected it over correctness. This design differs in
  scope and in what it guarantees. See [Alternatives](#alternatives).

## The problem

A source can write a definite error into its persist shard, and nothing ever writes the
compensating retraction. Every later read returns that error instead of data, because a
dataflow is in an error state whenever its errs stream holds at least one element
(`src/compute/src/render.rs:62-65`). Kafka writes such an error when the topic does not
exist (`src/storage/src/source/kafka.rs:535`), when compaction has advanced the low
watermark past the resume upper (`:597`), when the topic is recreated (`:1836`), and when
it is deleted (`:1912`). Transient conditions such as a disconnect do not take this path,
so they never poison a collection.

Storage names the intended escape but provides no way to take it. A definite error passing
through the ingestion pipeline carries the health hint `retracting the errored value may
resume the source` (`src/storage/src/source/source_reader_pipeline.rs:355-361`). No
user-facing mechanism performs that retraction, so the only escape today is to drop the
source and recreate it, paying a full re-ingest.

Value-decode errors in upsert sources already have a narrower escape, `ENVELOPE UPSERT
(VALUE DECODING ERRORS = INLINE)`, which shipped from the 2024 design. Nothing covers the
source-level definite errors above.

## Success criteria

* A user can read the ok rows of a poisoned collection through an ad-hoc `SELECT` or
  `SUBSCRIBE` without recreating anything.
* The escape is opt-in per statement, so no existing query changes behavior.
* No durable object can be defined in terms of the escape.
* A degraded answer is reported as such by the server.
* No new obligation on the optimizer, and no new constraint on how errors are classified
  or worded.

## Out of scope

* **Write-time repair**, meaning retraction of an error from a shard. See
  [Alternatives](#alternatives).
* **Durable objects.** A materialized view over a poisoned source stays broken, and
  rebuilding it still requires a working source.
* **Inline errors in a subscribe stream.** Delivering errors as retractable rows in an
  extra column would give stream consumers a signal they cannot miss, and would let
  subscribe stop poisoning permanently (`src/compute/src/sink/subscribe.rs:177-181`). It
  needs a change to `SubscribeResponse` and interacts with the envelope options.
* **A queryable error relation**, the `ONLY ERRORS` half of the 2024 design. It requires
  threading an oks or errs selector through every use of `GlobalId`.
* **Dead letter queues.** The `REDIRECT ERRORS` proposal from the 2024 design remains
  unimplemented and is unaffected.

## Solution proposal

```sql
SELECT a FROM t ORDER BY a LIMIT 10 WITH (IGNORE ERRORS);
SUBSCRIBE t WITH (IGNORE ERRORS);
```

The option sits at the end of the statement, after `ORDER BY`, `LIMIT` and `OFFSET`
and before `AS OF`, in the position `AS OF` already occupies relative to the query.

### Semantics

The option discards every error the statement would otherwise return, without inspecting
any of them. Whether an error is dropped never depends on its value, its variant, its
message, or where it arose in the plan. One arbitrary discarded error is retained for
diagnostics, and which one is retained never affects the rows returned.

The rows carry no correctness guarantee: when the errs stream is non-empty there are no
semantics for the oks stream (`src/compute/src/render.rs:86`). An error beneath a reduce
yields an aggregate over a subset, so the result is wrong rather than merely incomplete.
The 2024 design restricted its modifier to sources and subsources precisely to keep a
guarantee here, namely that a decode error omits only the record that failed. This design
drops that guarantee in exchange for requiring nothing of the optimizer.

Where an error arises per row, only the rows that raised one are missing, because
operators keep the ok stream as correct as they can. `SELECT 1 / a FROM t` over `a` in
`{1, 2, 0}` returns two rows under the option rather than none. Nothing guarantees this
in general, and it does not survive an aggregation or a join, so the guarantee stated
above is the one to rely on.

Two consequences follow:

* `SELECT 1/0 WITH (IGNORE ERRORS)` returns zero rows and a notice, because the query
  folds to a constant error at plan time and has no ok rows left to return.
* A source sealed by a definite error reports an empty upper
  (`src/storage/src/source/kafka.rs:1705`), which reads as a complete trace at
  `Timestamp::MAX` (`src/adapter/src/coord/timestamp_selection.rs:749-754`). The answer is
  a snapshot frozen at the moment of failure, taken at the current time.

### Reporting

The server emits a notice carrying the retained error and a statement that the answer may
be incomplete or incorrect. Reporting is required, not optional: without it a degraded
answer is indistinguishable from a clean one.

The notice carries no count. Every read path stops at the first error it meets, so
counting would turn a bounded probe into a full walk of the error trace on every
execution. The number would also not mean affected rows, both because compute collapses
error multiplicities per binding during rendering (`src/compute/src/render.rs:1212`) and
because a single source error poisons a whole collection with no row correspondence.

For `SELECT`, the notice follows the rows and precedes `ReadyForQuery`. A peek learns
what it discarded only as its rows stream, which is after the connection loop has already
drained pending notices for that statement (`src/pgwire/src/protocol.rs:888`), so the
drain that carries it is the one immediately before `ReadyForQuery`. Without that drain
the notice arrives with the next statement, or is lost when the session ends after a
single query, which is the interactive case the feature exists for.

For `SUBSCRIBE`, the copy-out loop selects on the notice channel and interleaves
`NoticeResponse` with `CopyData` (`src/pgwire/src/protocol.rs:2908`), so a notice raised
mid-stream is delivered rather than held to the end.

A notice is a weak channel. Most clients discard notices, so a script that adopts the
option once can report frozen data as current indefinitely, and nothing in the catalog or
the source status history records that a reader is working around a poison. This is
accepted for an opt-in, per-statement feature, and it is a reason not to widen the option
to durable objects without a stronger signal.

### Syntax

The option attaches at statement level, in the same position relative to `AS OF` that
`SUBSCRIBE` already uses for its option list (`src/sql-parser/src/parser.rs:9191`).
`SELECT` gains a trailing option list in `parse_select_statement`
(`src/sql-parser/src/parser.rs:7756`).

Attaching at `SelectStatement` rather than at `Query` excludes durable objects
syntactically rather than by a planner check: `CREATE VIEW`, `CREATE MATERIALIZED VIEW`,
`CREATE INDEX` and `INSERT ... SELECT` all parse a `Query`
(`src/sql-parser/src/parser.rs:8881`), so the option has no position in which it could
appear. This mirrors the existing restriction that only the outermost `SELECT` may carry
an `AS OF`.

A `WITH (...)` list avoids the alias ambiguity a trailing keyword introduces. `IGNORE` is
a keyword (`src/sql-lexer/src/keywords.txt:236`) that is not reserved in table-alias
position (`src/sql-lexer/src/keywords.rs:114`), so `SELECT a FROM t IGNORE ERRORS` parses
`t` aliased as `ignore`, and reserving it would break bare-alias queries. The 2024 design
rejected its own trailing-keyword form over the same ambiguity. The list is distinct from
the per-`Select` `OPTIONS (...)` list (`src/sql-parser/src/parser.rs:8160`), which carries
planner hints and would let a subquery carry the option.

### Implementation

Each read path drops errors where it currently surfaces them, and retains the first one it
discards.

| Path | Site | Change |
|---|---|---|
| Index fast path | `ErrorScan::step`, `src/compute/src/compute_state/error_scan.rs:95` | Continue past errors instead of returning, retain the first |
| Peek offload and stash | `src/compute/src/compute_state/peek_offload.rs` | None, offload reuses the index scan |
| Rendered-dataflow peek | `ErrorScan`, as above | None beyond the index fast path |
| Persist fast path | `data.map_err(PeekError::from)?`, `src/compute/src/compute_state.rs:1804` | Skip the row, retain the first error |
| Constant folding | `src/adapter/src/peek_client.rs` and `src/adapter/src/coord/peek.rs` | Return an empty result and a notice. Both peek paths fold constants, and each needs the branch |
| Subscribe sink | `send_batch`, `src/compute/src/sink/subscribe.rs:198` | Drop pending errors, never poison |

The index walk changes from a bounded probe to a full walk, since it no longer stops at
the first error. The persist fast path has no deduplication, and parts containing errors
cannot be pruned by statistics, so the option cannot distinguish a single source error,
which is cheap, from many distinct decode errors, which are not.

The protocol change is asymmetric, and the return direction is the larger half. Outbound
is a boolean: `Peek` (`src/compute-client/src/protocol/command.rs`) and
`SubscribeSinkConnection` (`src/compute-types/src/sinks.rs`) carry the flag to the replica.

Inbound needs a new shape, because both response types encode rows exclusive-or an error
while this feature produces rows together with an error. `PeekResponse`
(`src/compute-client/src/protocol/response.rs`) separates `Rows` and `Stashed` from
`Error`, so the two successful variants gain a field for the retained error, and
`merge_peek_responses` (`src/compute-client/src/service.rs`) keeps any one of them when
combining workers that dropped errors with workers that did not. `SubscribeBatch.updates`
is a `Result`, whose `Err` arm is the poison channel this option removes, so it has no
slot to report a dropped error through and gains a separate field.

Returning the error text rather than a bare "errors were dropped" boolean is what makes
the notice actionable, since a deleted topic and a single failed decode call for different
responses. That is worth the touch points the shape change costs. None of these structures
is durable.

The feature is gated by a flag in `feature_flags!`
(`src/sql/src/session/vars/definitions.rs:1802`) with `default: false` and
`enable_for_item_parsing: false`, the latter safe because the syntax can never appear in a
catalog item. The CI default is on, set in `get_default_system_parameters`
(`misc/python/materialize/mzcompose/__init__.py:614`).

## Minimal viable prototype

The syntax and planner gate can land first, with the option accepted and ignored. The
first execution path to wire up is the persist fast path, because `SELECT * FROM src WITH
(IGNORE ERRORS)` against a source poisoned by a deleted topic is the motivating query and
takes that path. `test/testdrive/kafka-recreate-topic.td` already builds the poisoned
states this feature targets and asserts the current errors (`:33`, `:85`), so it provides
the fixtures without new setup.

## Alternatives

### Write-time retraction

Retracting the error from the shard is not proposed here. It is the stronger fix for a
source an operator intends to keep: it heals dependent indexes and materialized views with
no query changes, needs no per-statement opt-in, is a single auditable action rather than
a cost paid on every read, and matches the repair the health hint already describes. This
design does not preclude it.

### Per-relation scope

The 2024 design placed the modifier on a table factor, as `FROM IGNORE ERRORS x`, and a
prototype implemented it with an `ignore_errors` field on `MirRelationExpr::Get`. That
form has the better blast radius, because an error raised by the statement's own
expressions still surfaces. It requires the optimizer to treat the annotated `Get` as a
barrier, so that a fused expression cannot widen or narrow the set of ignored errors and
common subexpression elimination cannot unify annotated and unannotated reads of the same
collection. That optimizer obligation is not justified by the narrower blast radius.

### Filtering by error variant

Discarding only the variants storage produces, namely `SourceError`, `DecodeError` and
`EnvelopeError` (`src/storage-types/src/errors.rs:489`), while always surfacing
`EvalError`, would recover most of the per-relation benefit with no optimizer work. The
classification holds today: no non-test code under `src/compute`, `src/compute-types` or
`src/compute-client` constructs a storage-origin variant, and storage wraps evaluation
failures rather than passing them through, as Postgres does by mapping a cast failure into
`DefiniteError::CastError` (`src/storage/src/source/postgres.rs:518`) and then into a
`SourceError` (`:406`).

It was rejected because it makes the variant assignment a compatibility surface. Query
results would depend on how an error is classified, so moving an error between variants
would silently change which rows a statement returns. Materialize reserves the right to
reword, recode and reclassify errors freely, and that freedom is worth more than the
sharper blast radius. The approximation also leaks invisibly, since an `EvalError`
persisted by a materialized view is data-origin in spirit but query-origin by variant.

## Open questions

* Should a subscribe that discards errors during its snapshot report before the first
  batch, so that a consumer using `SNAPSHOT = false` still learns the collection is
  poisoned?
* Does the notice need a distinct SQLSTATE, given that most clients discard notices?
* Is `IGNORE ERRORS` the right name, given that it names the rejected per-relation design
  in the 2024 document and the semantics here are broader?
