# Request-context tracing diagnostic

This is a benchmark-only experiment, not a production tracing configuration.
It compares the cumulative QPS baseline with two approaches to reducing tracing
overhead. SQL throughput must be measured separately from callback microbenchmarks.

Set `MZ_QPS_TRACING_MODE` before process startup:

| Mode | Internal execution spans | stderr logs |
| --- | --- | --- |
| `baseline` (default) | Original behavior | Original behavior |
| `filter` | Original behavior | Original format, static-filter lifecycle gate |
| `request` | Globally disabled | Event fields plus explicit request identity |
| `detailed` | Enabled through an OTEL exporter | Event fields plus explicit request identity |

The lifecycle gate delegates filter decisions to the original EnvFilter. After
any span-dependent filter has been installed, it keeps forwarding lifecycle
callbacks permanently so spans surviving a reload remain tracked correctly.

## Request identity and ownership

Identity is the existing session UUID and a session-local request sequence.
Zero is reserved for session lifecycle work. Pgwire assigns an identity to each
frontend message. A simple-query message containing multiple statements shares
one identity. Extended-protocol messages have separate identities. COPY payloads
processed within a query keep the query identity. HTTP, WebSocket and MCP SQL
execution share the same session sequence allocator.

Identity does not depend on statement logging. It is held explicitly across
command/response queues, staged execution, deferred work, pending peeks, and
retirement. Execution context retains its identity when the statement-logging
guard moves to another owner. Mixed-request batches restore each response's own
identity, not the identity of the first request in the batch.

The TLS slot is entered only during synchronous execution, each future poll,
and destruction. The eager future wrapper scopes destruction even if canceled
before its first poll. It does not leave TLS entered while suspended.

Task inheritance is opt-in. `spawn_in_request` and
`spawn_blocking_in_request` are for request-owned child work. Ordinary task
spawning remains unchanged, so shared controller or persistence workers cannot
retain the identity of the request that happened to create them. Queued blocking
closures retain identity during destruction if canceled before execution.

`InProcessContext` is not serializable. Baseline/filter modes retain the original
text carrier for comparison, detailed mode carries a direct span handle, and
request mode carries no span. Cross-process OpenTelemetry propagation is
unchanged. Not every internal text carrier has been replaced by this diagnostic.

## Observability restrictions

Request-context modes require JSON logs and reject span/field-dependent stderr
filters at startup and reload. Target/level filters remain supported. Event
fields, warning/error levels, timestamps, and bridged `log` metadata are retained.
Span ancestry, span-only fields, span names, and span timing are absent from
request-only stderr logs. Detailed mode exports execution spans separately.

Request mode requires no exporter. Detailed mode requires an exporter. Both
request-context modes reject Sentry, Tokio console, and configured span capture
instead of silently disabling those consumers. Dedicated local dispatchers used
by EXPLAIN are not replaced by the global diagnostic subscriber.

Request attribution is an in-process SQL diagnostic, not an end-to-end identity
for compute, storage, authentication middleware, or arbitrary background work.
Cross-process logs retain their own fields and existing wire tracing behavior.
HTTP authentication before SQL dispatch is outside the request scope. Session
lifecycle work outside a received pgwire message uses request zero.

## Validation and comparison

Core tests exercise real configured JSON output, concurrent request isolation,
async migration, cancellation/drop, shared-task non-inheritance, filter reloads,
and actual SDK-exported remote/local trace parentage. Adapter tests verify
identity ownership independently of statement logging, alongside response
barriers and strict-read completion. These are not a substitute for SQL-level
correctness and performance tests.

Before drawing a performance conclusion, compare the original cumulative image
with diagnostic `baseline` to quantify instrumentation/control overhead, then
compare `filter` and `request` under identical SQL load and resource settings.
Keep statement logging disabled and retain normal warning/error logging. Do not
interpret synthetic callback throughput as SQL QPS. Record observability losses,
latency distributions, throughput, CPU/thread profiles and experimental settings.
Restore the original stack settings after the experiment. Nothing here is ready
to merge or enable in production without a separate design decision.
