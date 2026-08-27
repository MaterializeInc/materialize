---
source: src/pgwire/src/protocol.rs
revision: b0d4c751f6
---

# pgwire::protocol

Implements the core PostgreSQL wire protocol state machine for a single client connection, covering both the simple and extended query flows, COPY, authentication (cleartext and SCRAM-SHA-256), and session teardown.
Exposes `run(RunParams)` which drives the full session lifecycle after the startup handshake, and `match_handshake(buf)` which detects a pgwire startup message by sniffing the protocol version bytes.
Integrates with `mz_adapter`, `mz_frontegg_auth`, `mz_authenticator`, and `mz_pgcopy`, delegating statement execution to the adapter and routing authentication to the configured authenticator.
The private `FetchResult` enum used in streaming row fetch carries an `ErrorResponse` (not a bare `String`) in its `Error` variant, allowing subscribe errors such as dependency drops to propagate their specific SQLSTATE codes (e.g. `42704` for undefined object) rather than always using `XX000`.

During startup, parameters that are successfully applied via `set()` are collected in `applied_params` and subsequently registered as session defaults via `set_default()`, after role defaults have been applied. This means `RESET` and `DISCARD ALL` restore to the startup parameter values rather than server defaults, matching PostgreSQL behavior and allowing connection poolers (e.g., pgbouncer) to rely on `DISCARD ALL` for session reset.

In the ready state, stray `CopyData`, `CopyDone`, and `CopyFail` messages are accepted and ignored (returning `State::Ready`) rather than triggering drain. Clients stream COPY data optimistically, so these messages can arrive after a COPY statement fails before COPY mode is entered; draining would discard unrelated messages until the next Sync.

`COPY TO STDOUT` and query result encoding both read the session's `text_encode_settings()` (which packages `extra_float_digits` and similar session variables into a `TextEncodeSettings`) and forward it to the codec and to `encode_copy_format`. This ensures session-configured text encoding (e.g. float digit count) is honored for session-bound output, while dataflow-layer encoding (e.g. `COPY TO <external destination>`) always uses `TextEncodeSettings::STABLE`.

When decoding bind parameters, NUL characters in a decoded string value produce an error with `SqlState::CHARACTER_NOT_IN_REPERTOIRE`, matching PostgreSQL's SQLSTATE for this condition.

In the extended query protocol, after each `Execute` message is processed the handler checks whether the current implicit transaction `may_span_pipeline`. Transactions that can span the pipeline stay open, allowing the whole pipeline to commit or roll back as a unit. Transactions that cannot span the pipeline are marked for commit (`txn_needs_commit = true`) so that `ensure_transaction` commits them before the next statement begins; this keeps single-statement optimizations available. The `Sync` message commits any open implicit transaction. The flag `extended_protocol_implicit_transaction_enabled` gates this pipeline-spanning behavior; when the flag is off, every implicit transaction is marked for commit after each Execute.

When an implicit transaction ends (via `end_transaction`), any session parameters that changed during the transaction are announced to the client as `ParameterStatus` messages, restricted to the notify set established at startup. This mirrors the behavior of explicit `COMMIT`/`ROLLBACK`. Without this announcement, a `SET LOCAL` issued outside an explicit transaction would send its new value at SET time but never announce the revert when the implicit transaction closes, leaving clients that cache parameters with a stale value.

When `enable_statement_arrival_logging` is on, `maybe_log_message_arrival` logs each arriving frontend message at info level before it is processed, so a message that crashes the process still appears in the log. SQL text is redacted with the same policy as the statement log. Bind parameter values are data that redaction cannot reach, so only their count is logged. Authentication payloads are never logged. COPY data payloads are logged as their byte length only.
