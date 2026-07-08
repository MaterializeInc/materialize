---
source: src/pgwire/src/protocol.rs
revision: 339168a637
---

# pgwire::protocol

Implements the core PostgreSQL wire protocol state machine for a single client connection, covering both the simple and extended query flows, COPY, authentication (cleartext and SCRAM-SHA-256), and session teardown.
Exposes `run(RunParams)` which drives the full session lifecycle after the startup handshake, and `match_handshake(buf)` which detects a pgwire startup message by sniffing the protocol version bytes.
Integrates with `mz_adapter`, `mz_frontegg_auth`, `mz_authenticator`, and `mz_pgcopy`, delegating statement execution to the adapter and routing authentication to the configured authenticator.
The private `FetchResult` enum used in streaming row fetch carries an `ErrorResponse` (not a bare `String`) in its `Error` variant, allowing subscribe errors such as dependency drops to propagate their specific SQLSTATE codes (e.g. `42704` for undefined object) rather than always using `XX000`.

During startup, parameters that are successfully applied via `set()` are collected in `applied_params` and subsequently registered as session defaults via `set_default()`, after role defaults have been applied. This means `RESET` and `DISCARD ALL` restore to the startup parameter values rather than server defaults, matching PostgreSQL behavior and allowing connection poolers (e.g., pgbouncer) to rely on `DISCARD ALL` for session reset.

In the ready state, stray `CopyData`, `CopyDone`, and `CopyFail` messages are accepted and ignored (returning `State::Ready`) rather than triggering drain. Clients stream COPY data optimistically, so these messages can arrive after a COPY statement fails before COPY mode is entered; draining would discard unrelated messages until the next Sync.

When decoding bind parameters, NUL characters in a decoded string value produce an error with `SqlState::CHARACTER_NOT_IN_REPERTOIRE`, matching PostgreSQL's SQLSTATE for this condition.
