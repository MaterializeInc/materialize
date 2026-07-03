---
source: src/pgwire/src/protocol.rs
revision: 942739f7f7
---

# pgwire::protocol

Implements the core PostgreSQL wire protocol state machine for a single client connection, covering both the simple and extended query flows, COPY, authentication (cleartext and SCRAM-SHA-256), and session teardown.
Exposes `run(RunParams)` which drives the full session lifecycle after the startup handshake, and `match_handshake(buf)` which detects a pgwire startup message by sniffing the protocol version bytes.
Integrates with `mz_adapter`, `mz_frontegg_auth`, `mz_authenticator`, and `mz_pgcopy`, delegating statement execution to the adapter and routing authentication to the configured authenticator.
The private `FetchResult` enum used in streaming row fetch carries an `ErrorResponse` (not a bare `String`) in its `Error` variant, allowing subscribe errors such as dependency drops to propagate their specific SQLSTATE codes (e.g. `42704` for undefined object) rather than always using `XX000`.
