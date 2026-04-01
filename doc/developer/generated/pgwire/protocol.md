---
source: src/pgwire/src/protocol.rs
revision: 5f785f23fd
---

# pgwire::protocol

Implements the core PostgreSQL wire protocol state machine for a single client connection, covering both the simple and extended query flows, COPY, authentication (cleartext and SCRAM-SHA-256), and session teardown.
Exposes `run(RunParams)` which drives the full session lifecycle after the startup handshake, and `match_handshake(buf)` which detects a pgwire startup message by sniffing the protocol version bytes.
Integrates with `mz_adapter`, `mz_frontegg_auth`, `mz_authenticator`, and `mz_pgcopy`, delegating statement execution to the adapter and routing authentication to the configured authenticator.
