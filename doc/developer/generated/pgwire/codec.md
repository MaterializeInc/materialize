---
source: src/pgwire/src/codec.rs
revision: 59358e137b
---

# pgwire::codec

Implements frame-level encoding and decoding of the PostgreSQL wire protocol, per the [Frontend/Backend Protocol: Message Formats](https://www.postgresql.org/docs/11/protocol-message-formats.html) spec.
Provides `FramedConn<A>`, which wraps an async I/O stream with buffered send/recv methods for `BackendMessage` and `FrontendMessage`, and a `Codec` struct that implements `tokio_util::codec::{Encoder, Decoder}`.
Also contains SASL/SCRAM-SHA-256 message parsers (`decode_sasl_client_first_message`, `decode_sasl_initial_response`, `decode_sasl_response`) implementing RFC 5802 parsing.
