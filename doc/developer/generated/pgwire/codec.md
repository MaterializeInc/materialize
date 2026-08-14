---
source: src/pgwire/src/codec.rs
revision: dd328dbda5
---

# pgwire::codec

Implements frame-level encoding and decoding of the PostgreSQL wire protocol, per the [Frontend/Backend Protocol: Message Formats](https://www.postgresql.org/docs/11/protocol-message-formats.html) spec.
Provides `FramedConn<A>`, which wraps an async I/O stream with buffered send/recv methods for `BackendMessage` and `FrontendMessage`, and a public `Codec` struct that implements `tokio_util::codec::{Encoder, Decoder}`.
`Codec` carries an `encode_state` (the per-column type/format pairs) and a `text_settings` field (`mz_pgrepr::TextEncodeSettings`) that governs session-specific text encoding; both are installed together via `FramedConn::set_encode_state`. When encoding `DataRow` fields, `text_settings` is forwarded to `Value::encode` so the session's `extra_float_digits` and similar parameters are respected.
The count fields that precede repeated groups in Parse and Bind messages (parameter types, format codes, parameter values) are decoded as unsigned 16-bit integers via `Cursor::read_u16`, matching PostgreSQL's behavior; the protocol labels them `Int16` but clients may send up to 65535 entries.
Also contains SASL/SCRAM-SHA-256 message parsers (`decode_sasl_client_first_message`, `decode_sasl_initial_response`, `decode_sasl_response`) implementing RFC 5802 parsing.
