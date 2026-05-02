---
source: src/pgwire-common/src/codec.rs
revision: 5680493e7d
---

# mz-pgwire-common::codec

Implements encoding and decoding of pgwire messages per the PostgreSQL Frontend/Backend Protocol specification.
`Pgbuf` extends `BufMut` with pgwire-specific write helpers: `put_string` (null-terminated), `put_length_i16`, `put_format_i8`, and `put_format_i16`.
`decode_startup` reads the initial startup frame from an async stream and returns a `FrontendStartupMessage` (or `None` on clean close), dispatching on the version field to `Startup`, `CancelRequest`, `SslRequest`, or `GssEncRequest`.
`FrontendStartupMessage::encode` and `FrontendMessage::encode` write messages into a `BytesMut`, back-patching the length field after writing the body.
`Cursor` provides error-returning byte-level reads over a `&[u8]` slice: `peek_byte`, `read_byte`, `read_cstr` (null-terminated UTF-8 string), `read_i16`, `read_i32`, `read_u32`, `read_format`.
`parse_frame_len` validates and converts the 4-byte big-endian frame length prefix.
`MAX_REQUEST_SIZE` caps incoming request size at 2 MiB; `CodecError::StringNoTerminator` signals a missing null terminator.
