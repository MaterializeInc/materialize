---
source: src/pgwire-common/src/codec.rs
revision: 2daa609ac4
---

# mz-pgwire-common::codec

Implements encoding and decoding of pgwire messages per the PostgreSQL Frontend/Backend Protocol specification.
`Pgbuf` extends `BufMut` with pgwire-specific write helpers: `put_string` (null-terminated), `put_length_i16`, `put_length_u16`, `put_format_i8`, and `put_format_i16`. `put_length_u16` writes a count field as an unsigned 16-bit integer (up to 65535); the protocol labels these fields `Int16` but PostgreSQL decodes them as unsigned.
`decode_startup` reads the initial startup frame from an async stream and returns a `FrontendStartupMessage` (or `None` on clean close), dispatching on the version field to `Startup`, `CancelRequest`, `SslRequest`, or `GssEncRequest`. The `max_frame_len` argument bounds the frame the client may declare before the body buffer is sized; pass `MAX_STARTUP_FRAME_SIZE` for direct clients or `MAX_FORWARDED_STARTUP_FRAME_SIZE` when a balancer may have appended parameters.
`FrontendStartupMessage::encode` and `FrontendMessage::encode` write messages into a `BytesMut`, back-patching the length field after writing the body.
`Cursor` provides error-returning byte-level reads over a `&[u8]` slice: `peek_byte`, `read_byte`, `read_cstr` (null-terminated UTF-8 string), `read_u16`, `read_i32`, `read_u32`, `read_format`. Count fields in the extended protocol (parameter types, format codes, parameter values) are read via `read_u16` so values above 32767 are handled correctly.
`parse_frame_len` validates and converts the 4-byte big-endian frame length prefix; it takes a `max_frame_len` argument so each call site states its own bound rather than sharing a single global ceiling. The per-call-site ceiling makes rejections diagnosable by including both the declared size and the limit in the error message.
Frame-size constants: `MAX_REQUEST_SIZE` caps post-authentication request size at 2 MiB; `MAX_STARTUP_FRAME_SIZE` (10,000 bytes) matches PostgreSQL's `MAX_STARTUP_PACKET_LENGTH`; `MAX_FORWARDED_STARTUP_FRAME_SIZE` adds `FORWARDED_STARTUP_PARAM_ALLOWANCE` (512 bytes) to cover parameters that a balancer appends when forwarding; `MAX_PREAUTH_FRAME_SIZE` (16 KiB) limits pre-authentication credential frames. `CodecError::StringNoTerminator` signals a missing null terminator.
