---
source: src/ore/src/netio/read_exact.rs
revision: 0f7a9b2733
---

# mz-ore::netio::read_exact

Provides `read_exact_or_eof`, a `Future` constructor that reads exactly enough bytes to fill a buffer from an `AsyncRead`, but treats an early EOF as success rather than an error.
The returned `ReadExactOrEof` future resolves to the number of bytes actually read.
This differs from `tokio::io::AsyncReadExt::read_exact`, which treats a short read as an error, making `read_exact_or_eof` appropriate for framing protocols where EOF is a valid terminator.
