---
source: src/ore/src/netio/timeout.rs
revision: 3211051cc6
---

# mz-ore::netio::timeout

Provides `TimedReader<R>` and `TimedWriter<W>`, `AsyncRead`/`AsyncWrite` wrappers that return `io::ErrorKind::TimedOut` if a single read or write operation exceeds a configurable `Duration`.
The timeout resets after each completed operation, so it applies per-operation rather than to the lifetime of the stream.
