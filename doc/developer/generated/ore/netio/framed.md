---
source: src/ore/src/netio/framed.rs
revision: 0f7a9b2733
---

# mz-ore::netio::framed

Defines the `MAX_FRAME_SIZE` constant (64 MiB) and the `FrameTooBig` error type for framed-stream protocols.
Both are thin definitions used by codec implementations elsewhere to enforce a maximum frame length.
