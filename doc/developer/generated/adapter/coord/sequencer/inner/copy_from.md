---
source: src/adapter/src/coord/sequencer/inner/copy_from.rs
revision: cc16aaf421
---

# adapter::coord::sequencer::inner::copy_from

Implements `sequence_copy_from`, which sets up a `COPY ... FROM STDIN` ingestion: spawns parallel batch-builder tasks that decode raw bytes from the pgwire channel, writes completed `ProtoBatch`es to persist, and commits them to the target table in a group commit.
Returns a `CopyFromStdinWriter` to pgwire for streaming the raw bytes in.
