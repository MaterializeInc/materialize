---
source: src/adapter/src/coord/sequencer/inner/copy_from.rs
revision: 1d15c92c08
---

# adapter::coord::sequencer::inner::copy_from

Implements `sequence_copy_from`, which handles all `COPY FROM` variants: HTTP URL, AWS S3 / GCS (accepting `s3://` and `gs://` URIs via an S3-compatible connection), and STDIN.
For URL and S3/GCS sources, it dispatches an `OneshotIngestionRequest` to the storage controller.
For STDIN, it spawns parallel batch-builder tasks that decode raw bytes from the pgwire channel, writes completed `ProtoBatch`es to persist, and commits them to the target table in a group commit, returning a `CopyFromStdinWriter` to pgwire for streaming the raw bytes in.
