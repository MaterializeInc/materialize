---
source: src/adapter/src/coord/sequencer/inner/copy_from.rs
revision: 66acbdcf41
---

# adapter::coord::sequencer::inner::copy_from

Implements two `COPY FROM` entry points.
`sequence_copy_from` handles URL and AWS S3/GCS sources (accepting `http://`/`https://` URLs and `s3://`/`gs://` URIs via an S3-compatible connection) by dispatching an `OneshotIngestionRequest` to the storage controller; when the ingestion finishes, a closure sends the resulting batches to the coordinator via `Message::StagedBatches`.
`setup_copy_from_stdin` handles STDIN: it spawns parallel batch-builder tasks on blocking threads that receive raw byte chunks, decode them, apply column defaults/reordering, and accumulate `ProtoBatch`es in persist; a collector task gathers the batches from all workers and returns them via a oneshot `completion_rx` channel embedded in a `CopyFromStdinWriter`, which pgwire uses to distribute the raw byte stream across workers and await the final result.
