---
source: src/adapter/src/coord/sequencer/inner/copy_from.rs
revision: a23b5e40d2
---

# adapter::coord::sequencer::inner::copy_from

Implements two `COPY FROM` entry points.
`sequence_copy_from` rejects sessions using the bounded staleness isolation level with `AdapterError::BoundedStalenessReadOnly` before any other processing, since bounded staleness is read-only. For permitted sessions it handles URL and AWS S3/GCS sources (accepting `http://`/`https://` URLs and `s3://`/`gs://` URIs via an S3-compatible connection) by dispatching an `OneshotIngestionRequest` to the storage controller; when the ingestion finishes, a closure sends the resulting batches to the coordinator via `Message::StagedBatches`.
`setup_copy_from_stdin` handles STDIN: it spawns up to `COPY_FROM_STDIN_MAX_WORKERS` (8) parallel async tasks (one per worker) that receive raw byte chunks, offload CPU-intensive decoding and columnar encoding to the blocking pool for the duration of each chunk, and accumulate `ProtoBatch`es in persist; a collector task gathers the batches from all workers and returns them via a oneshot `completion_rx` channel embedded in a `CopyFromStdinWriter`, which pgwire uses to distribute the raw byte stream across workers and await the final result. The worker cap bounds how much of the shared blocking pool any one COPY can occupy.
