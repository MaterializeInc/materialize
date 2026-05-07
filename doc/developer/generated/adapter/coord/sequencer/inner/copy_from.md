---
source: src/adapter/src/coord/sequencer/inner/copy_from.rs
revision: 122dfd0789
---

# adapter::coord::sequencer::inner::copy_from

Implements two `COPY FROM` entry points.
`sequence_copy_from` handles URL and AWS S3/GCS sources (accepting `http://`/`https://` URLs and `s3://`/`gs://` URIs via an S3-compatible connection) by dispatching an `OneshotIngestionRequest` to the storage controller.
`setup_copy_from_stdin` handles STDIN: it spawns parallel batch-builder tasks on blocking threads that receive raw byte chunks, decode them, apply column defaults/reordering, and accumulate `ProtoBatch`es in persist; a collector task gathers the batches from all workers and sends them to the coordinator via `Message::StagedBatches`, returning a `CopyFromStdinWriter` to pgwire for distributing the raw byte stream across workers.
