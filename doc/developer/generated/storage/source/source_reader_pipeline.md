---
source: src/storage/src/source/source_reader_pipeline.rs
revision: b382b15182
---

# mz-storage::source::source_reader_pipeline

Implements `create_raw_source`, the function that turns a `SourceRender` implementation and a `RawSourceCreationConfig` into raw timely streams ready for reclocking and decoding.
It renders the source in a `SourceTimeDomain` root scope using a `tokio::sync::watch` channel to pass the probed upstream frontier, captures the output streams using `PusherCapture` to cross scope boundaries, creates the remap operator to write timestamp bindings into the remap shard, and feeds bindings into the `reclock` utility to produce an `IntoTime`-timestamped data stream. It also derives a no-data stream (`remap_upper`) whose frontier tracks the remap shard upper by dropping all binding data; this stream is returned alongside the reclocked exports and is consumed by the persist sink during snapshotting to pace ceiling commitments.
`RawSourceCreationConfig` bundles all per-source creation metadata (id, exports, as-of, resume uppers, metrics, persist clients, etc.).
`create_raw_source` accepts one committed-upper stream per export (`BTreeMap<GlobalId, StreamVec<'scope, mz_repr::Timestamp, ()>>`). `reclock_committed_upper` reclocks each per-export `IntoTime` upper into the source's `FromTime` domain and packages them as `ResumeUppers<FromTime>`, where `ResumeUppers::source` is the frontier every export has committed through (absent while any export is still snapshotting) and `ResumeUppers::exports` holds each export's individual frontier.
When a `DataflowError::SourceError` carries a non-empty `hint`, that hint is forwarded to `HealthStatusUpdate::stalled` so the status collection surfaces the recovery suggestion to the user. For all other error kinds, a generic default hint is used.
