---
source: src/compute/src/render/sinks.rs
revision: 9d0a7c3c6f
---

# mz-compute::render::sinks

Entry point for rendering sink dataflow fragments; dispatches on `ComputeSinkConnection` variant to render either a `Subscribe`, `MaterializedView`, or `CopyToS3Oneshot` sink.
Applies a `MapFilterProject` to the collection before passing it to the sink-specific rendering function, so that sinks only observe the columns they need.
Also handles the `StartSignal` probe that delays sink writes until the dataflow has fully hydrated.
