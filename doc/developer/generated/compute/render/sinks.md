---
source: src/compute/src/render/sinks.rs
revision: e79a6d96d9
---

# mz-compute::render::sinks

Entry point for rendering sink dataflow fragments; dispatches on `ComputeSinkConnection` variant to render either a `Subscribe`, `MaterializedView`, `CopyToS3Oneshot`, or `ContinualTask` sink.
Applies a `MapFilterProject` to the collection before passing it to the sink-specific rendering function, so that sinks only observe the columns they need.
Also handles the `StartSignal` probe that delays sink writes until the dataflow has fully hydrated.
