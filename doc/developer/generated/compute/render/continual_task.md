---
source: src/compute/src/render/continual_task.rs
revision: e79a6d96d9
---

# mz-compute::render::continual_task

Renders `ContinualTask` sinks, which watch an input collection and, on each new input change at time `T`, execute writes to an output shard at the same time `T`.
Unlike materialized views, continual tasks only react to new input deltas and do not re-process historical data, making rehydration time independent of input size.
The rendering wires together a source reader for the input's new updates, a reference reader for the output (or other referenced collections), and a persist sink that applies the computed delta.
