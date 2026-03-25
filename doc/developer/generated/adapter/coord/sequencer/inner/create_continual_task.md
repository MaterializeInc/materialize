---
source: src/adapter/src/coord/sequencer/inner/create_continual_task.rs
revision: 5b9fb22e87
---

# adapter::coord::sequencer::inner::create_continual_task

Implements `sequence_create_continual_task`, which translates a `CreateContinualTaskPlan` into a catalog entry and a compute dataflow by calling `ct_item_from_plan` and `apply_catalog_implications`.
