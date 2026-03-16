---
source: src/adapter/src/continual_task.rs
revision: 1e050fdd07
---

# adapter::continual_task

Provides `ct_item_from_plan`, a helper that converts a `CreateContinualTaskPlan` into a `ContinualTask` catalog object by substituting the placeholder local ID with the assigned global ID and constructing the catalog entry with the correct `as_of`, cluster, and dependency information.
This module centralises continual-task–specific plan-to-catalog translation that is shared between the sequencer and the catalog open path.
