---
source: src/adapter/src/explain/fast_path.rs
revision: 09152d2db8
---

# adapter::explain::fast_path

Implements `Explain` for `FastPathPlan`, rendering fast-path peek plans in text or JSON format via the `Explainable` newtype.
Fast-path plans are either a direct persist read or a dataflow with a constant finishing; this module formats them alongside any `DataflowMetainfo` notices.
