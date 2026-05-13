---
source: src/catalog/src/builtin/mz_introspection.rs
revision: 36df04b4d9
---

# catalog::builtin::mz_introspection

Defines all built-in catalog objects for the `mz_introspection` SQL schema.

This module exports 74 public items: `BuiltinLog` statics for per-worker Timely/Differential/Compute log sources, and `BuiltinView` statics that aggregate or reshape those logs.

**Logs** (`BuiltinLog`) — Each log wraps a `LogVariant` (from `mz_compute_client::logging`) and exposes raw per-worker data from the Timely and Differential dataflow runtimes. Key logs include:

- Timely scheduling and operator logs: `MZ_DATAFLOW_OPERATORS_PER_WORKER`, `MZ_DATAFLOW_ADDRESSES_PER_WORKER`, `MZ_DATAFLOW_CHANNELS_PER_WORKER`, `MZ_SCHEDULING_ELAPSED_RAW`, `MZ_COMPUTE_OPERATOR_DURATIONS_HISTOGRAM_RAW`, `MZ_SCHEDULING_PARKS_HISTOGRAM_RAW`.
- Arrangement metrics: `MZ_ARRANGEMENT_RECORDS_RAW`, `MZ_ARRANGEMENT_BATCHES_RAW`, `MZ_ARRANGEMENT_SHARING_RAW`, `MZ_ARRANGEMENT_BATCHER_*`, `MZ_ARRANGEMENT_HEAP_*`.
- Compute-level logs: `MZ_COMPUTE_EXPORTS_PER_WORKER`, `MZ_COMPUTE_DATAFLOW_GLOBAL_IDS_PER_WORKER`, `MZ_COMPUTE_FRONTIERS_PER_WORKER`, `MZ_COMPUTE_IMPORT_FRONTIERS_PER_WORKER`, `MZ_COMPUTE_ERROR_COUNTS_RAW`, `MZ_COMPUTE_HYDRATION_TIMES_PER_WORKER`, `MZ_COMPUTE_OPERATOR_HYDRATION_STATUSES_PER_WORKER`, `MZ_COMPUTE_LIR_MAPPING_PER_WORKER`.
- Peek and message logs: `MZ_ACTIVE_PEEKS_PER_WORKER`, `MZ_PEEK_DURATIONS_HISTOGRAM_RAW`, `MZ_MESSAGE_COUNTS_*`, `MZ_MESSAGE_BATCH_COUNTS_*`.
- Prometheus metrics: `MZ_CLUSTER_PROMETHEUS_METRICS`.

Many logs carry `Ontology` annotations with `OntologyLink` entries expressing foreign-key and dependency relationships between per-worker dataflow entities, used to populate the `mz_internal.mz_ontology_*` views.

**Views** (`BuiltinView`) — Aggregate the per-worker logs into user-facing introspection views by summing or grouping across workers: e.g., `mz_dataflow_operators`, `mz_dataflow_channels`, `mz_scheduling_elapsed`, `mz_compute_frontiers`, `mz_arrangement_sizes`.
