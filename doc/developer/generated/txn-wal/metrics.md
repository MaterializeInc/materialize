---
source: src/txn-wal/src/metrics.rs
revision: ea8a221481
---

# mz-txn-wal::metrics

Defines `Metrics` (top-level registry), `FallibleOpMetrics` (latency histograms and error counters for operations that can fail, e.g. `commit_at`), `InfallibleOpMetrics` (latency histograms for always-succeeding operations), and `BatchMetrics` (tracks batch sizes for multi-shard transactions).
All structs are constructed once and shared via `Arc` from `TxnsHandle`.
