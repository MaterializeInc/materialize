---
source: src/ore/src/stats.rs
revision: bea88fb10b
---

# mz-ore::stats

Provides pre-defined Prometheus histogram bucket sets and a sliding window min/max data structure.
`histogram_seconds_buckets` and `histogram_milliseconds_buckets` return subsets of standard bucket arrays (spanning sub-microsecond to multi-hour ranges), ensuring comparable percentiles across histograms.
`HISTOGRAM_BYTE_BUCKETS` is a fixed constant array for size-based histograms.
`SlidingMinMax<T>` tracks the minimum and maximum over a fixed-size sliding window using a two-stack algorithm (inspired by `moving_min_max`), with O(1) amortized push and O(1) query.
