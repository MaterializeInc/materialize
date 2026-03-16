---
source: src/compute/src/sink/correction_v2.rs
revision: f13235ce64
---

# mz-compute::sink::correction_v2

An alternative implementation of the `Correction` buffer with a more structured design that separately manages compaction (`since`), revelation (`upper`), and storage of updates bucketed by time.
Provides amortized-efficient insertion, compaction, and iteration over consolidated updates below an `upper` frontier, designed to handle large volumes of future-timestamped retractions from temporal filters without re-sorting on every write.
