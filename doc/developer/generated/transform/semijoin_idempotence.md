---
source: src/transform/src/semijoin_idempotence.rs
revision: 5680493e7d
---

# mz-transform::semijoin_idempotence

Implements `SemijoinIdempotence`, which removes repeated applications of the same semijoin.
It detects the pattern where `A join B` is a semijoin restricting `A` and `B` already contains `Get{id} join C` on the same key columns as `A`, in which case `B` can be simplified to a cheaper expression derived from `C`.
