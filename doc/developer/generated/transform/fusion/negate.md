---
source: src/transform/src/fusion/negate.rs
revision: a5355b2e89
---

# mz-transform::fusion::negate

Implements `Negate` fusion: collapses two consecutive `Negate` operators into zero (removing both), since double negation is the identity.
