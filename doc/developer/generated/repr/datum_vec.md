---
source: src/repr/src/datum_vec.rs
revision: ff0e43cd83
---

# mz-repr::datum_vec

Provides `DatumVec` and `DatumVecBorrow`, a re-usable allocation for temporary `Datum` slices that avoids repeated heap allocation in hot loops such as MFP evaluation.
Uses `mz_ore::vec::repurpose_allocation` to safely reborrow the underlying vector with a shorter lifetime, requiring the borrow to be cleared before reuse.
