---
source: src/compute-types/src/dyncfgs.rs
revision: e46178d370
---

# compute-types::dyncfgs

Declares all dynamic configuration (`dyncfg`) constants for the compute layer.
Includes flags for join implementation choice (`ENABLE_MZ_JOIN_CORE`), MV sink correction buffer version (`ENABLE_CORRECTION_V2`), temporal bucketing (`ENABLE_TEMPORAL_BUCKETING`, `TEMPORAL_BUCKETING_SUMMARY`), linear join yielding policy (`LINEAR_JOIN_YIELDING`), and several others.
`all_dyn_configs` registers all constants into a `ConfigSet`.
