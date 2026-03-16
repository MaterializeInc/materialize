---
source: src/storage/src/upsert_continual_feedback.rs
revision: e1e5d200de
---

# mz-storage::upsert_continual_feedback

Implements `upsert_inner`, an alternative upsert operator variant that uses a persist feedback loop to rehydrate upsert state on restart rather than reprocessing all historical data.
The operator has two inputs: the source upsert stream and a persist-read feedback stream that reconstructs the current key-value state.
It does not update internal state until the persist feedback frontier has caught up, ensuring correctness across restarts without storing the full history in memory.
