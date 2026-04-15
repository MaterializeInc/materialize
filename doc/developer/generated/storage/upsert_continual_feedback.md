---
source: src/storage/src/upsert_continual_feedback.rs
revision: b0fa98e931
---

# mz-storage::upsert_continual_feedback

Implements `upsert_inner`, an alternative upsert operator variant that uses a persist feedback loop to rehydrate upsert state on restart rather than reprocessing all historical data.
The operator has two inputs: the source upsert stream and a persist-read feedback stream that reconstructs the current key-value state.
It only updates its internal map-like state from the persist (feedback) input, never from the source input directly, ensuring all concurrent pipeline instances produce output consistent with the global persisted state.
Source updates are stashed until their timestamp is "done" (the source upper has advanced past it) and the persist upper has caught up, at which point they are processed against the current state.
As an optimization, partial emission allows processing updates at the input frontier using provisional values that are later overwritten by the persist feedback.
