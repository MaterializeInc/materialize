---
name: adapter-guide
description: >
  This skill should be used when the user works on the adapter layer,
  coordinator, pgwire, frontend peek path, peek client, timestamp oracle,
  batching oracle, or related crates. Trigger when the user mentions or edits
  files in src/adapter/, src/pgwire/, src/timestamp-oracle/, or mentions
  frontend, coordinator, read holds, timestamp selection, timestamp oracle,
  batching oracle, or query sequencing.
---

# Adapter Guide Skill

When working on the adapter layer or related crates, always read and follow the
guidance in `doc/developer/guide-adapter.md` before making changes. That
document contains correctness invariants, architectural notes, and a list of
optimizations that have been tried and rejected for correctness reasons.

Read `doc/developer/guide-adapter.md` now.

When working on the timestamp oracle (`src/timestamp-oracle/`), also read the
module-level docs in `src/timestamp-oracle/src/lib.rs` and
`src/timestamp-oracle/src/batching_oracle.rs` for the `TimestampOracle` trait
contract and the batching/linearizability invariants.

When you or the user discover that an approach is incorrect or heading in a
wrong direction, suggest distilling the learning into
`doc/developer/guide-adapter.md`. Add it to whichever section fits best — a
new correctness invariant, a new subsection under an existing invariant, a
rejected optimization, or a new top-level section if needed. Describe what was
tried, why it's wrong, and any relevant context. This keeps the guide up to
date and prevents future re-discovery of the same pitfalls.
