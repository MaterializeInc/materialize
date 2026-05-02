---
name: mz-adapter-guide
description: >
  Correctness invariants and architectural guidance for the adapter layer,
  coordinator, pgwire, peek paths, and timestamp oracle. Trigger when the user
  works on or asks questions about these subsystems — including "how does the
  coordinator work", "what are read holds", "explain the peek path", "how does
  timestamp selection work", "why does this query block". Also trigger when
  editing files in src/adapter/, src/pgwire/, or src/timestamp-oracle/.
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
