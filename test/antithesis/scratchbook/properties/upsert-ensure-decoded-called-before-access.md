# upsert-ensure-decoded-called-before-access

## Summary

The six `StateValue` accessors that require the cell to be in `Value` form are always called after `ensure_decoded` has been called on that cell — the panics that currently guard the type-state protocol never fire.

## Targeted sites

`src/storage/src/upsert/types.rs`:

| Line | Accessor | Message |
|------|----------|---------|
| 297 | `into_decoded` | `panic!("called \`into_decoded without calling \`ensure_decoded\`")` |
| 369 | `into_provisional_value` | `panic!("called \`into_provisional_value\` without calling \`ensure_decoded\`")` |
| 403 | `into_provisional_tombstone` | `panic!("called \`into_provisional_tombstone\` without calling \`ensure_decoded\`")` |
| 416 | `provisional_order` | `panic!("called \`provisional_order\` without calling \`ensure_decoded\`")` |
| 430 | `provisional_value_ref` | `panic!("called \`provisional_value_ref\` without calling \`ensure_decoded\`")` |
| 440 | `into_finalized_value` | `panic!("called \`into_finalized_value\` without calling \`ensure_decoded\`")` |

Each becomes `assert_unreachable!("upsert: <accessor> on Consolidating StateValue")` with a distinct, accessor-specific message.

## Why this is a real property, not just dead code

Two reasons.

1. **Refactor net.** The upsert operator has been rewritten twice (`upsert_classic`, `upsert_continual_feedback`, `upsert_continual_feedback_v2`). Every rewrite added new call sites that touch `StateValue`. A future refactor that forgets to call `ensure_decoded` would today abort clusterd; with the Antithesis SDK in place, it surfaces as a property failure during the very first nightly run after the change.
2. **Replay anchors.** If Antithesis ever does trip one of these, the failure pinpoints the exact accessor and code path. That is materially more useful than a stack trace from a process abort, especially in a multi-replica scenario where the abort is invisible behind clusterd's auto-restart.

## What this property does *not* catch

This property only checks the type-state protocol — "ensure_decoded was called first." It does not check that the consolidating math itself is correct (that is `upsert-state-consolidation-wellformed`). The two are complementary.

## Antithesis angle

These panics are most likely to fire after a code change to the upsert operator's hot path. Antithesis exercises every operator branch with random fault injection — it should reach the rewrite-sensitive accessor sites if any exist. Cost of instrumenting is trivial (rename `panic!` to `assert_unreachable!`); the value is the regression net.

## Existing instrumentation

The `panic!`s already exist. They abort the process on misuse. The work is wrapping each with `assert_unreachable!` so the misuse is reported.

## Provenance

Surfaced by: Wildcard (this is the type-state guard family that doesn't fit a standard focus).
