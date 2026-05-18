# upsert-no-internal-panic

## Summary

The upsert operator's explicit `assert!`s and `panic!`s — currently process-aborting guards — never fire under any Antithesis-injected fault sequence. Each site is converted to a uniquely-messaged `assert_always!` / `assert_unreachable!` so a firing surfaces as a reportable Antithesis property failure rather than a clusterd crash.

## Targeted assertion sites

| File | Line | Site | Antithesis form |
|------|------|------|------------------|
| `src/storage/src/upsert.rs` | 541 | `assert!(diff.is_positive(), "invalid upsert input")` | `assert_always!(diff.is_positive(), "upsert: input diff positive (classic)")` |
| `src/storage/src/upsert.rs` | 636 | `panic!("key missing from commands_state")` | `assert_unreachable!("upsert: key missing from commands_state (classic)")` |
| `src/storage/src/upsert.rs` | 1031 | `unreachable!("pending future never returns")` | `assert_unreachable!("upsert: pending future returned (classic)")` |
| `src/storage/src/upsert_continual_feedback.rs` | 626 | `assert!(diff.is_positive(), "invalid upsert input")` | `assert_always!(diff.is_positive(), "upsert: input diff positive (cf v1)")` |
| `src/storage/src/upsert_continual_feedback.rs` | 800 | `panic!("key missing from commands_state")` | `assert_unreachable!("upsert: key missing from commands_state (cf v1)")` |
| `src/storage/src/upsert_continual_feedback_v2.rs` | 315 | `assert!(diff.is_positive(), "invalid upsert input")` | `assert_always!(diff.is_positive(), "upsert: input diff positive (cf v2)")` |
| `src/storage/src/upsert_continual_feedback_v2.rs` | 483 | `unreachable!()` on `(None, None)` from joined prior/new state | `assert_unreachable!("upsert: cf v2 join produced (None, None)")` |
| `src/storage/src/upsert/types.rs` | 580 | `panic!("merge_update_state called with non-consolidating state")` | `assert_unreachable!("upsert: merge_update_state on non-Consolidating state")` |
| `src/storage/src/upsert/types.rs` | 1062 | `panic!("attempted completion of already completed upsert snapshot")` | `assert_unreachable!("upsert: snapshot completion called twice")` |

Each message is unique; an Antithesis failure report names exactly the site that was reached.

## Why these sites

These are structural invariants the operator's authors believed to be impossible. Bug history confirms several have fired in production (commits `f177db8286`, `1accbe28b3`). The cost of wrapping them with the Antithesis SDK is trivial; the upside is reportable, replayable property failures.

## Antithesis angle

- Multi-replica clusters: most relevant for `key missing from commands_state` and the `unreachable!` on `(None, None)`.
- Order-key edge cases: maps to the `assert!(diff.is_positive())` family.
- Snapshot completion: the `panic!("attempted completion of already completed upsert snapshot")` is reached if the snapshot-completion state machine is re-entered (rehydration after a crash that already completed snapshot).

## Relationship to other properties

This property is the *operator-internal* counterpart to `upsert-state-consolidation-wellformed` (which guards the math in `ensure_decoded`) and `upsert-ensure-decoded-called-before-access` (which guards the type-state protocol on `StateValue` accessors). Together they form the SUT-side instrumentation backbone for the UPSERT envelope.

## Existing instrumentation

The `assert!` / `panic!` calls already exist as process-aborting guards. They abort in test today; the work is converting them to `assert_always!`/`assert_unreachable!` so failures are *reported* rather than masked as "clusterd was restarted." Each site gets a distinct, specific message per the property-catalog requirement that assertion messages be unique.

## Provenance

Surfaced by: Concurrency, Failure Recovery. Regression targets: commits `f177db8286`, `1accbe28b3`, materialize#26655, database-issues#9160.
