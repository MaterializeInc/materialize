# Compute protocol semantics: reading order and theorem cross-reference

Phase 1 of `doc/developer/design/20260724_compute_protocol_semantics.md`:
a Lean 4 model of the compute protocol's staging and command/response
legality contract.

## Reading order

1. `MzCompute/Basic.lean`: identifiers, `Frontier`, and the small
   enums (`Stage`, `PeekState`, `FrontierKind`) the model dispatches
   on.
2. `MzCompute/Event.lean`: the `ComputeCommand`/`ComputeResponse`/
   `Event` mirror types.
3. `MzCompute/State.lean`: `ProtocolState` and the `step` relation.
   Start here to see how a `command.rs`/`response.rs` doc-comment
   contract becomes a guard.
4. `MzCompute/Run.lean`: folding `step` into an execution,
   `WellFormed`, and two worked example traces (one legal, one not).
5. `MzCompute/Staging.lean`, `Legality.lean`, `Frontiers.lean`,
   `Peeks.lean`: the seven theorems, grouped by which part of the
   protocol they cover.

## Theorem cross-reference

| Theorem | Module | Encodes |
| --- | --- | --- |
| `hello_first` / `create_instance_second` | `Staging.lean` | `protocol.rs:52-58` |
| `create_before_reference` | `Staging.lean` | `command.rs:148-153,158-163,185-187,219-224`, `response.rs:52-54,106-107,120-121` |
| `no_reference_after_drop` | `Legality.lean` | `command.rs:152-153,162-163` |
| `create_dataflow_ids_fresh` | `Legality.lean` | `command.rs:107-108` |
| `frontiers_monotone_and_terminal` | `Frontiers.lean` | `response.rs:37-51` |
| `peek_response_unique` | `Peeks.lean` | `response.rs:66-69` |
| `cancel_requires_peek` | `Peeks.lean` | `command.rs:248-250` |

This table is the concrete form of the design doc's Success Criteria
("a reader can go from a doc-comment sentence... to the Lean lemma
that encodes it, and back"). Keep it in sync if a later phase adds or
renames a theorem.
