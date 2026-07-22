# mz-compute

## Dataflow

Avoid tees in dataflow.
They clone data.
Tees are fine for `Rc`'ed data, such as a `Trace`.
Where a raw data collection needs reuse, prefer a passthrough output over teeing (see the arrange operator's passthrough in `src/render/context.rs`).

Operators must be data-parallel.
Exchanging by anything other than a key is an exception that must be documented as such.
Render exchanges route by key; the non-key exchanges live in `src/sink` (for example batch- or worker-routed output) and are documented there.

## Operators

Operators should be stateless unless state is explicitly required.
When state is required, say why (for example Top-K maintains output-sized retraction state in `src/render/top_k.rs`).
Operators should re-use allocations and aim to be linear time in the input size.

Operators generally run to completion.
The exception is yielding to enable data consolidation for other operators.
Yielding purely for interactivity is an anti-pattern.
The documented exception is `mz_join_core` (`src/render/join/mz_join_core.rs`), which yields within a single key because one key's work can be unbounded.
Prefer the consolidation rationale, and treat interactivity yielding as a last resort with a written justification.

## Rendering

Rendering is generic.
Do not add special-interest structures serving other parts of the code here.
If something special is needed, build the right abstractions to absorb the special-casing in a localized implementation.

## Priorities

Maintainability over complexity.
Correctness over performance.
