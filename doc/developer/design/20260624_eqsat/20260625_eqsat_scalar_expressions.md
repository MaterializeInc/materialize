# Equality saturation for scalar expressions

> Scoping note for a parallel session.
> This is a design note, not an implementation plan.
> A session picking this up should brainstorm the open questions below, then write a plan.
> File and line references were grounded on 2026-06-25 against branch `claude/mir-equality-optimizer-sodbej`.
> Re-verify them before relying on any specific line, they drift.

## Purpose

The relational equality-saturation optimizer in `src/transform/src/eqsat` is a research prototype with a hard organizational constraint: there is no optimizer team, and the prevailing sentiment is to not touch the optimizer.
A scalar-expression e-graph is the most defensible slice of that work to land, because the scalar domain is small, saturation actually terminates, the cost model is trivial, and the soundness story is clean.
The motivating bug is CLU-137: a single hand-written rewrite (`MirScalarExpr::undistribute_and_or`) bundled two distinct rewrites under one guard, and the guard that was correct for one disabled the other.
A non-destructive rule-based rewriter eliminates that entire class of bug by construction, because no rewrite can shadow or disable another.
This note scopes what a scalar e-graph would subsume, where it plugs in, and whether a generic substrate shared with the relational engine is worth building.

## The CLU-137 lesson, stated precisely

`undistribute_and_or` (`src/expr/src/scalar.rs:916`) performs two rewrites.
The `could_error` guard added at `src/expr/src/scalar.rs:939` gates on
`self.could_error()`, the whole expression including the common factor. That is
too broad: a fallible cast inside the common factor (the CLU-137 case) blocks
factoring even though the factor is never dropped.

NOTE: factoring `(a∧b)∨(a∧c) → a∧(b∨c)` is NOT unconditionally sound under
exact evaluation, contrary to an earlier statement here. `And`/`Or` are
non-strict and surface an operand error only when no short-circuit value
masks it (`func/variadic.rs:80`, error wins over null when there is no
`false`/`true`). Worked counterexample, all bool, `a=null, b=true, c` an
erroring bool:

* `(a∧b)∨(a∧c)` = `(null∧true)∨(null∧err)` = `null ∨ err` = `err`.
* `a∧(b∨c)` = `null∧(true∨err)` = `null∧true` = `null` (the `b∨c` short-circuit
  masks `c`'s error).

So restructuring can move an error to a null even when no operand is dropped.
The right precondition is not "drops an operand" but "could change which errors
the term surfaces". Two sufficient, independently sound gates: factoring is
sound when every recombined residual operand cannot error, or when the common
factor cannot be null. Absorption `a∨(a∧c) → a` is sound when the dropped
operand cannot error, or when `a` cannot be null. The gates are on different
operands (factoring on the residuals/factor, absorption on the dropped
operand), so the split that decouples them from each other still holds. Both
gates are intentionally on operands OTHER than reduce's over-broad
whole-expression `self.could_error()`.

With factoring disabled, a temporal conjunct `mz_now() < cast(ts)` stayed
trapped inside an OR, and downstream temporal-filter detection
(`as_mut_temporal_filter`, `src/expr/src/scalar.rs:568`) rejected the non-binary
shape and panicked at render. The residual-error gate fixes the real repro: the
fallible cast lives in the common factor, while the residuals (`s IS NULL`,
`s = ''`) cannot error, so factoring fires.

### Soundness is an envelope, not a single transform

Materialize has no single prescribed error semantics. Where an error surfaces
depends on position, and outside `CASE` the engine is free to reorder operands
and to elide or introduce error masking (this is why `And`/`Or` are non-strict).
So the soundness question for a rule is not "does this one rewrite preserve exact
evaluation" but "does the rewritten term stay inside the envelope of behaviors
the query already permits". `CASE` and `If` are the exception: their condition is
evaluated first and the branch is selected before the others are touched, so an
error in that prescribed position must be preserved.

Two consequences for this work. First, exact-evaluation equivalence is a SAFE
SUBSET of envelope equivalence: a rule that preserves exact evaluation
(including errors) is always within the envelope, so the conservative gates
above and the exact-evaluation differential test are sound to ship as-is, they
just may be stricter than strictly necessary. Second, going beyond them (for
example firing factoring ungated because the err-vs-null difference sits in a
non-strict position the query does not pin down) requires formalizing the
envelope and replacing the differential test's exact-equality oracle with a
refinement check. That is deferred. We ship the conservative gates first.

Two separate properties matter here, and they are easy to conflate.

* Compositional, non-destructive application.
  Each rewrite is its own rule with its own precondition, and applying one never removes a form another rule needs.
  An e-graph provides this for free, because the original term stays in its e-class after a rewrite fires.
  This is the property that kills the CLU-137 class of bug.
* Rule-level proof of soundness.
  An e-graph does not prove the rules you wrote are correct.
  A wrong rule still corrupts the e-class, it just does not silently delete a sibling form.
  Proving rules sound needs verification tooling (an SMT check or property tests per rule), which is orthogonal to the e-graph and could be bolted onto the existing passes too.

The immediate CLU-137 fix is independent of all of this: split `undistribute_and_or` so factoring is gated on its residuals (not on the whole expression) and absorption stays gated on the dropped operand. Narrowing reduce's `self.could_error()` to the residual operands fixes the bug while keeping both rewrites sound.
That fix should ship on its own.
The scalar e-graph is the structural answer to the underlying fragility, not the patch for this one bug.

## What already exists, and what does not

The engine has the generic machinery a scalar rewriter needs, but the scalar IR is a stub.
What exists: a pluggable `Objective` trait (`objective.rs`, default `ArrangementCount`), a pluggable extractor (`extract.rs`, greedy and ILP), and a rule DSL with a matcher and codegen (`dsl.rs`, `matcher.rs`, `rules/`, `rules.rs`).
These are algebra-independent and reusable.

What does not exist is a decomposed scalar e-graph.
`EScalar` (`src/transform/src/eqsat/ir.rs:61`) is `struct EScalar { expr: MirScalarExpr, lit: Option<bool> }`: it interns a whole `MirScalarExpr` as one opaque payload, with a precomputed constant-fold flag.
It is not a scalar operator decomposed into e-nodes with `Id` children.
Scalars live as payload inside relational e-nodes (`Map { input: Id, scalars: Vec<EScalar> }`, `Filter { input: Id, predicates: Vec<EScalar> }`), not as e-classes the engine can rewrite.
The DSL is explicit about this: its header states that scalars are opaque, the DSL never destructures a predicate or a map expression, it only moves whole payload lists around.
`raise.rs` reconstructs predicates by reading `s.expr` straight off the payload, `cse.rs` only deduplicates relational subterms, and `cost.rs` queries scalars only through `cols()`.

The consequence is concrete: there are zero scalar rewrite rules, and none can be written today, because the matcher and DSL cannot match or build scalar structure.
Scalar simplification is still delegated to the destructive `MirScalarExpr::reduce` path and the `coalesce_mfp` crutch that the relational design retained as an INCLUDE rather than a subsume.
So a scalar e-graph is not "add rules to an IR that already represents the terms."
It is "decompose the scalar IR into real e-nodes first, then add rules, a scalar cost, and a non-destructive boundary."
The decomposition is the load-bearing prerequisite, scoped in the next section.

## Prerequisite: decompose the scalar IR

Before any rule can fire, the scalar e-graph has to exist.
This is the first body of work, and it has no shortcut.

* Add scalar operator e-nodes to the IR (`CallUnary`, `CallBinary`, `CallVariadic`, `If`, `Column`, `Literal`, ...), each holding `Id` children for its operands rather than a nested `MirScalarExpr`.
  Leaves (`Column`, `Literal`, `CallUnmaterializable`) stay as payload.
* Extend the lower bridge to walk a `MirScalarExpr` and intern its subterms as e-nodes, and the raise bridge (`raise.rs`) to reconstruct a `MirScalarExpr` from the extracted e-nodes instead of cloning `s.expr`.
* Extend the DSL and matcher to match and construct scalar structure (operator patterns with operand variables, and side conditions on payload leaves such as "is a literal" or "could error").
* Give scalars a cost in the objective so extraction can choose among equivalent forms.

This is where the relational design deliberately stopped, because the relational fixes did not need scalar e-classes.
The cost is real but bounded: the scalar algebra is small and finite, and the decomposition is mechanical.

## Proposed scope

Once the IR is decomposed, build a non-destructive, form-targeted scalar canonicalizer.

The rewrite inventory of `MirScalarExpr::reduce` and `undistribute_and_or` is 54 distinct rewrites, of which 52 are always sound and 2 are precondition-gated (the `undistribute_and_or` pair).
The distribution matters for sequencing.

* The bulk, roughly 40 rewrites, are local term rewrites that map directly to rules: constant folding, null and error propagation, identity elimination, boolean algebra, if-condition resolution, canonical operand ordering, and `RecordGet` on `RecordCreate`.
  These are the easy, high-value core.
* A handful, around 6 rewrites, need operand metadata to fire: involution elimination and if-then-else function distribution inspect `preserves_uniqueness`, `could_error`, or type unification.
  These map to rules with side conditions on payload leaves.
* The two hardest rewrites are exactly the `undistribute_and_or` pair that motivated this work.
  Factoring `(a∧b)∨(a∧c) → a∧(b∨c)` needs a global search for common operands across an OR, and absorption `a∨(a∧c) → a` carries the `could_error` precondition (`src/expr/src/scalar.rs:939`).
  In the current pass these run as a recursive fixpoint with a heuristic choice among factoring opportunities, not as a local rewrite.
  An e-graph reframes this: factoring becomes a rule that fires non-destructively wherever the pattern matches, and the extractor, not a heuristic, picks the form.
  This is the rewrite that most needs the e-graph, and also the one whose porting is least mechanical, so it should be designed deliberately rather than ported line for line.

Scope boundaries.

* Subsume the `reduce` folding, identity, boolean-algebra, and null-propagation rewrites as individual rules.
* Subsume `undistribute_and_or` as two separate rules: unconditional factoring, and absorption carrying its own `could_error` precondition.
  Splitting them is the structural answer to CLU-137.
* Plug into `CanonicalizeMfp` (`src/transform/src/canonicalize_mfp.rs`), which is where MFP predicates are normalized and where the temporal-filter shape is established.
* Leave arithmetic and function-specific identities as an optional later expansion, the boolean and null layer is where the soundness wins concentrate.

The deliverable is a canonicalizer that replaces a destructive simplification pass, not a generic algebraic simplifier.
That framing keeps the surface small and the merge argument narrow.

## The load-bearing design constraint

Scalar simplification does not optimize for minimal expression size in isolation.
Downstream consumers demand specific normal forms, and a smaller but misshapen AST breaks them.
The concrete consumers and their shape requirements are:

* Temporal-filter detection.
  `as_mut_temporal_filter` (`src/expr/src/scalar.rs:568`) feeds `extract_temporal_bounds` (`src/expr/src/scalar/optimizable.rs:127`), which requires a binary predicate `mz_now() OP expr` with `OP` in `{Eq, Lt, Lte, Gt, Gte}`, `mz_now()` as a top-level operand, and no `mz_now()` in the other side.
  Symmetric forms are swapped and the operator inverted.
  A non-conforming shape is the CLU-137 failure: detection rejects it and rendering panics.
* Literal-constraint matching.
  `literal_constraints.rs` (`any_expr_eq_literal`) requires `expr = literal` at predicate top level, not under a `NOT`, to build index-accelerated filters.
  Violation silently disables the index path.
* Filter characteristics for join ordering.
  `join_implementation.rs` reads equality and inequality against literals at top level to estimate selectivity.
  Violation produces wrong cardinality estimates and bad join orders.
* Predicate canonicalization.
  `canonicalize_predicates` expects a flat conjunction, no nested `And`, and casts normalized onto the literal side (`invert_casts_on_expr_eq_literal`).

So the extraction objective must produce the canonical form these consumers expect, not the smallest AST.
This is the same lesson the relational work paid for, that extraction objective must match downstream consumers, recurring in the scalar domain.
The practical consequence is that this is a form-targeted extractor with a cost or constraint that encodes the required shape, and that requirement should drive the objective design from the start.

## Generic substrate: shared core for relational and scalar

The honest answer is yes there is a shared substrate, and no you should not build it speculatively from one user.

The egg and egglog architecture is the well-trodden shape: an e-graph parameterized by a `Language` trait (what a node is) and an `Analysis` trait (the lattice attached to each e-class, merged on union), with a generic saturation runner and a generic extractor parameterized by a cost function.
Our engine already gestures at this with pluggable `Objective` and `Extractor`, but `egraph.rs` and the IR are hardcoded to the relational and scalar node types in `ir.rs`.

The relational engine deliberately diverged from egg for real reasons, and those reasons do not all apply to scalars.

* The relational cost is non-compositional (set-cardinality over distinct arrangements), which is why bottom-up extraction was insufficient and ILP was added.
  Scalar cost is compositional (expression size or eval cost), so egg-style bottom-up extraction suffices.
* Relational nodes carry heavy payloads (type, join implementation, equivalences).
  Scalar nodes are light.
* Relational saturation blows up on join-order enumeration, the canonical e-graph explosion, and that risk is explicitly deferred.
  Scalar saturation terminates in practice, this is the classic egg use case.

So the generic core cannot be just egg, it has to be the generalization of what already works here.

Recommendation, graded by risk.

* Share immediately, low risk and obviously generic.
  The e-graph data structure (hash-cons, union-find, congruence closure), the saturation loop skeleton with its iteration and size bounds, the extractor mechanism (already pluggable via the cost trait), and the rule DSL syntax and codegen.
  These are algebra-independent and sharing them costs little.
* Do not share yet, let the two domains differ until a second user exists.
  Node definitions, the MIR-to-e-graph lower and raise bridges, the rewrite rules, the cost semantics, and the analyses content.
  These are exactly where the domains genuinely diverge, and abstracting them from a single user guesses wrong.

The sound sequence is to build the scalar canonicalizer as its own instantiation first, discover what it actually shares with the relational engine, then extract the common core once two concrete users constrain the abstraction.
Building the framework first, from one user, is premature generalization and will produce the wrong seams.
The generic substrate is the destination, not the opening move.

## Risks and non-goals

* Non-goal: proving rules sound out of the box.
  The e-graph gives non-destructive application, not rule proofs.
  Per-rule verification is a worthwhile follow-up, not a precondition.
* Non-goal: replacing the relational engine or running scalar eqsat on the customer query path by default.
  Land it as a contained canonicalizer, gated, so it needs minimal ongoing ownership.
* Risk: the form-targeting constraint.
  If the extractor minimizes size without encoding the downstream normal forms, it will produce smaller expressions that break temporal-filter detection or index matching.
  This must be designed in, not discovered in CI.
* Risk: destabilizing the working relational engine if the generic refactor is attempted too early.
  Defer it until the scalar user exists.

## The CanonicalizeMfp boundary

`CanonicalizeMfp` (`src/transform/src/canonicalize_mfp.rs`) extracts an MFP, runs `mfp.optimize()` (structural only, no `reduce`), then rebuilds, and the scalar simplification happens in `canonicalize_predicates` (`src/expr/src/relation/canonicalize.rs`).
The destructive scalar calls in that path are `p.reduce(...)` at `canonicalize.rs:230` and `:437` (plus `:87` on the popped expression), each operating on a single `&mut MirScalarExpr`.
`undistribute_and_or` runs underneath, via `reduce`.
In the pipeline, `CanonicalizeMfp` runs in `physical_optimizer` after `LiteralConstraints` and the `JoinImplementation` fixpoint, around `RelationCSE`.

The cleanest seam is to replace the per-predicate `reduce` calls at `canonicalize.rs:230` and `:437` with a non-destructive eqsat canonicalize step, gated behind a flag.
The boundary type is a single predicate, the replacement is form-preserving by construction, and the external contract of `CanonicalizeMfp` does not change.

## Suggested first steps for the parallel session

1. Decompose the scalar IR: add scalar operator e-nodes with `Id` children, and extend the lower and raise bridges to walk `MirScalarExpr` instead of interning it whole.
   This is the prerequisite that unblocks everything else.
2. Extend the DSL and matcher to express scalar operator patterns and payload-leaf side conditions, then port the ~40 local rewrites from `reduce`.
3. Encode the downstream normal forms (temporal, literal-constraint, filter-characteristics, flat-conjunction) as the extraction objective, before relying on extraction output.
4. Design the `undistribute_and_or` pair as two rules (unconditional factoring, `could_error`-gated absorption) and confirm the extractor, not a heuristic, selects the temporal-safe form.
5. Wire the non-destructive step into `canonicalize_predicates` behind a flag, and validate against the CLU-137 repro plus the index and join-ordering consumers.
