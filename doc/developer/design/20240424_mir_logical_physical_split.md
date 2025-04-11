# Splitting MIR into logical and physical ASTs

- Associated: (Insert list of associated epics, issues, or PRs)

MIR optimization is split into several phases; following [`optimize_dataflow()`](https://github.com/materializeinc/materialize/blob/main/src/transform/src/dataflow.rs#L44):

 - [view inlining](https://github.com/MaterializeInc/materialize/blob/8f5f8df8a08eaab6dffbad6ae71ee70abd57ac70/src/transform/src/dataflow.rs#L49)
 - logical optimization
   + [per dataflow logical optimization](https://github.com/MaterializeInc/materialize/blob/8f5f8df8a08eaab6dffbad6ae71ee70abd57ac70/src/transform/src/dataflow.rs#L52-L57)
   + [`projection_pushdown`](https://github.com/MaterializeInc/materialize/blob/main/src/transform/src/movement/projection_pushdown.rs#L43)
     * [`optimize_dataflow_filters`](https://github.com/MaterializeInc/materialize/blob/8f5f8df8a08eaab6dffbad6ae71ee70abd57ac70/src/transform/src/dataflow.rs#L59)
     * [`optimize_dataflow_demand`](https://github.com/MaterializeInc/materialize/blob/8f5f8df8a08eaab6dffbad6ae71ee70abd57ac70/src/transform/src/dataflow.rs#L66C5-L66C29)
   + [`logical_cleanup_pass`](https://github.com/MaterializeInc/materialize/blob/8f5f8df8a08eaab6dffbad6ae71ee70abd57ac70/src/transform/src/dataflow.rs#L70-L74)
 - physical optimization
   + [per dataflow physical optimization](https://github.com/MaterializeInc/materialize/blob/8f5f8df8a08eaab6dffbad6ae71ee70abd57ac70/src/transform/src/dataflow.rs#L77-L81)
   + [inter-dataflow monotonicity tracking](https://github.com/MaterializeInc/materialize/blob/8f5f8df8a08eaab6dffbad6ae71ee70abd57ac70/src/transform/src/dataflow.rs#L83)
   + [inter-dataflow pruning](https://github.com/MaterializeInc/materialize/blob/8f5f8df8a08eaab6dffbad6ae71ee70abd57ac70/src/transform/src/dataflow.rs#L85-L89)

The logical/physical split as it exists now is somewhat arbitrary, and many passes are used in both phases.
It would simplify some work with the optimizer to realize the logical/physical distinction in the MIR AST itself.
This design doc proposes splitting the MIR AST in two parts: a logical MIR AST and a physical MIR AST.
Making this split should simplify writing MIR AST passes and open up opportunities for making arrangement choices more explicitly.
The cost of this split will be some code duplication: there are transforms we'd like to make on both ASTs.

## The Problem

Making changes to the optimizer is challenging: any change to a pass or ordering of passes can have unpredictable effects on client queries.

I suspect that splitting up our ASTs will let us cordon off some forms of complexity, simplifying some of the optimiziers work (and hopefully making it easier for us to make changes without undesirable side-effects).
In particular, I hope that the logical/physical split will form a natural barrier for letting the optimizer think about more than one plan at once: logical transforms might eventually yield more than one possible plan; cost-based decisions could select which plans to run physical optimization on and which final plan to select.

## Success Criteria

A solution here will:

 - split the MIR AST in two
 - alter transforms to work on the new split ASTs (with some transforms being duplicated)
 - update the spec tests (and the `EXPLAIN PLAN` parser) and SLT tests to pass
 - update the plan tracing to support tracing of both kinds of plans
 - not cause egregious regressions in customer code

## Out of Scope

The following goals are out of scope:

 - adding new transforms or altering logical transforms to actually yield more than one possibility
 - getting _identical_ results to the current optimizer
 - adding new invariants/approaches to the AST (e.g., turning `Map`/`Filter`/`Project` into unified `Mfp` nodes)

## Solution Proposal

I propose the following split in the MIR ASTs:

> The logical MIR AST would not have `ArrangeBy` nodes at all; `JoinImplementation` would be left out of logical `Join` nodes.

## Minimal Viable Prototype

I have not done the prototyping, settling instead for planning. The changes here would not be small; I imagine breaking them into the following chunks.

1. **Create a new, `ArrangeBy`-less AST, `MirLogicalRelationExpr`.** No changes to the optimizer proper; would need to make sure we had appropriate humanizing for various `EXPLAIN` usages.

2. **Rewrite [lowering](https://github.com/materializeinc/materialize/blob/main/src/sql/src/plan/lowering.rs) to pass through `MirLogicalRelationExpr`.** Changing the target of lowering should not be complex (lowering does not seem to generate any `ArrangeBy`s); adding an injection from `MirLogicalRelationExpr` to `MirRelationExpr` should be very easy.

3. **Change logical-only passes to work on `MirLogicalRelationExpr`.** Passes that only work in the logical phase could be adapted directly. See [Who modifies what in MIR transforms? (Notion)](https://www.notion.so/materialize/3529513ff1f642dcb19fe30ac3f1af95?v=b4a02da5974f46a1b98cd36bedaab958) for details on these, but there are many eligible passes:

  - view inlining
  - `FuseAndCollapse`/`NormalizeOps` passes
    + `ProjectionExtraction`
    + `ProjectionLifting`
    + `FlatMapToMap`
    + `TopKElision`
    + `fusion::Reduce`
  - `logical_optimizer` passes
    + `NonNullRequirements`
    + `NonNullable`
    + `ReductionPushdown`
    + `ReduceElision`
  - `logical_cleanup_pass` passes
    + `SemijoinIdempotence`
  - inter-view passes
    + `ProjectionPushdown`
  - multi-pass, logical-only transforms
    + `Fusion`
    + `fusion::join::Join`
    + `UnionNegateFusion`
    + `UnionBranchCancellation`
    + `RedundantJoin`
    + `PredicatePushdown`

Migrating any of these passes to work on `MirLogicalRelationExpr` would mean also making changes in spec tests and the appropriate parser, at a minimum. They might induce changes in the SLT tests, as well.

We'll want to write these passes and have them typecheck and pass tests... but we will not yet want to plumb them through. This will let us write a series of smaller PRs, since we don't need the optimizer to work end-to-end yet.

4. **Split hybrid passes.** Passes that are used in both logical and physical phases will need to be split in two: one version that works on the logical AST and one that works on the physical AST (but see [we could get away without this, at some modest cost](#alternatives)). There are a few of these, and some of them are complex:
  - `NormalizeLets`
  - `FoldConstants`
  - `ThresholdElision`
  - `ColumnKnowledge`
  - `Demand`
  - `LiteralLifting`
  - `RelationCSE`
  - `CanonicalizeMfp`

5. **Make necessary changes in the physical passes.** There are only two physical-only passes: `LiteralConstraints` and `JoinImplementation`. The latter may need to change to accommodate missing arrangement information.

6. **Plumb through the new split pipeline.** Actually hook everything up! If we've ported tests as we've worked we should be able to do this in a series of PRs that slowly moves more and more of the processing into the adapted passes. Because of the two different AST types, We'll want to migrate this pipeline in optimizer order, i.e., plumbing things through bit by bit from the beginning of the optimizer on through.

## Alternatives

- Do nothing; invest energy elsewhere.

- Follow this plan, but don't split hybrid/logical physical passes---instead, expose an interface that works for both `MirRelationExpr` and `MirLogicalRelationExpr`.

- We could plumb through changes earlier, at the price of (a) only working on passes in the order the optimizer runs them, or (b) having very large PRs.

- Try to consider more than one plan without changing the AST.

- Enforce invariants in other ways (e.g., typechecker-like passes to ensure certain nodes are present or absent at various stages), without altering the AST.

- Try other alterations to the AST instead/at the same time. Some possibilities:

  + unified `Mfp` nodes or nodes for outer joins

  + ANF-ized ASTs

  + blackbox MIR node

  + n-ary outer join MIR node (logical only?)

- Reduce the surface of the optimizer in other ways.

  + Fold passes into `EquivalencePropagation`

  + Combine `Attribute` and `Analysis` into a single

  + Try to reduce the size of fixpoints such that, e.g., `SemijoinIdempotence` and `RedundantJoin` don't run regularly, but at single prescribed times (possibly in their, local fixpoint)

  + Focus efforts on normalization

- Characterize the meaningful pre-image of `SemijoinIdempotence`, `RedundantJoin`, and other fragile transforms; write transforms that prepare terms especially for them

## Open questions

Is this split the right one?
Are there other forms we could remove?
