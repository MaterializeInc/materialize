# Intermediate Representation Normalization

## Summary

<!--
// Brief, high-level overview. A few sentences long.
// Be sure to capture the customer impact - framing this as a release note may be useful.
-->

Introduce normalizing transformations to some of HIR, MIR, and LIR that bring ASTs to a normal form with certain predictable properties.
The intent is to introduce *predictability* to the representation of an AST, so that transformations, explanations, and other moments where Materialize and users interact with an expression AST do not have to worry or reason about as many equivalent alternatives.

## Goals

<!--
// Enumerate the concrete goals that are in scope for the project.
-->

The main goal is to produce and maintain a `Normalize` transform that acts on a `MirRelationExpr` such that two structurally equivalent expressions become identical.

For example, the `Union` operator has its input collection in an arbitrary order, but is equivalent to any other `Union` operator with the same inputs in any other order.
The `Normalize` transform would ensure that a `Union` operator has its inputs in some specific order.

A complementary ongoing goal is to identify and document normalizing principles and transformations that support them.
We have a selection of them in this proposal, but it is possible that we will learn more as we work.
We may also conclude that some of them are too controversial and should be discarded.

Normalization should ideally be computationally cheap to perform or maintain.
It has value because we can return changed expressions to the normal form, and if that is for any reason expensive the value is reduced.
This is however not as important as bringing expressions to normal forms; we should do this even if there is some cost.

Normalization should ideally not be harmful.
If normalization undoes optimizations, we must pay careful attention to where we use it, to ensure optimization makes forward progress.

Ideally normalization would only make changes that all parties agree are fundamentally uninteresting.

## Non-Goals

<!--
// Enumerate potential goals that are explicitly out of scope for the project
// ie. what could we do or what do we want to do in the future - but are not doing now
-->

Normalization is not meant to encompass all of optimization.

We expect to have transformations that dramatically alter an AST bringing two structurally different expressions to equivalent representations.
For example, the `Union` operators may have two inputs that can be canceled (because one is the `Negate` of the other);
the transformation that removes cancelable pairs does not need to be part of normalization.

In the future we may want to include some optimizing transformations that are sufficiently easy and obvious. 
For the moment, we can rely on the fact that we plan to use normalization as part of the optimizer, and all optimizations will be run alongside normalization.

## Description

<!--
// Describe the approach in detail. If there is no clear frontrunner, feel free to list all approaches in alternatives.
// If applicable, be sure to call out any new testing/validation that will be required
-->

I came up with the following components of syntactic normalization.
These are meant to be structural changes to a `MirRelationExpr`: those that do not risk a change in meaning, by restricting themselves to very relatively simple actions on the shape of the AST.
Whether a transform is "syntactic" or an action "relatively simple" is up for debate; I hope we'll know these when we see them.

### Simplification

Some operators are just more complicated forms of other operators.
A `Reduce` with a constant argument can move that argument to a `Map`.
A `Join` with one input could be a `Filter`.
A `Join` with a constant argument *may* be able to express that argument as a `Map`. 

Some of these skate close to optimizations.
Turning an entire `Reduce` into a `Map` relies on the `group_key` being a unique key for the input collection.
I think we should start judiciously here, especialy as the complexity can spiral.
Again, we'll be running normalization alongside more sophisticated transforms that can perform more interesting optimizations.

### Movement

Many operators commute.
When it is appropriate to do so, we should move commuting operators to canonical representations.
For example, contiguous blocks of `Map`, `Filter`, and `Project` operators can be re-ordered to be `Map`s, `Filter`s, and then `Project`s.
A tree of `Union` operators may benefit from having linear operators (as above, plus `Negate`, `FlatMap`) pushed down to their inputs, that multiple `Union`s can be merged.
A tree of `Join` operators may benefit from having linear operators lifted to its root, that multiple `Join`s can be merged.

Group-wise operators like `Reduce`, `Threshold`, `TopK`, etc. (and `ArrangeBy`) may need none of this.

There are *optimizations* that allow us to move e.g. filters through the keys of reductions, but I think there may be some (debatable) value in doing structural and semantic normalization independently.
Moving e.g. filters great distances may have value, but different transforms may disagree about the direction of movement (up or down) and we would like to keep normalization generally useful.

WARNING: `Filter` operators do not introduce a specific evaluation order, and moving them is a recurring source of stress when it is used to guard erroring computation.
We are "allowed" to commute it wherever we want, but we may need to push back on constructs that try to use `Filter` as an operator that "guarantees" much of anything, as it doesn't guarantee very much about the act of execution and evaluation, and once you panic (read: "introduce an record into the error output") you can't easily undo it.

### Fusion

Stacks of equivalent operators can often be consolidated into (at most) one operator.
For example, each of `Map`, `Filter`, `Project`, `Union`, `Join`, `Negate`.
Some forms of `Reduce` and `TopK` can be fused (`Distinct`, and same (key, order) respectively).
We can restrict our attention to operators that can be fused without further investigation of the properties of their inputs.

### Canonicalization

Each operator should canonicalize its representation. 
`Filter` should sort, deduplicate, and simplify its predicates.
`Union` and `Join` should (could?) sort their inputs and permute their outputs.
`Reduce` can sort its `group_key` and `aggregate` arguments.
All operators should `reduce` their argument expressions to their simplest forms.

Several of these proposals *introduce* permutations.
These permutations are new AST nodes, but they should be easy to remove as part of a post-order traversal.

### Elision

Operators that no longer need to exist should be removed.
For example,
* Maps and Filters with empty arguments,
* Projections that take no action,
* Joins and Unions with single inputs (and no constraints),

If we introduce permutations in prior steps, this is also a great moment to remove them.
Similarly, we may wish to absorb all projections in to certain parent operators (e.g. `Reduce`).

### Constant Folding

Collections that are entirely constants can be simplified.
This is not the same as constant value propagation for columns; let's not do that here.
If you have a seven element collection and a filter on it, apply the filter


## Alternatives

<!--
// Similar to the Description section. List of alternative approaches considered, pros/cons or why they were not chosen
-->

The alternative is to not attempt to normalize expressions, or to normalize them less.

I think we will learn some about the reasons for normalization, and we may conclude that only some of the proposed normalizations have value. 

## Open questions

<!--
// Anything currently unanswered that needs specific focus. This section may be expanded during the doc meeting as
// other unknowns are pointed out.
// These questions may be technical, product, or anything in-between.
-->

The specific normalizations that have value are very much up for discussion.
The tests we use to determine if something belongs as a normalization, e.g. that it is only removes ambiguity, are up for discussion.