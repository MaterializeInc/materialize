# Eqsat extraction under non-local cost: literature vs `extract.rs`

## Purpose

This note records what the e-graph extraction literature says about extracting a cheapest plan under a cost that does not decompose per node, and how our extractor in `src/transform/src/eqsat/extract.rs` compares.
It is reference material for future cost-model and extraction work, not a change proposal.
The four papers surveyed are SPORES (Wang et al., VLDB 2020), Carpentry Compiler (Wu et al., TOG 2019), Tensat (Yang et al., MLSys 2021), and Guided Equality Saturation (Kœhler, Goens et al., POPL 2024).
The conclusion is that our extractor already sits at or ahead of the state of the art for our specific cost model, with one cheap hardening applied and one design decision recorded for a future code path.

## The shared problem

Greedy bottom-up extraction assumes the cheapest plan for a node is built from the cheapest plan for each child.
Common subexpressions break that assumption: a shared subterm should be charged once, so a node's effective cost depends on what else the plan selects.
All four papers hit this wall independently, which is strong evidence it is intrinsic, not a quirk of our cost model.
SPORES gives the canonical worked counterexample (its Figure 3, the "CSE problem"), and both SPORES and Tensat report greedy failing on real workloads, which is why both add an integer-program extractor.
Our cost is non-local for the same reason and then some: it counts distinct maintained arrangements where the same collection arranged by the same key is charged once across all consumers, and an arrangement already covered by an index is free.

## Our extractor as built

The crate ships two extractors behind the `Extractor` trait, selected by a feature flag, with greedy as the always-sound fallback.

* `GreedyExtractor` (`extract.rs:42`) runs bottom-up dynamic programming under the configured `Objective` (`objective.rs`), and is the default.
* `IlpExtractor` (`extract.rs:64`) builds and solves a 0/1 program in `solve` (`extract.rs:134`), falling back to greedy on any failure (`extract.rs:113`).

The ILP `solve` proceeds as follows.

* It collects the subgraph reachable from the root, then caps model size at 600 nodes, deferring larger problems to greedy (`extract.rs:143`).
* It rejects a cyclic reachable subgraph up front via `reachable_has_cycle` (`extract.rs:75`, called at `extract.rs:154`); see the acyclicity section below.
* It creates one binary per `(class, node)` selection, one binary per distinct `(class, key)` arrangement, and one `class_used` binary per class.
* The objective (`extract.rs:246`) has three strictly nested tiers: the primary term is the count of non-oracle-covered arrangements, then a time-degree tie-break, then a node-count tie-break, with weights scaled so each tier dominates the next.
* Five constraint families encode the selection (`extract.rs:266` onward): root picks exactly one node, root class is used, `class_used` equals the sum of its node selections, a selected node forces each child class used, and a selected node forces its required arrangement indicators.
* It solves with `microlp` under `catch_unwind` (`extract.rs:326`), reconstructs the plan, and post-validates polarity (`extract.rs:356`), falling back to greedy on solver error, panic, or a non-linear-operator polarity violation.

The objective the ILP minimizes is its own `obj_expr`, not `Objective::cmp`; the `Objective` trait drives only greedy.
This matters: the ILP minimizes arrangement count plus tie-breaks, while greedy minimizes the full `Cost` memory-degree vector, so the two extractors can select different optima even absent solver limits.

## Head to head

### SPORES (VLDB 2020): validates our ILP shape

SPORES optimizes linear algebra via relational equality saturation on `egg`, and extracts with a Gurobi ILP.
Its formulation (its Figure 5) is structurally ours: a binary per operator node, "operator selected implies each child class selected", "class selected implies some member selected", and `minimize sum of cost times selection`.
Because a shared node carries one binary, its cost is counted once, which is the entire reason SPORES uses an ILP over greedy.
Our constraints at `extract.rs:266` onward are the same selection polytope, routed through an explicit `class_used` binary.

Two differences matter.

* SPORES shares over node identity (one node, many parents), while we share over a derived resource, the `(collection, key)` arrangement, which two distinct nodes can both require; our arrangement binary at `extract.rs` charges that once, a strict generalization SPORES has no template for.
* SPORES costs output cardinality (number of non-zeroes), which is the static size proxy our cost model deliberately moved away from; we borrow its encoding shape, not its cost unit.

SPORES omits acyclicity entirely, which is safe only because its expression DAGs are small and acyclic.

### Carpentry Compiler (TOG 2019): different remedy, transferable patterns

Carpentry compiles fabrication plans on a bespoke e-graph, with a non-additive time metric whose cost depends on shared tool setups across reordered or stacked operations.
It extracts with NSGA-II, a multi-objective genetic search, because it wants a Pareto frontier across material, precision, and time, and it judged exact solvers too expensive for producing many trade-off points.
We minimize a single scalar, so that argument against exact solvers does not bind us, and our ILP remains defensible.

Two patterns transfer.

* Two-level extraction: select a candidate, then compute its true cost with an inner sub-optimization (their vertex-collapsing schedule). This is an escape hatch if our arrangement sharing ever resists clean linearization inside one ILP, at the cost of the global-optimality guarantee the ILP gives today.
* Model sharing as a global grouping decision with a validity constraint, not a per-edge discount baked into node cost. Their "collapse same-setup vertices unless it creates a dependency cycle" is the shape of "merge operators using the same `(collection, key)` into one arrangement when valid", which is what makes "charged once" exact.

Their linearity is consumption (a resource destroyed by use), the opposite of our reusable arrangements, so that machinery does not transfer.

### Tensat (MLSys 2021): the acyclicity reference, and its retreat

Tensat superoptimizes tensor graphs on `egg` and extracts with an ILP that does encode acyclicity.
It uses a per-node binary, a per-class real topological rank, an additive objective of measured per-node runtime, a child-selection constraint, and an acyclicity constraint (its Equation 4): for a selected node, its class must rank strictly above each child class, a big-M topological order that forbids a back edge.

The decisive finding is its Section 6.5: Equation 4 is the ILP bottleneck, with solves timing out past an hour, so Tensat dropped it and moved cycle handling out of band.
Its Algorithm 2 detects cycles after each saturation iteration and pins the offending node with a `selection = 0` constraint, giving a 10x to 1000x ILP speedup.
So in-ILP acyclicity is a known dead end even for the paper that introduced it.

### Guided Equality Saturation (POPL 2024)

This paper factors one infeasible saturation into a sequence steered by human-supplied sketches, resetting the e-graph between waypoints.
Its extraction needs a local and monotonic cost, which ours is not, and it explicitly cites Wang et al. 2020 and Wu et al. 2019 for the non-local case.
Its transplantable mechanisms (episodic saturation with reset, and sketch-satisfying extraction as a debug or plan-pin tool) are orthogonal to the cost-model question this note addresses, so they are out of scope here and recorded in the project memory instead.

## Where we lead and where we lag

We lead on the cost model.
Our objective expresses exact resource-level sharing (one arrangement binary per `(collection, key)`, charged once) plus the free-if-indexed oracle by simply omitting the term for an oracle-covered `ArrangeBy`, which mirrors `cost.rs::collect_memory_into` (`cost.rs:427`) and `cost.rs::input_already_arranged` (`cost.rs:551`).
This is strictly richer than Tensat's additive per-node cost and than SPORES's node-identity sharing.

We do not encode acyclicity in the ILP, and we do not need to.
The only source of cycles in our IR is a recursive binding, and the engine deliberately does not union a recursive reference into its own definition (`engine.rs:320`), so recursive references stay opaque `LocalGet` leaves and a Let-free fragment's e-graph is acyclic by construction.
Adopting Tensat's Equation 4 would import exactly the solve-time cost Tensat spent a subsection escaping, to defend against a cycle our construction cannot produce.

We lag, by design, on objective fidelity between the two extractors.
The ILP objective omits join-intermediate and group-key memory degrees that the full `Cost` vector includes, because those are join-order dependent and only the cost model computes them.
The ILP is the arrangement-count specialist; greedy is the full-cost path.

## Decisions

* Applied: an explicit acyclicity guard.
  `reachable_has_cycle` (`extract.rs:75`) does a three-color DFS over canonical class ids and rejects a cyclic reachable subgraph before the solve (`extract.rs:154`), turning the previous silent `depth > 500` reconstruction guard (`extract.rs:379`) into an intentional, fast greedy fallback.
  This is defense in depth: the structural invariant already prevents cycles, so the guard documents and enforces it rather than fixing a live bug.
* Not adopted: Tensat Equation 4 in-ILP acyclicity, because the payoff is zero (no reachable cycle) and the cost is a known solve-time regression.
* Recorded for the future: if LetRec de-opacification ever unions recursive references into their definitions, fragment e-graphs gain real cycles and the structural defense vanishes.
  At that point adopt Tensat's Algorithm 2 (lazy detect-and-pin), not Equation 4, matching Tensat's own conclusion.
* Open: if exactness across the full memory-degree vector ever matters, the ILP objective needs the join-intermediate and group-key degree terms, which is non-trivial because they are join-order dependent.

## References

* SPORES, Wang et al., PVLDB 13(11):1919, 2020. Figure 3 (CSE counterexample), Figure 5 (ILP), Sections 3.1 to 3.2.
* Carpentry Compiler, Wu et al., ACM TOG 38(6):195, 2019. Section 5.3 (objectives, vertex-collapsing time metric, NSGA-II extraction).
* Tensat, Yang et al., MLSys 2021, arXiv 2101.01332. Section 5.1 (ILP, Equation 4), Section 6.5 and Algorithm 2 (out-of-band cycle handling).
* Guided Equality Saturation, Kœhler, Goens et al., POPL 2024, Article 58. Listing 1 and Figure 6 (guided loop), Section 3.3.3 (sketch-satisfying extraction).
* Project memory: `reference_eqsat_extraction_literature`.
