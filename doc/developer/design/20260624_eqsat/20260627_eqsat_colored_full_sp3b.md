# SP3b — Full Colored E-Graph Mechanism

Status: design approved (2026-06-27)
Effort: relational+scalar eqsat unification, sub-project SP3b
Predecessors: SP1 (generic core), SP2a (scalar instance), SP3a (measurement spike)
Memory: `eqsat-shared-core-extraction`

## 1. Purpose and scope

SP3b builds the **production colored e-graph mechanism** (Singher–Itzhaky style;
arXiv:2305.19203, FMCAD'24 "Easter Egg") on top of the generic core
`core::EGraph<L>`. SP3a was a gated measurement spike whose verdict — *proceed
with shared-delta as-is* — cleared this build. SP3b replaces the spike artifacts
(the `Vec`-backed `ColoredUf` and the batch `close_color`) with the real
mechanism: a sparse delta union-find, a color hierarchy, colored congruence
closure, a per-color colored-conclusion store, and color-aware extraction.

A color is a relational context (the equivalences valid at a Filter/Join scope);
context-dependent equalities become **colored congruence**, the path that will
subsume the relational `Equivalences` analysis. SP3b builds the mechanism; SP4
wires it to the runtime (color derivation from relational structure, the real
cost model, rule-driven colored conclusions).

### In scope (the full mechanism)

- **Sparse delta union-find** (`ColoredUnionFind`): per-color `HashMap`-backed
  parent/size, path compression, union-by-size, `remove`. Stores only the
  black-roots a color has merged.
- **Color hierarchy**: colors form a tree; layered `find` canonicalizes through
  the ancestor chain to black, then applies each color's own delta. Black unions
  propagate to all colors for free.
- **Colored congruence**: the generalized `close_color` kernel over the sparse UF
  + hierarchy + colored delta nodes; **batch**, processed in tree pre-order.
- **Colored conclusions**: a per-color **canonical** hash-cons (`delta_nodes`)
  storing colored e-nodes as a delta, never in base (the ≤ 2× bound). Driven
  synthetically (`add_colored`) — there is no rule machinery yet.
- **Color-aware extraction**: a generic `extract_colored` parameterized by a
  pluggable `CostModel` trait; exercised with a toy additive cost over `ToyLang`.
- Generic over `L: Language`; exercised with `ToyLang` only. No relational/scalar
  coupling.
- A `#[cfg(test)]` differential **"clones" oracle** (`oracle_close`) that is
  the ground truth for both partition and extraction, extended to hierarchy,
  conclusions, and extraction.
- SP3a's `measure_color_explosion` harness carries over (still `#[ignore]`d).

### Explicitly out of scope (deferred to SP4)

- **Incremental colored congruence** (dirty-class tracking, colors reprocessing
  only their delta after a black rebuild). SP3b is **batch**: the layered find
  already shares the base; dirty-tracking is a perf optimization to tune against
  a real workload. SP3a measured single-pass convergence for fan_out ≥ 2.
- **Real cost integration.** `cost.rs`/`extract.rs` are the prototype's IR/Rel
  -specific machinery; generalizing them is SP4. SP3b ships the `CostModel` trait
  + a toy impl; SP4 supplies the real impl.
- **Rule-driven colored conclusions.** No DSL/matcher/applier for colors yet
  (SP2b/SP4). SP3b builds and tests the conclusion *store* via a direct
  `add_colored` API.
- **Color derivation from relational structure** (subsuming `Equivalences`). SP4.
- **Real LDBC-BI workloads.** SP4 validation, once color derivation exists.

### Production status

The `colored/` submodule is dead code in production builds until SP4 consumes it
(`#![allow(dead_code)]` at module scope, with a note naming SP4). It compiles in
normal builds (to catch breakage) and is exercised only by tests/harness.

## 2. Background: the colored model

A colored e-graph is one base e-graph (the "black"/root congruence `≅`) plus a
tree of colors. A color `c` adds equalities, yielding a coarsening `≅_c`
(`≅ ⊆ ≅_c`): colors only *add* edges, never split classes. This monotonicity is
what lets the base be shared. A child color inherits its parent's (and
transitively black's) coarsening.

Two cost sources (from SP3a §2): (1) **induced merges** (congruence cascade,
representation-independent); (2) **distinct colored canonical e-node forms** (the
per-color delta a shared-delta layer must store). SP3a measured both for the
congruence case and found the sharing ratio ≪ 1, decreasing with base size, and
the cascade factor 0.000 for fan_out ≥ 2. The shared-delta representation built
here is exactly what that verdict cleared.

The differential oracle is anchored in the paper's "clones" semantic model: a
colored e-graph under color `c` is equivalent to a separate concrete e-graph with
the same e-nodes but `c`'s representatives. The oracle *realizes* that ground
truth not by reconstructing an `EGraph<L>` (a fragile topological re-add) but by
an **independent all-pairs congruence** over the base id space plus the visible
colored delta nodes — exactly the approach SP3a's `close_color_by_copy` used
(O(n²·iters), test-sized graphs only). It shares no code with the sparse-UF /
layered-find mechanism under test, so agreement is meaningful; the mechanism's
partition and extraction must match it.

## 3. Module structure

SP3a's single `src/transform/src/eqsat/colored.rs` becomes a submodule
`src/transform/src/eqsat/colored/`:

- `colored.rs` (module root; the repo forbids `mod.rs` files via the
  `mod_module_files` lint, so the module root sits beside the `colored/` dir) —
  `ColoredEGraph<'b, L>`, `ColorId`, `ColorData`, layered
  `find`/`union`, `new_color`; module-scope `#![allow(dead_code)]` +
  `// SP4 consumes this`. Re-exports the submodule items used across files.
- `colored/union_find.rs` — `ColoredUnionFind` (sparse delta UF).
- `colored/congruence.rs` — `close`/`close_all`, `ColorMetrics` (carried from
  SP3a).
- `colored/conclusions.rs` — `add_colored`, the colored-conclusion hash-cons.
- `colored/extract.rs` — `CostModel` trait, `extract_colored`.
- `colored/toy.rs` — `ToyLang`, `ToyNode`, `ToySym`, `ToyCost`, the workload
  generators (`Lcg`, `Locality`, `GenParams`, `gen_base`, `gen_colors`,
  `indegree`) carried over from SP3a.
- `colored/oracle.rs` (`#[cfg(test)]`) — `oracle_close`, `same_partition`,
  and the differential tests.
- The `measure_color_explosion` harness moves under `colored/` (test module),
  still `#[ignore]`d.

`src/transform/src/eqsat.rs` keeps `mod colored;` (now resolving to the
directory). `src/transform/src/eqsat/core.rs` keeps the `uf_len` accessor added
in SP3a (no further core change required; see §9 for the one possible addition).

## 4. `ColoredUnionFind` — sparse delta union-find

```rust
/// A sparse, path-compressed, union-by-size union-find storing only the
/// black-roots a single color has merged. A missing key ⇒ that id is its own
/// representative (the color inherits the base/parent partition there).
pub(crate) struct ColoredUnionFind {
    parent: HashMap<Id, Id>,
    size: HashMap<Id, usize>,
}

impl ColoredUnionFind {
    pub(crate) fn new() -> Self;
    /// Local representative within THIS color's delta (input must already be
    /// canonicalized through the ancestor chain). Path-compresses.
    pub(crate) fn find_local(&mut self, id: Id) -> Id;
    /// Union two already-ancestor-canonicalized ids; union-by-size. Returns
    /// whether they were distinct.
    pub(crate) fn union_local(&mut self, a: Id, b: Id) -> bool;
    /// Drop `id`'s delta entry (hierarchy maintenance: a black/parent union has
    /// made this color's local edge redundant). Singher–Itzhaky `remove`.
    pub(crate) fn remove(&mut self, id: Id);
}
```

`find_local(id)`: if `id` not in `parent`, return `id`; else walk to root and
path-compress. `union_local(a, b)`: find both roots; if equal return false; else
attach the smaller-size root under the larger, updating `size`; insert default
entries (`parent[r] = r`, `size[r] = 1`) for roots seen for the first time.
`remove(id)`: detach `id` and reattach its children to `id`'s representative so
the partition is unchanged (the entry is redundant once a coarser layer already
unifies them).

## 5. Color tree and layered find

```rust
/// Index into a ColoredEGraph's color tree. Black (the base congruence) is the
/// implicit root every color chain bottoms out at; it is not a ColorId.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub(crate) struct ColorId(pub(crate) usize);

/// A colored e-graph: one shared immutable base + a tree of colors. Generic
/// over `L`; built on `core::EGraph<L>`. The base is borrowed, never copied.
pub(crate) struct ColoredEGraph<'b, L: Language> {
    base: &'b EGraph<L>,
    colors: Vec<ColorData<L>>,
    /// Global allocator for colored e-node ids (starts at base.uf_len()).
    next_colored_id: Id,
}

struct ColorData<L: Language> {
    parent: Option<ColorId>,           // None ⇒ parent is black (the base)
    uf: ColoredUnionFind,              // this color's own delta over its parent
    delta_nodes: HashMap<L::Node, Id>, // colored-conclusion hash-cons (§7)
}

impl<'b, L: Language> ColoredEGraph<'b, L> {
    pub(crate) fn new(base: &'b EGraph<L>) -> Self;
    pub(crate) fn new_color(&mut self, parent: Option<ColorId>) -> ColorId;
    pub(crate) fn find(&mut self, c: ColorId, x: Id) -> Id;
    pub(crate) fn union(&mut self, c: ColorId, x: Id, y: Id) -> bool;
}
```

**Two id spaces, no collision.** Base ids occupy `0..base.uf_len()`. Colored ids
(allocated by `add_colored`, §7) come from `next_colored_id`, which starts at
`base.uf_len()` and only increments — so colored ids are globally unique and
never alias base ids.

**Layered `find(c, x)`** (the load-bearing definition):
1. Black step: `let mut r = if x < base.uf_len() { base.find(x) } else { x };`
   (a colored id is its own black-level root).
2. Build the ancestor chain `[root_color, …, c]` (top-down: the color whose
   parent is black first, `c` last).
3. For each color in that order, `r = colors[color].uf.find_local(r);`.
4. Return `r`.

So color `c` sees: base partition ∪ all ancestors' deltas ∪ its own delta. Black
unions are inherited for free because step 1 always consults `base.find`.

**`union(c, x, y)`**: `let (rx, ry) = (self.find(c, x), self.find(c, y));
self.colors[c].uf.union_local(rx, ry)`.

`find` takes `&mut self` for path compression; callers that only need to read use
it the same way (compression is transparent). `new_color` pushes a `ColorData`
with an empty `uf`/`delta_nodes`; a cycle in `parent` is a caller bug (colors are
created parent-first).

## 6. Colored congruence

```rust
/// Metrics recorded by one close() run (carried from SP3a).
pub(crate) struct ColorMetrics {
    pub applied_equalities: usize,
    pub induced_merges: usize,
    pub delta_classes: usize,
    pub delta_nodes: usize,
    pub iters: usize,
}

impl<'b, L: Language> ColoredEGraph<'b, L> {
    /// Apply `equalities` in color `c`, then close congruence to a fixpoint over
    /// every e-node visible to `c`. Assumes `c`'s ancestors are already closed.
    pub(crate) fn close(&mut self, c: ColorId, equalities: &[(Id, Id)]) -> ColorMetrics;
    /// Close colors in tree pre-order (parents before children) so each child
    /// inherits its ancestors' induced merges. `per_color` lists each color's
    /// asserted equalities.
    pub(crate) fn close_all(
        &mut self,
        per_color: &[(ColorId, Vec<(Id, Id)>)],
    ) -> Vec<(ColorId, ColorMetrics)>;
}
```

`close(c, equalities)`:
1. Apply each `(a, b)` via `union(c, a, b)`, counting `applied_equalities`.
2. Collect the e-nodes **visible to `c`**: every base e-node (read immutably via
   `base.class_ids()`/`base.nodes()`, paired with its base class) plus every
   delta e-node of `c` and its ancestors (paired with its colored id).
3. Fixpoint: each round, build a fresh `memo: HashMap<L::Node, Id>`; for each
   visible node `n` with owner `o`, compute the colored canonical form
   `L::map_children(&n, |ch| self.find(c, ch))` and rep `self.find(c, o)`; on a
   memo collision with a distinct rep, `union(c, …)` and count `induced_merges`.
   Repeat until a round makes no merge; count `iters`.
4. Delta metrics (as SP3a): `delta_classes` = base roots whose colored root
   differs; `delta_nodes` = base e-nodes whose colored canonical form differs
   from their base canonical form.
5. Re-canonicalize `c`'s `delta_nodes` keys against the new partition (the ≤ 2×
   node-storage minimization; §7). **UF-edge pruning** — calling
   `ColoredUnionFind::remove` for color-local edges a coarser layer now subsumes
   — is **deferred to SP4**: it is a non-load-bearing UF-space optimization (the
   layered `find` returns correct reps whether or not redundant edges are
   pruned, and the ≤ 2× *node* bound is held by the canonical hash-cons, not by
   UF pruning). `remove` is implemented and unit-tested for SP4 to wire in.

`close_all` walks the color tree in pre-order, calling `close` per color, and
returns per-color metrics. Black must be `rebuild()`-ed by the caller first.

This is SP3a's `close_color` generalized: same memo-bucketed fixpoint, now over
the sparse UF, the hierarchy (via `find`), and the colored delta nodes.

## 7. Colored conclusions

```rust
impl<'b, L: Language> ColoredEGraph<'b, L> {
    /// Insert `node` as a colored e-node in `c`. Canonicalizes children through
    /// find(c,·). If the canonical form already exists in base, returns the base
    /// class (no colored storage). If it exists in c's or an ancestor's
    /// delta_nodes, returns that id. Else allocates a fresh colored id and
    /// stores it canonically in c.delta_nodes.
    pub(crate) fn add_colored(&mut self, c: ColorId, node: L::Node) -> Id;
}
```

`add_colored(c, node)`:
1. `let cn = L::map_children(&node, |ch| self.find(c, ch));`.
2. If `base` already contains `cn` as a canonical e-node (look it up via a base
   accessor; §9), the conclusion is already represented in black — return its
   base class. No colored storage.
3. Else search `c` and its ancestors' `delta_nodes` for `cn`; if found, return
   that id.
4. Else `let id = self.next_colored_id; self.next_colored_id += 1;` insert
   `cn -> id` into `colors[c].delta_nodes`; return `id`.

**The ≤ 2× bound.** Delta nodes are keyed by their *colored-canonical* form and
re-canonicalized on insert, so each e-node has at most one base form plus one
colored form per colored class: storage is ≤ 2× the e-nodes, never the
`∏ᵢ |[xᵢ]_b|` blow-up. `close` re-canonicalizes the `delta_nodes` keys after
merges (the paper's "≤ 1 black e-class with colored e-nodes per colored class"
minimization), and `ColoredUnionFind::remove` keeps the deltas minimal.

## 8. Color-aware extraction

```rust
/// A pluggable cost model over language `L`. SP3b ships a toy additive impl;
/// SP4 supplies the real relational/scalar cost.
pub(crate) trait CostModel<L: Language> {
    type Cost: Clone + Ord;
    /// Cost of `node` given the current best cost of each child class.
    fn cost(&self, node: &L::Node, child_cost: &dyn Fn(Id) -> Self::Cost) -> Self::Cost;
}

impl<'b, L: Language> ColoredEGraph<'b, L> {
    /// Extract the lowest-cost e-node per class under color `c`. Resolves children
    /// through find(c,·) and considers, for each colored class, its base e-nodes
    /// PLUS its colored delta e-nodes (siblings).
    pub(crate) fn extract_colored<C: CostModel<L>>(
        &mut self,
        c: ColorId,
        model: &C,
    ) -> HashMap<Id, (L::Node, C::Cost)>;
}
```

Algorithm (a bounded least-fixpoint, like `core::run_analysis`):
1. Group every e-node visible to `c` (base + `c`/ancestor delta nodes) by its
   colored class `find(c, owner)`.
2. Initialize every class's best to "unknown".
3. Iterate ≤ `MAX_ANALYSIS_ITERS`: for each class, for each member e-node,
   compute `model.cost(node, |child| best[find(c, child)])` (treating unknown
   children as +∞ so a class with no fully-costed e-node stays unknown this
   round); keep the min over the class's e-nodes (base **and** colored siblings);
   update until no class's best changes.
4. Return `class -> (best e-node, total cost)`.

The color-awareness lives entirely in (a) resolving children through `find(c,·)`
and (b) merging base + colored-sibling e-nodes into one class's candidate set, so
a colored equality can expose a cheaper representative the base extraction would
not see.

**Toy cost** (`colored/toy.rs`): `ToyCost` with `Cost = u64`, `cost(node, ch) =
1 + Σ child costs` (tree size). Lets tests assert that a colored equality
strictly lowers an extracted cost versus the base.

## 9. Core accessors

SP3a added `pub(crate) fn uf_len(&self) -> usize`. SP3b needs one read-only
lookup to implement `add_colored` step 2 — "does base already contain this
canonical e-node?":

```rust
// src/transform/src/eqsat/core.rs
/// The canonical class of `node` if it is present as a (canonical) e-node in the
/// base, else None. Read-only; lets a colored layer detect when a colored
/// conclusion is already represented in black.
#[allow(dead_code)] // SP3b colored/ consumes this; SP4 uses it in production.
pub(crate) fn lookup(&self, node: &L::Node) -> Option<Id> {
    self.memo.get(&self.canon(node)).map(|&id| self.find(id))
}
```

This and `uf_len` are the only changes to existing core code. `nodes`,
`class_ids`, `find`, `canon` already exist and are `pub(crate)`/`pub`.

## 10. Testing

The trust anchor is the **"clones" oracle**, realized (as in SP3a) by an
independent all-pairs congruence — not by reconstructing an `EGraph<L>`.

```rust
#[cfg(test)]
/// Ground truth for color `c`. `equalities` is the union of the asserted
/// equalities along `c`'s chain (c + all ancestors; black is the base); the test
/// supplies them (it created them). `delta_nodes` are the colored e-nodes visible
/// to `c`. Computes the partition by all-pairs congruence over the base e-nodes +
/// delta nodes, independent of the sparse-UF/layered-find mechanism. Exposes a
/// representative map used for both partition checks and an independent toy-cost
/// extraction. O(n²·iters); test-sized graphs only.
fn oracle_close<L: Language>(
    base: &EGraph<L>,
    equalities: &[(Id, Id)],
    delta_nodes: &[(Id, L::Node)],
) -> OracleColor<L>;
```

`OracleColor` exposes `rep(id) -> Id` (the canonical representative) and the list
of visible `(owner, node)` pairs, so partition tests compare `ceg.find(c, id)`
against `oracle.rep(id)`, and the extraction test runs the same toy-cost fixpoint
over the oracle's grouping to compare against `extract_colored`.

Test groups (all `#[mz_ore::test]`):

1. **`ColoredUnionFind`** — sparse find/union-by-size/`remove`; path compression;
   missing key ⇒ self-rep; `remove` leaves the partition unchanged.
2. **Layered find/union (hierarchy)** — a fresh colored partition equals the base
   partition; a child color inherits parent ∪ black; a black union propagates to
   all colors; sibling colors stay independent.
3. **Colored congruence vs oracle** — `close` partition matches
   `oracle_close` on fixed cases and seeded-random workloads, **including
   multi-level color trees** (extends SP3a's flat oracle to hierarchy). Metrics
   sanity: empty equalities ⇒ all-zero delta; leaf equality ⇒ no induced merges;
   `f(x)/f(y)` with `x ≅ y` ⇒ induced merge and colored-equal.
4. **Conclusions** — `add_colored` canonical dedup; a form already in base returns
   the base class (no colored storage); the ≤ 2× bound holds across random
   insertions; `close` sees base + delta nodes.
5. **`remove` correctness** — after a black/parent union subsumes a color's local
   edge, `remove` leaves the colored partition unchanged.
6. **Extraction vs oracle** — `extract_colored` matches the toy-cost extraction
   over `oracle_close`; a colored equality strictly lowers an extracted cost
   versus base extraction.
7. **Measurement harness** — SP3a's `measure_color_explosion` carries over (still
   `#[ignore]`d); §8 of the SP3a spec remains the gate of record.

Commands (mz-test skill):
- unit: `bin/cargo-test -p mz-transform eqsat::colored`
- sweep: `bin/cargo-test -p mz-transform --run-ignored ignored-only --no-capture eqsat::colored::tests::measure_color_explosion`

## 11. Conventions (locked from SP3a)

- `#[mz_ore::test]` always; never bare `#[test]`.
- Seeded inline `Lcg` (SplitMix64); no `rand` dependency.
- Module-scope `#![allow(dead_code)]` + a note naming SP4.
- Every type used across the `colored/` files is `pub(crate)` and derives
  `Debug` where the crate's `missing_debug_implementations` lint applies — the
  `private_interfaces` lint is CI-breaking under `cargo clippy -D warnings`.
- The base `EGraph<L>` is borrowed and never copied or mutated (enforced by
  `&'b EGraph<L>`).

## 12. Risks and notes for SP4

- **Speculation without a consumer.** Extraction and the conclusion store are
  built before SP4's runtime exists to validate them. Mitigated by: the
  `CostModel` trait boundary (SP4 swaps the cost without touching the mechanism),
  the synthetic `add_colored` API (SP4 swaps the driver), and the clones oracle
  (correctness is anchored independent of any consumer).
- **Incremental congruence** is the main deferred optimization; the batch design
  is correct and SP3a showed single-pass convergence for realistic arity.
- **UF-edge minimization** (`close` calling `remove` on subsumed color-local
  edges) is deferred to SP4 (§6 step 5): a non-load-bearing space optimization.
  `remove` is built and unit-tested so SP4 can wire it in.
- **Soundness** (carried from SP3a §7): maintain `≅ ⊆ ≅_c` (colors only add);
  mere presence of a node is not a judgement (only same-class membership is);
  detect vacuous (contradictory) colors — flagged for SP4's rule-driven path.
