//! The language-agnostic equality-saturation substrate shared by the relational
//! and scalar engines: the e-class id type, the [`Language`] trait that
//! describes a node language, the [`EGraph`] data structure with its congruence
//! closure, and the per-class [`Analysis`] framework.
//!
//! SP1 introduces this core and instantiates it once, for the relational
//! engine (`EGraph<RelLang>`). The scalar engine and colored e-graphs are added
//! in later sub-projects. See `doc/developer/design/20260627_eqsat_generic_core.md`.

use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::hash::Hash;

/// An e-class id. A bare index into the union-find; not a newtype.
pub type Id = usize;

/// A node language for the generic e-graph: the e-node type plus the structural
/// operations the core needs (child traversal, child remapping, operator
/// symbol). One implementor per sort (relational `RelLang`; scalar later).
pub trait Language {
    /// The e-node type. Operator nodes carry child `Id`s; leaves carry payload.
    type Node: Clone + Eq + Hash + Ord + Debug;
    /// The operator symbol used to bucket e-nodes in the matcher index.
    type Sym: Eq + Hash + Clone;
    /// Language-owned auxiliary state stored on the e-graph (relational: the
    /// index-availability oracle; toy/scalar: `()`).
    type GraphData: Default;

    /// Child e-class ids in operand order (empty for leaves).
    fn children(node: &Self::Node) -> Vec<Id>;
    /// Rewrite a node's child ids (used to canonicalize against the union-find).
    fn map_children(node: &Self::Node, f: impl Fn(Id) -> Id) -> Self::Node;
    /// The operator symbol for index bucketing.
    fn symbol(node: &Self::Node) -> Self::Sym;

    /// Hook fired when `add` creates a NEW e-class `id` for `node` (not on a
    /// hash-cons hit). `get` resolves a child id to its canonical root. Lets a
    /// language maintain `GraphData`-resident per-class analysis incrementally.
    /// Default: no-op (the relational engine uses the batch `Analysis` driver).
    fn on_add(
        _data: &mut Self::GraphData,
        _id: Id,
        _node: &Self::Node,
        _get: &dyn Fn(Id) -> Id,
    ) {
    }

    /// Hook fired when `union` folds `loser`'s class into `winner`'s. Lets a
    /// language merge its `GraphData`-resident analysis. Default: no-op.
    fn on_union(_data: &mut Self::GraphData, _winner: Id, _loser: Id) {}
}

/// Follow union-find parent pointers in `uf` from `id` to its root. Non-
/// compressing: reads only `uf`, so it can run while another field of the
/// e-graph is mutably borrowed (the `on_add` hook closure).
fn find_in(uf: &[Id], mut id: Id) -> Id {
    while uf[id] != id {
        id = uf[id];
    }
    id
}

/// The generic view of the e-graph: every e-node grouped by its operator symbol,
/// paired with its canonical parent class.
///
/// The relational engine builds a bespoke `Sym`-keyed, `ENode`-valued index
/// (`EGraph::rel_index`) instead, so this generic form is currently unused; it
/// is retained as substrate for a future colored/scalar matcher (SP4b).
#[allow(dead_code)]
pub type Index<L> = HashMap<<L as Language>::Sym, Vec<(Id, <L as Language>::Node)>>;

/// An e-graph over node language `L`: a union-find of e-classes, each a set of
/// e-nodes, with a hash-cons memo for deduplication.
pub struct EGraph<L: Language> {
    uf: Vec<Id>,
    pub(crate) classes: HashMap<Id, HashSet<L::Node>>,
    memo: HashMap<L::Node, Id>,
    pub(crate) data: L::GraphData,
}

// Manual `Default`/`new` so we do not impose `L: Default` (the derive would).
impl<L: Language> Default for EGraph<L> {
    fn default() -> Self {
        EGraph {
            uf: Vec::new(),
            classes: HashMap::new(),
            memo: HashMap::new(),
            data: L::GraphData::default(),
        }
    }
}

impl<L: Language> EGraph<L> {
    pub fn new() -> Self {
        EGraph::default()
    }

    /// The canonical id of `id`.
    pub fn find(&self, id: Id) -> Id {
        find_in(&self.uf, id)
    }

    fn new_class(&mut self) -> Id {
        let id = self.uf.len();
        self.uf.push(id);
        self.classes.insert(id, HashSet::new());
        id
    }

    /// A copy of `n` with its children canonicalized against the union-find.
    pub(crate) fn canon(&self, n: &L::Node) -> L::Node {
        L::map_children(n, |c| self.find(c))
    }

    /// Add an e-node, returning its (canonical) e-class. Hash-conses.
    pub fn add(&mut self, node: L::Node) -> Id {
        let node = self.canon(&node);
        if let Some(&id) = self.memo.get(&node) {
            return self.find(id);
        }
        let id = self.new_class();
        self.classes.get_mut(&id).unwrap().insert(node.clone());
        // Maintain language analysis for the new class. Borrow `uf` and `data`
        // as disjoint fields so the `get` closure and `&mut data` don't alias.
        let uf = &self.uf;
        L::on_add(&mut self.data, id, &node, &|c| find_in(uf, c));
        self.memo.insert(node, id);
        id
    }

    /// Union the classes of `a` and `b`; returns whether they were distinct.
    pub fn union(&mut self, a: Id, b: Id) -> bool {
        let (ra, rb) = (self.find(a), self.find(b));
        if ra == rb {
            return false;
        }
        self.uf[rb] = ra;
        let nodes = self.classes.remove(&rb).unwrap_or_default();
        self.classes.entry(ra).or_default().extend(nodes);
        L::on_union(&mut self.data, ra, rb);
        true
    }

    /// Restore canonical-children and congruence invariants after unions.
    pub fn rebuild(&mut self) {
        loop {
            let mut merged = false;
            let mut memo: HashMap<L::Node, Id> = HashMap::new();
            let ids: Vec<Id> = self.classes.keys().copied().collect();
            for id in ids {
                let rep = self.find(id);
                let nodes: Vec<L::Node> = self
                    .classes
                    .get(&id)
                    .map(|s| s.iter().cloned().collect())
                    .unwrap_or_default();
                for n in nodes {
                    let cn = self.canon(&n);
                    if let Some(&other) = memo.get(&cn) {
                        if self.union(other, rep) {
                            merged = true;
                        }
                    } else {
                        memo.insert(cn, rep);
                    }
                }
            }
            let mut new_classes: HashMap<Id, HashSet<L::Node>> = HashMap::new();
            let old: Vec<(Id, HashSet<L::Node>)> = self.classes.drain().collect();
            for (id, nodes) in old {
                let rep = self.find(id);
                let entry = new_classes.entry(rep).or_default();
                for n in nodes {
                    entry.insert(self.canon(&n));
                }
            }
            self.classes = new_classes;
            if !merged {
                break;
            }
        }
        self.memo.clear();
        for (&id, nodes) in &self.classes {
            for n in nodes {
                self.memo.insert(n.clone(), id);
            }
        }
    }

    /// Build the operator-symbol index over all e-nodes.
    ///
    /// Call `rebuild()` first: node child ids are only canonical after a rebuild.
    /// Currently unused — the relational engine uses `EGraph::rel_index` (a
    /// `Sym`-keyed, `ENode`-valued view); retained as substrate for a future
    /// colored/scalar matcher (SP4b).
    #[allow(dead_code)]
    pub(crate) fn index(&self) -> Index<L> {
        let mut idx: Index<L> = HashMap::new();
        for (&id, nodes) in &self.classes {
            for n in nodes {
                idx.entry(L::symbol(n)).or_default().push((id, n.clone()));
            }
        }
        idx
    }

    /// The set of e-nodes in `id`'s canonical class. Empty if `id` is unknown.
    pub(crate) fn nodes(&self, id: Id) -> Vec<L::Node> {
        let rep = self.find(id);
        self.classes
            .get(&rep)
            .map(|s| s.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// The canonical ids of all live e-classes.
    pub(crate) fn class_ids(&self) -> Vec<Id> {
        self.classes.keys().copied().collect()
    }

    /// Total number of e-nodes across all classes.
    pub(crate) fn node_count(&self) -> usize {
        self.classes.values().map(|ns| ns.len()).sum()
    }

    /// The size of the union-find id space (count of ever-created e-classes,
    /// including ids since merged away). Read-only; lets a colored layer seed a
    /// union-find over the full id space.
    #[allow(dead_code)] // SP3a colored.rs consumes this; SP3b uses it in production.
    pub(crate) fn uf_len(&self) -> usize {
        self.uf.len()
    }

    /// The canonical class of `node` if it is present as a (canonical) e-node in
    /// the base, else `None`. Read-only; lets a colored layer detect when a
    /// colored conclusion is already represented in black.
    #[allow(dead_code)] // SP3b colored/ consumes this; SP4 uses it in production.
    pub(crate) fn lookup(&self, node: &L::Node) -> Option<Id> {
        self.memo.get(&self.canon(node)).map(|&id| self.find(id))
    }

    /// The language-owned auxiliary state.
    pub(crate) fn data(&self) -> &L::GraphData {
        &self.data
    }

    /// Mutable access to the language-owned auxiliary state.
    pub(crate) fn data_mut(&mut self) -> &mut L::GraphData {
        &mut self.data
    }
}

/// Default maximum fixpoint iterations for [`EGraph::run_analysis`] on
/// finite-height lattices (`NonNeg`, `Monotonic`, `Keys`). Those lattices
/// have height bounded by the plan size, so they converge in a handful of
/// rounds — well under this cap.
pub(crate) const MAX_ANALYSIS_ITERS: usize = 100;

/// A per-e-class lattice analysis over language `L`. `Domain` is the lattice
/// element; `make` is the per-node transfer function (reading children via
/// `get` and a language-specific per-run `ctx`); `merge` joins two e-nodes'
/// facts within a class.
pub trait Analysis<L: Language> {
    type Domain: Clone + Eq;
    /// Per-run context handed to every `make` call (relational: an arity
    /// provider; scalar later: column types). A copyable borrow.
    type Ctx<'a>: Copy;

    fn bottom(&self) -> Self::Domain;
    fn make(
        &self,
        node: &L::Node,
        get: &dyn Fn(Id) -> Self::Domain,
        ctx: Self::Ctx<'_>,
    ) -> Self::Domain;
    fn merge(&self, a: Self::Domain, b: Self::Domain) -> Self::Domain;
}

impl<L: Language> EGraph<L> {
    /// Run a lattice analysis to a fixpoint (bounded), one fact per e-class.
    pub fn run_analysis_bounded<A: Analysis<L>>(
        &self,
        a: &A,
        ctx: A::Ctx<'_>,
        max_iters: usize,
    ) -> HashMap<Id, A::Domain> {
        let mut m: HashMap<Id, A::Domain> =
            self.classes.keys().map(|&id| (id, a.bottom())).collect();
        for iter in 0..max_iters {
            let mut updates: Vec<(Id, A::Domain)> = Vec::new();
            for (&id, nodes) in &self.classes {
                let get = |c: Id| m.get(&self.find(c)).cloned().unwrap_or_else(|| a.bottom());
                let mut d = a.bottom();
                for n in nodes {
                    d = a.merge(d, a.make(n, &get, ctx));
                }
                if m.get(&id) != Some(&d) {
                    updates.push((id, d));
                }
            }
            if updates.is_empty() {
                break;
            }
            if iter + 1 == max_iters {
                tracing::debug!(
                    "run_analysis: did not converge after {max_iters} iterations; \
                     returning partial (under-approximate) result"
                );
                for (id, d) in updates {
                    m.insert(id, d);
                }
                break;
            }
            for (id, d) in updates {
                m.insert(id, d);
            }
        }
        m
    }

    /// Run a lattice analysis to a fixpoint with the default iteration bound.
    pub fn run_analysis<A: Analysis<L>>(
        &self,
        a: &A,
        ctx: A::Ctx<'_>,
    ) -> HashMap<Id, A::Domain> {
        self.run_analysis_bounded(a, ctx, MAX_ANALYSIS_ITERS)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Debug)]
    enum Arith {
        Num(i64),
        Add(Id, Id),
        Mul(Id, Id),
    }

    #[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
    enum ArithSym {
        Num,
        Add,
        Mul,
    }

    struct ArithLang;

    impl Language for ArithLang {
        type Node = Arith;
        type Sym = ArithSym;
        type GraphData = ();

        fn children(node: &Arith) -> Vec<Id> {
            match node {
                Arith::Num(_) => vec![],
                Arith::Add(a, b) | Arith::Mul(a, b) => vec![*a, *b],
            }
        }

        fn map_children(node: &Arith, f: impl Fn(Id) -> Id) -> Arith {
            match node {
                Arith::Num(x) => Arith::Num(*x),
                Arith::Add(a, b) => Arith::Add(f(*a), f(*b)),
                Arith::Mul(a, b) => Arith::Mul(f(*a), f(*b)),
            }
        }

        fn symbol(node: &Arith) -> ArithSym {
            match node {
                Arith::Num(_) => ArithSym::Num,
                Arith::Add(..) => ArithSym::Add,
                Arith::Mul(..) => ArithSym::Mul,
            }
        }
    }

    #[mz_ore::test]
    fn add_hashconses_identical_nodes() {
        let mut eg = EGraph::<ArithLang>::new();
        let a1 = eg.add(Arith::Num(1));
        let a2 = eg.add(Arith::Num(1));
        assert_eq!(a1, a2, "identical leaf nodes must hashcons to one class");
    }

    #[mz_ore::test]
    fn congruence_collapses_after_union() {
        let mut eg = EGraph::<ArithLang>::new();
        let a = eg.add(Arith::Num(1));
        let b = eg.add(Arith::Num(2));
        let x = eg.add(Arith::Num(3));
        let add_ax = eg.add(Arith::Add(a, x));
        let add_bx = eg.add(Arith::Add(b, x));
        assert_ne!(eg.find(add_ax), eg.find(add_bx));
        eg.union(a, b);
        eg.rebuild();
        // Congruence: once a ≡ b, Add(a,x) ≡ Add(b,x).
        assert_eq!(eg.find(add_ax), eg.find(add_bx));
    }

    #[mz_ore::test]
    fn index_buckets_by_symbol() {
        let mut eg = EGraph::<ArithLang>::new();
        let a = eg.add(Arith::Num(1));
        let b = eg.add(Arith::Num(2));
        let _ = eg.add(Arith::Add(a, b));
        let _ = eg.add(Arith::Mul(a, b));
        let idx: HashMap<ArithSym, Vec<(Id, Arith)>> = eg.index();
        assert_eq!(idx[&ArithSym::Num].len(), 2);
        assert_eq!(idx[&ArithSym::Add].len(), 1);
        assert_eq!(idx[&ArithSym::Mul].len(), 1);
    }

    // A toy analysis: each class carries the integer value of its (single) node,
    // maintained incrementally by the hooks. Exercises on_add (reading children
    // via `get`) and on_union (folding loser into winner).
    #[derive(Default)]
    struct SumData {
        vals: HashMap<Id, i64>,
    }

    struct SumLang;

    impl Language for SumLang {
        type Node = Arith;
        type Sym = ArithSym;
        type GraphData = SumData;

        fn children(node: &Arith) -> Vec<Id> {
            ArithLang::children(node)
        }
        fn map_children(node: &Arith, f: impl Fn(Id) -> Id) -> Arith {
            ArithLang::map_children(node, f)
        }
        fn symbol(node: &Arith) -> ArithSym {
            ArithLang::symbol(node)
        }
        fn on_add(data: &mut SumData, id: Id, node: &Arith, get: &dyn Fn(Id) -> Id) {
            let v = match node {
                Arith::Num(x) => *x,
                Arith::Add(a, b) => data.vals[&get(*a)] + data.vals[&get(*b)],
                Arith::Mul(a, b) => data.vals[&get(*a)] * data.vals[&get(*b)],
            };
            data.vals.insert(id, v);
        }
        fn on_union(data: &mut SumData, winner: Id, loser: Id) {
            // Loser folds into winner: drop the loser's entry, keep the winner's.
            let lo = data.vals.remove(&loser);
            if !data.vals.contains_key(&winner) {
                if let Some(lo) = lo {
                    data.vals.insert(winner, lo);
                }
            }
        }
    }

    #[mz_ore::test]
    fn on_add_reads_children_through_get() {
        let mut eg = EGraph::<SumLang>::new();
        let a = eg.add(Arith::Num(2));
        let b = eg.add(Arith::Num(3));
        let s = eg.add(Arith::Add(a, b));
        assert_eq!(eg.data().vals[&s], 5, "on_add must combine child analyses");
        assert_eq!(eg.nodes(s).len(), 1, "nodes() exposes the class contents");
    }

    #[mz_ore::test]
    fn on_union_folds_loser_into_winner() {
        let mut eg = EGraph::<SumLang>::new();
        let a = eg.add(Arith::Num(7));
        let b = eg.add(Arith::Num(9));
        // union(a, b): winner = find(a), loser = find(b). Loser's entry is dropped.
        eg.union(a, b);
        let w = eg.find(a);
        assert!(eg.data().vals.contains_key(&w), "winner keeps an entry");
        assert_eq!(eg.data().vals.len(), 1, "loser entry was removed on union");
    }

    #[mz_ore::test]
    fn default_hooks_are_noop_for_unit_graphdata() {
        // ArithLang has GraphData = () and does not override the hooks; add/union
        // must still work (defaults are no-ops).
        let mut eg = EGraph::<ArithLang>::new();
        let a = eg.add(Arith::Num(1));
        let b = eg.add(Arith::Num(2));
        eg.union(a, b);
        eg.rebuild();
        assert_eq!(eg.find(a), eg.find(b));
    }

    #[mz_ore::test]
    fn lookup_finds_present_node_and_misses_absent() {
        let mut eg = EGraph::<ArithLang>::new();
        let a = eg.add(Arith::Num(1));
        let b = eg.add(Arith::Num(2));
        let s = eg.add(Arith::Add(a, b));
        assert_eq!(eg.lookup(&Arith::Add(a, b)), Some(eg.find(s)), "present node");
        assert_eq!(eg.lookup(&Arith::Num(99)), None, "absent node");
    }
}
