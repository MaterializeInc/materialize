// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

//! Processing Bindings
//!
//! This module implements functionality for inspecting and modifying dataflow
//! graphs in the presence of local bindings. [`RelationExpr`] includes the
//! ability to abstract over values through [`RelationExpr::Let`]. Each such let
//! expression associates a name with a value and makes that *binding* available
//! within a body, which can then access the value again through
//! [`RelationExpr::Get`]. At the same time, the binding's visibility is
//! restricted to the body. It is *scoped*.
//!
//! While all of this is familiar from programming languages, it does have
//! non-trivial implications on optimizations. Notably, a [`RelationExpr`]
//! with let and get expressions is not referentially transparent and thus
//! restricts how it can be transformed. In particular, the body of a let
//! expression depends on the binding and the binding, in turn, depends on
//! its value. Shadowing introduces further constraints that are difficult
//! to preserve in a system that may just execute several independent queries
//! at the same time. To avoid that, we introduce our first invariant:
//!
//! >   **Invariant 1**: Every binding has a unique name, i.e., there is *no*
//! >   shadowing of names.
//!
//! But even without shadowing the *depends-on* relationship just introduced
//! imposes certain ordering constraints when processing bindings.
//!
//! Since preserving *depends-on* constraints during arbitrary optimizations can
//! be difficult, we may want to hoist bindings to the top of the dataflow
//! graph. Our second invariant covers that case:
//!
//! >   **Invariant 2**: The value of a local binding must be processed before
//! >   its body.
//!
//! All methods for visiting [`RelationExpr`] including `visit1()` and
//! `visit1_must()` already preserve that invariant. Our third invariant follows
//! directly from the second invariant and covers the hoisting of existing local
//! bindings:
//!
//! >   **Invariant 3**: When extracting bindings from a dataflow graph, e.g.,
//! >   during hoisting, they must be extracted *in-order*.
//!
//! Our fourth invariant also follows from the second one and covers the
//! introduction of new bindings, notably when deduplicating repeated subgraphs:
//!
//! >   **Invariant 4**: When injecting bindings into a dataflow graph, e.g.,
//! >   during deduplication, they must be injected in *post-order*.
//!
//! Please do note that hoisting and deduplication interact with each other.
//! While it is possible to first perform hoisting, deduplication still needs
//! to process the values of the hoisted local bindings while preserving the
//! second invariant, i.e., inserting new bindings just above the hoisted
//! binding currently being processed. Also, the reversed pre-order of a tree
//! does *not* result in the post-order.

use crate::RelationExpr;
use indexmap::IndexMap;
use repr::RelationType;
use std::collections::HashMap;

// -----------------------------------------------------------------------------

/// For now, an identifier is just a string,
type Identifier = String;

/// Create a fresh identifier that is guaranteed not to be bound.
pub fn fresh_id() -> Identifier {
    format!("bdg-{}", uuid::Uuid::new_v4())
}

/// Determine whether the expression is bindable, i.e., does not produce a
/// constant or access the value of a binding. This function is used to suppress
/// bindings for such expressions.
pub fn is_bindable(expr: &RelationExpr) -> bool {
    match expr {
        RelationExpr::Constant { .. } => false,
        RelationExpr::Get { .. } => false,
        _ => true,
    }
}

/// Create a new *local* binding, i.e., [`RelationExpr::Let`].
pub fn bind_local<I>(name: I, value: RelationExpr, body: RelationExpr) -> RelationExpr
where
    I: Into<Identifier>,
{
    RelationExpr::Let {
        name: name.into(),
        value: Box::new(value),
        body: Box::new(body),
    }
}

//// -----------------------------------------------------------------------------
//
///// A binding between a name and a value.
//#[derive(Debug)]
//pub struct Binding {
//    name: Identifier,
//    value: RelationExpr,
//}
//
//impl Binding {
//    /// Create a new binding for the given name and value.
//    pub fn new(name: Identifier, value: RelationExpr) -> Self {
//        Self { name, value }
//    }
//
//    /// Apply the binding to the body, yielding a [`RelationExpr::Let`].
//    pub fn use_in(self, body: RelationExpr) -> RelationExpr {
//        RelationExpr::Let {
//            name: self.name,
//            value: Box::new(self.value),
//            body: Box::new(body),
//        }
//    }
//}

// -----------------------------------------------------------------------------

/// An environment.
///
/// Environments track bindings from names to values in insertion order.
/// In contrast to [`RelationExpr::Let`], environment bindings have no
/// body. Nonetheless, their scope is implicit in the ordering, namely all
/// subsequent bindings.
#[derive(Debug, Default)]
pub struct Environment {
    bindings: IndexMap<Identifier, RelationExpr>,
}

impl Environment {
    /// Determine whether the name is bound in this environment.
    #[allow(clippy::ptr_arg)]
    pub fn is_bound(&self, name: &Identifier) -> bool {
        self.bindings.contains_key(name)
    }

    #[allow(clippy::ptr_arg)]
    fn ensure_unbound(&self, name: &Identifier) {
        if self.bindings.contains_key(name) {
            panic!("environment already contains binding for {}", name);
        }
    }

    /// Add a new binding for the given name and value to this environment.
    /// This method panics if this environment already contains a binding
    /// with the given name.
    pub fn bind(&mut self, name: Identifier, value: RelationExpr) -> &mut Self {
        self.ensure_unbound(&name);
        self.bindings.insert(name, value);
        self
    }

    /// Look up the given name in this environment.
    #[allow(clippy::ptr_arg)]
    pub fn lookup(&self, name: &Identifier) -> Option<&RelationExpr> {
        self.bindings.get(name)
    }

    /// Extract the binding from the given dataflow graph. If the expression is
    /// a [`RelationExpr::Let`], this method adds a binding for the name and
    /// value to this environment and replaces the let expression with the body.
    /// Otherwise, it does nothing.
    pub fn extract(&mut self, expr: &mut RelationExpr) -> &mut Self {
        if let RelationExpr::Let { .. } = expr {
            if let RelationExpr::Let { name, value, body } = expr.take() {
                self.ensure_unbound(&name);
                self.bindings.insert(name, *value);
                *expr = *body;
            } else {
                unreachable!();
            }
        }
        self
    }

    /// Extract all bindings from the given dataflow graph. This method
    /// traverses the entire dataflow graph while relying on
    /// [`Environment::extract`] to remove individual instances of
    /// [`RelationExpr::Let`] from the graph.
    pub fn extract_all(&mut self, expr: &mut RelationExpr) -> &mut Self {
        // NB: visit1_mut() invokes the callback on the children of a
        // RelationExpr. That is not good enough for RelationExpr::Let since the
        // value and body might just be RelationExpr::Let's, too. Hence we
        // recurse on extract().
        if let RelationExpr::Let { value, .. } = expr {
            self.extract_all(value);
            self.extract(expr);
            // By the magic vested in extract1(), expr now is let's body.
            self.extract_all(expr);
        } else {
            expr.visit1_mut(|e| {
                self.extract_all(e);
            });
        }
        self
    }

    pub fn unbind_all(&mut self, expr: &mut RelationExpr) {
        match expr {
            RelationExpr::Let { .. } => {
                if let RelationExpr::Let {
                    name,
                    mut value,
                    body,
                } = expr.take()
                {
                    // Process value.
                    self.unbind_all(&mut value);

                    // Push binding.
                    let n2 = name.clone();
                    let prior = self.bindings.insert(name, *value);

                    // Process body.
                    *expr = *body;
                    self.unbind_all(expr);

                    // Pop binding.
                    if let Some(v) = prior {
                        self.bindings.insert(n2, v);
                    } else {
                        self.bindings.pop();
                    }
                } else {
                    unreachable!();
                }
            }
            RelationExpr::Get { name, .. } => {
                if let Some(v) = self.lookup(name) {
                    *expr = v.clone();
                }
            }
            _ => expr.visit1_mut(|e| self.unbind_all(e)),
        }
    }

    /// Use this environment's bindings in the given expression. This method
    /// folds the bindings from the right by creating [`RelationExpr::Let`]
    /// instances. The given expression serves as initial value for the
    /// accumulation.
    pub fn use_in(self, body: RelationExpr) -> RelationExpr {
        self.bindings
            .into_iter()
            .rev()
            .fold(body, |body, (name, value)| bind_local(name, value, body))
    }

    /// Inject this environment's bindings into the given dataflow graph. This
    /// method is the imperative version of [`Environment::use_in`].
    pub fn inject(self, expr: &mut RelationExpr) {
        *expr = self.use_in(expr.take());
    }
}

// =============================================================================

/// The hoist optimization.
#[derive(Debug)]
pub struct Hoist;

impl Hoist {
    /// Hoist all bindings to the top of a dataflow graph.
    pub fn hoist(expr: &mut RelationExpr) {
        let mut env = Environment::default();
        env.extract_all(expr);
        env.inject(expr);
    }
}

impl super::Transform for Hoist {
    /// Hoist all bindings to the top of a dataflow graph.
    fn transform(&self, expr: &mut RelationExpr, _meta: &RelationType) {
        Hoist::hoist(expr);
    }
}

// =============================================================================

#[derive(Debug)]
pub struct Unbind;

impl Unbind {
    pub fn unbind(expr: &mut RelationExpr) {
        let mut env = Environment::default();
        env.unbind_all(expr);
    }
}

impl super::Transform for Unbind {
    fn transform(&self, expr: &mut RelationExpr, _meta: &RelationType) {
        Unbind::unbind(expr);
    }
}

// =============================================================================

/// A count. It is used during the first phase of deduplication to
/// track the number of times a dataflow subgraph appears in a larger
/// dataflow graph.
#[derive(Debug, Default)]
pub struct Count {
    count: usize,
}

impl Count {
    /// Increment the count.
    pub fn incr(&mut self) -> &Self {
        self.count += 1;
        self
    }

    /// Determine whether the count is larger than one.
    pub fn is_repeated(&self) -> bool {
        self.count > 1
    }
}

/// A patch. It is used during the second phase of deduplication to
/// track the name and type of a repeated dataflow graph.
#[derive(Clone, Debug)]
pub struct Patch {
    name: Identifier,
    typ: RelationType,
}

impl From<Patch> for RelationExpr {
    /// Convert a patch into a [`RelationExpr::Get`].
    fn from(patch: Patch) -> Self {
        RelationExpr::Get {
            name: patch.name,
            typ: patch.typ,
        }
    }
}

/// The metadata record associated with a dataflow graph during deduplication.
#[derive(Debug)]
pub enum Metadata {
    Counting(Count),
    Patching(Patch),
}

impl Default for Metadata {
    fn default() -> Self {
        Metadata::Counting(Count::default())
    }
}

impl Metadata {
    /// Determine whether the metadata is a count.
    pub fn is_count(&self) -> bool {
        if let Metadata::Counting(_) = self {
            true
        } else {
            false
        }
    }

    /// Unwrap a count. This method panics, if the metadata is a patch.
    pub fn unwrap_count(&mut self) -> &mut Count {
        if let Metadata::Counting(count) = self {
            count
        } else {
            panic!("trying to unwrap a patch as a count")
        }
    }

    /// Unwrap a patch. This method panics if the metadata is a count.
    pub fn unwrap_patch(&self) -> &Patch {
        if let Metadata::Patching(patch) = self {
            patch
        } else {
            panic!("trying to unwrap a count as a patch")
        }
    }

    /// Start patching the given dataflow graph. This method transitions
    /// the metadata from counting to patching. The resulting patch has
    /// a fresh name and the value's type. This method panics if the
    /// metadata is a count smaller than two or already a patch.
    pub fn start_patching(&mut self, value: &RelationExpr) -> (Identifier, RelationExpr) {
        if let Metadata::Counting(Count { count }) = self {
            if *count < 2 {
                panic!("trying to patch dataflow node that isn't duplicated");
            }

            let patch = Patch {
                name: fresh_id(),
                typ: value.typ(),
            };
            let name = patch.name.clone();
            let reference = patch.clone().into();

            *self = Metadata::Patching(patch);
            (name, reference)
        } else {
            panic!("trying to patch dataflow node that has been patched already");
        }
    }
}

// -----------------------------------------------------------------------------

/// The deduplicate optimization.
#[derive(Debug)]
pub struct Deduplicate;

impl Deduplicate {
    /// Determine how many times each dataflow graph appears in the given
    /// dataflow graph. This method assumes that the given census is empty.
    pub fn count_all<'a>(expr: &'a RelationExpr, census: &mut HashMap<&'a RelationExpr, Metadata>) {
        let metadata = census.entry(expr).or_insert_with(Metadata::default);
        if !metadata.unwrap_count().incr().is_repeated() {
            expr.visit1(|e| Deduplicate::count_all(e, census));
        }
    }

    /// Patch the dataflow graph. This method adds a binding to the given
    /// environment for each repeated subgraph, while also replacing each
    /// occurrence with a [`RelationExpr::Get`] for the binding. It relies
    /// on the given census for identifying repeated subgraphs. It assumes
    /// that the census contains a metadata entry for each subgraph and
    /// that each entry is a count.
    pub fn patch_all(
        expr: &mut RelationExpr,
        census: &mut HashMap<&RelationExpr, Metadata>,
        env: &mut Environment,
    ) {
        let metadata = census
            .get_mut(expr)
            .expect("metadata for dataflow graph is missing from census");

        if let Metadata::Counting(Count { count }) = metadata {
            if *count <= 1 || !is_bindable(expr) {
                expr.visit1_mut(|e| Deduplicate::patch_all(e, census, env));
            } else {
                let (name, reference) = metadata.start_patching(expr);
                expr.visit1_mut(|e| Deduplicate::patch_all(e, census, env));
                let value = std::mem::replace(expr, reference);
                env.bind(name, value);
            }
        } else {
            *expr = metadata.unwrap_patch().clone().into();
        }
    }

    /// Deduplicate repeated subgraphs.
    pub fn deduplicate(expr: &mut RelationExpr) {
        let mut census = HashMap::new();
        let expr_prime = expr.clone();
        Deduplicate::count_all(&expr_prime, &mut census);

        let mut env = Environment::default();
        Deduplicate::patch_all(expr, &mut census, &mut env);
        env.inject(expr);
    }
}

impl super::Transform for Deduplicate {
    /// Deduplicate repeated subgraphs.
    fn transform(&self, expr: &mut RelationExpr, _metadata: &RelationType) {
        Deduplicate::deduplicate(expr);
    }
}

// =============================================================================

/// The normalize optimization.
#[derive(Debug)]
pub struct Normalize;

impl Normalize {
    /// Normalize the names of local bindings.
    pub fn normalize(expr: &mut RelationExpr) {
        let mut count: usize = 0;
        let mut names = HashMap::new();

        fn rename(expr: &mut RelationExpr, count: &mut usize, names: &mut HashMap<String, String>) {
            if let RelationExpr::Let { name, value, body } = expr {
                rename(value, count, names);

                *count += 1;
                let stale = std::mem::replace(name, format!("id-{}", count));
                names.insert(stale, name.clone());

                rename(body, count, names);
            } else if let RelationExpr::Get { name, .. } = expr {
                if let Some(n) = names.get(name) {
                    *name = n.clone();
                }
            } else {
                expr.visit1_mut(|e| rename(e, count, names));
            }
        }

        rename(expr, &mut count, &mut names);
    }
}

impl super::Transform for Normalize {
    /// Normalize names of local bindings.
    fn transform(&self, expr: &mut RelationExpr, _metadata: &RelationType) {
        Normalize::normalize(expr);
    }
}

// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use repr::Datum;

    fn trace(label: &str, expr: &RelationExpr) {
        println!(
            "{}\n{}\n{}\n{}",
            "━".repeat(80),
            label,
            "┈".repeat(80),
            expr.pretty()
        );
    }

    fn not_my_type() -> RelationType {
        RelationType::new(vec![])
    }

    fn n(i: i32) -> RelationExpr {
        RelationExpr::constant(vec![vec![Datum::Int32(i)]], not_my_type())
    }

    fn r<N>(n: N) -> RelationExpr
    where
        N: Into<String>,
    {
        RelationExpr::Get {
            name: n.into(),
            typ: not_my_type(),
        }
    }

    #[test]
    fn test_hoist() {
        let b = bind_local;
        let mut expr = b(
            "h",
            b(
                "d",
                b("b", b("a", n(1), n(2)), b("c", n(3), n(4))),
                b("f", b("e", n(5), n(6)), b("g", n(7), n(8))),
            ),
            b("i", n(9), b("j", n(10), n(11)).distinct().negate()),
        );

        trace("IN hoist", &expr);
        Hoist::hoist(&mut expr);
        trace("OUT hoist", &expr);

        assert_eq!(
            expr,
            b(
                "a",
                n(1),
                b(
                    "b",
                    n(2),
                    b(
                        "c",
                        n(3),
                        b(
                            "d",
                            n(4),
                            b(
                                "e",
                                n(5),
                                b(
                                    "f",
                                    n(6),
                                    b(
                                        "g",
                                        n(7),
                                        b(
                                            "h",
                                            n(8),
                                            b("i", n(9), b("j", n(10), n(11).distinct().negate()))
                                        )
                                    )
                                )
                            )
                        )
                    )
                )
            )
        );
    }

    #[test]
    fn test_unbind() {
        let b = bind_local;
        let mut expr = b(
            "a",
            b("b", n(1), r("b").union(r("b"))),
            r("a").union(b("a", n(2), r("a"))),
        );

        trace("IN unbind", &expr);
        Unbind::unbind(&mut expr);
        trace("OUT unbind", &expr);

        assert_eq!(expr, n(1).union(n(1)).union(n(2)))
    }

    fn extract_names(expr: &RelationExpr) -> (String, String) {
        if let RelationExpr::Let { name, body, .. } = expr {
            let n1 = name.clone();

            if let RelationExpr::Let { name, .. } = &**body {
                (n1, name.clone())
            } else {
                panic!("body of outermost expression expected to be local binding");
            }
        } else {
            panic!("outermost expression expected to be local binding");
        }
    }

    #[test]
    fn test_deduplicate() {
        let expr1 = n(1).negate().union(n(2));
        let expr2 = n(3).negate().union(n(4));
        let expr3 = expr1.clone().union(expr2.clone());
        let expr4 = expr3.clone().union(n(5).distinct()).threshold();
        let expr5 = expr3.clone().distinct().union(expr2.clone());
        let mut expr = expr4.union(expr5);

        trace("IN deduplicate", &expr);
        Deduplicate::deduplicate(&mut expr);
        trace("OUT deduplicate", &expr);

        let b = bind_local;
        let (n1, n2) = extract_names(&expr);

        let expected = r(n2.clone()).union(n(5).distinct()).threshold();
        let expected2 = r(n2.clone()).distinct().union(r(n1.clone()));
        let expected = expected.union(expected2);
        let expected2 = n(1).negate().union(n(2)).union(r(n1.clone()));
        let expected = b(n2, expected2, expected);
        let expected2 = n(3).negate().union(n(4));
        let expected = b(n1, expected2, expected);
        assert_eq!(expr, expected);
    }

    #[test]
    fn test_normalize() {
        let expr1 = n(1).negate().union(n(2));
        let expr2 = n(3).negate().union(n(4));
        let expr3 = expr1.clone().union(expr2.clone());
        let expr4 = expr3.clone().union(n(5).distinct()).threshold();
        let expr5 = expr3.clone().distinct().union(expr2.clone());
        let mut expr = expr4.union(expr5);

        trace("IN normalize", &expr);
        Deduplicate::deduplicate(&mut expr);
        Normalize::normalize(&mut expr);
        trace("OUT normalize", &expr);

        let b = bind_local;
        let expected = r("id-2").union(n(5).distinct()).threshold();
        let expected2 = r("id-2").distinct().union(r("id-1"));
        let expected = expected.union(expected2);
        let expected2 = n(1).negate().union(n(2)).union(r("id-1"));
        let expected = b("id-2", expected2, expected);
        let expected2 = n(3).negate().union(n(4));
        let expected = b("id-1", expected2, expected);
        assert_eq!(expr, expected);
    }
}
