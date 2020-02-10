// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

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

use std::collections::HashMap;

use indexmap::IndexMap;

use repr::RelationType;

use crate::{EvalEnv, GlobalId, Id, LocalId, RelationExpr, ScalarExpr};

// -----------------------------------------------------------------------------

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
pub fn bind_local(id: LocalId, value: RelationExpr, body: RelationExpr) -> RelationExpr {
    RelationExpr::Let {
        id,
        value: Box::new(value),
        body: Box::new(body),
    }
}

// -----------------------------------------------------------------------------

/// A local binding that is automatically removed from its environment again.
#[derive(Debug)]
pub struct LocalBinding<'a> {
    env: &'a mut Environment,
    id: LocalId,
    prior: Option<RelationExpr>,
}

impl Drop for LocalBinding<'_> {
    /// Drop this local binding by restoring the environment to its prior state.
    fn drop(&mut self) {
        if let Some(value) = self.prior.take() {
            self.env.bindings.insert(self.id, value);
        } else {
            self.env.bindings.pop();
        }
    }
}

/// An environment.
///
/// Environments track bindings from names to values in insertion order.
/// In contrast to [`RelationExpr::Let`], environment bindings have no
/// body. Nonetheless, their scope is implicit in the ordering, namely all
/// subsequent bindings.
#[derive(Debug, Default)]
pub struct Environment {
    bindings: IndexMap<LocalId, RelationExpr>,
    id: u64,
}

impl Environment {
    /// Determine whether the name is bound in this environment.
    pub fn is_bound(&self, id: LocalId) -> bool {
        self.bindings.contains_key(&id)
    }

    /// Add a new binding for the given ID and value to this environment.
    /// This method panics if this environment already contains a binding
    /// with the given name. See also [`Environment::bind_local`].
    pub fn bind(&mut self, id: LocalId, value: RelationExpr) -> &mut Self {
        if self.bindings.contains_key(&id) {
            panic!("environment already contains binding for {}", id);
        }
        self.bindings.insert(id, value);
        self
    }

    /// Add a new, *local* binding for the given name and value to this
    /// environment. This method does support shadowed identifiers but
    /// the binding only persists as long as the returned object. See also
    /// [`Environment::bind`].
    pub fn bind_local(&mut self, id: LocalId, value: RelationExpr) -> LocalBinding {
        let prior = self.bindings.insert(id, value);
        LocalBinding {
            env: self,
            id,
            prior,
        }
    }

    /// Look up the given name in this environment.
    pub fn lookup(&self, id: LocalId) -> Option<&RelationExpr> {
        self.bindings.get(&id)
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
        *expr = self.use_in(expr.take_dangerous());
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

        fn extract_all(expr: &mut RelationExpr, env: &mut Environment) {
            // NB: visit1_mut() invokes the callback on the children of a
            // RelationExpr. That is not good enough for RelationExpr::Let since
            // the value and body might just be RelationExpr::Let's, too. Hence
            // we recurse on extract().
            if let RelationExpr::Let { .. } = expr {
                if let RelationExpr::Let {
                    id,
                    mut value,
                    body,
                } = expr.take_dangerous()
                {
                    extract_all(&mut *value, env);
                    env.bind(id, *value);
                    *expr = *body;
                    // By the magic vested in extract1(), expr now is let's body.
                    extract_all(expr, env);
                } else {
                    unreachable!();
                }
            } else {
                expr.visit1_mut(|e| extract_all(e, env));
            }
        }

        extract_all(expr, &mut env);
        env.inject(expr);
    }
}

impl super::Transform for Hoist {
    /// Hoist all bindings to the top of a dataflow graph.
    fn transform(
        &self,
        expr: &mut RelationExpr,
        _: &HashMap<GlobalId, Vec<Vec<ScalarExpr>>>,
        _: &EvalEnv,
    ) {
        Hoist::hoist(expr);
    }
}

// =============================================================================

#[derive(Debug)]
pub struct Unbind;

impl Unbind {
    /// Eliminate all let expressions by replacing get expressions with the
    /// bound value. This optimization correctly handles shadowed bindings.
    /// Since it eliminates all local bindings, it also eliminates local
    /// bindings that are referenced only once and thus superfluous. It is
    /// highly recommend to perform the [`Deduplicate`] optimization next.
    /// Since all bindings have been eliminated, `Deduplicate` can correctly
    /// identify all actually shared subgraphs and introduce bindings for them.
    ///
    /// `Unbind` has worst-case exponential space requirements. The alternative
    /// is to compute a fixed-point of `UnbindTrivial` followed by
    /// `Deduplicate`. The not yet implemented `UnbindTrivial` optimization
    /// eliminates only let expressions that bind get expressions. In other
    /// words, it is the equivalent of alpha-renaming in the lambda calculus.
    pub fn unbind(expr: &mut RelationExpr) {
        let mut env = Environment::default();

        fn unbind_all(expr: &mut RelationExpr, env: &mut Environment) {
            match expr {
                RelationExpr::Let { .. } => {
                    if let RelationExpr::Let {
                        id,
                        mut value,
                        body,
                    } = expr.take_dangerous()
                    {
                        unbind_all(&mut value, env);

                        let local = env.bind_local(id, *value);
                        *expr = *body;
                        unbind_all(expr, local.env);
                    } else {
                        unreachable!();
                    }
                }
                RelationExpr::Get {
                    id: Id::Local(id), ..
                } => {
                    if let Some(value) = env.lookup(*id) {
                        *expr = value.clone();
                    }
                }
                _ => expr.visit1_mut(|e| unbind_all(e, env)),
            }
        }

        unbind_all(expr, &mut env);
    }
}

impl super::Transform for Unbind {
    /// Perform the unbind optimization.
    fn transform(
        &self,
        expr: &mut RelationExpr,
        _: &HashMap<GlobalId, Vec<Vec<ScalarExpr>>>,
        _: &EvalEnv,
    ) {
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
    pub fn incr(&mut self) {
        self.count += 1;
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
    id: LocalId,
    typ: RelationType,
}

impl From<Patch> for RelationExpr {
    /// Convert a patch into a [`RelationExpr::Get`].
    fn from(patch: Patch) -> Self {
        RelationExpr::Get {
            id: Id::Local(patch.id),
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
    pub fn start_patching(&mut self, value: &RelationExpr, id: LocalId) -> RelationExpr {
        if let Metadata::Counting(Count { count }) = self {
            if *count < 2 {
                panic!("trying to patch dataflow node that isn't duplicated");
            }

            let patch = Patch {
                id,
                typ: value.typ(),
            };
            let reference = patch.clone().into();

            *self = Metadata::Patching(patch);
            reference
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
        let metadata = census
            .entry(expr)
            .or_insert_with(Metadata::default)
            .unwrap_count();
        metadata.incr();
        if !metadata.is_repeated() {
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
                let id = LocalId::new(env.id);
                env.id += 1;
                let reference = metadata.start_patching(expr, id);
                expr.visit1_mut(|e| Deduplicate::patch_all(e, census, env));
                let value = std::mem::replace(expr, reference);
                env.bind(id, value);
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
    fn transform(
        &self,
        expr: &mut RelationExpr,
        _: &HashMap<GlobalId, Vec<Vec<ScalarExpr>>>,
        _: &EvalEnv,
    ) {
        Deduplicate::deduplicate(expr);
    }
}

#[cfg(test)]
mod tests {
    use repr::{ColumnType, Datum, ScalarType};

    use super::*;

    fn trace(label: &str, expr: &RelationExpr) {
        println!(
            "{}\n{}\n{}\n{}",
            "━".repeat(80),
            label,
            "┈".repeat(80),
            expr.pretty()
        );
    }

    fn n(i: i32) -> RelationExpr {
        RelationExpr::constant(
            vec![vec![Datum::Int32(i)]],
            RelationType::new(vec![ColumnType::new(ScalarType::Int32)]),
        )
    }

    fn r(id: char, t: RelationType) -> RelationExpr {
        RelationExpr::Get {
            id: Id::Local(LocalId::new(id as u64)),
            typ: t,
        }
    }

    pub fn force_bind(id: char, value: RelationExpr, body: RelationExpr) -> RelationExpr {
        bind_local(LocalId::new(id as u64), value, body)
    }

    #[test]
    fn test_hoist() {
        let b = force_bind;
        let mut expr = b(
            'h',
            b(
                'd',
                b('b', b('a', n(1), n(2)), b('c', n(3), n(4))),
                b('f', b('e', n(5), n(6)), b('g', n(7), n(8))),
            ),
            b('i', n(9), b('j', n(10), n(11)).distinct().negate()),
        );

        trace("IN hoist", &expr);
        Hoist::hoist(&mut expr);
        trace("OUT hoist", &expr);

        assert_eq!(
            expr,
            b(
                'a',
                n(1),
                b(
                    'b',
                    n(2),
                    b(
                        'c',
                        n(3),
                        b(
                            'd',
                            n(4),
                            b(
                                'e',
                                n(5),
                                b(
                                    'f',
                                    n(6),
                                    b(
                                        'g',
                                        n(7),
                                        b(
                                            'h',
                                            n(8),
                                            b('i', n(9), b('j', n(10), n(11).distinct().negate()))
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
        let b = force_bind;
        let a = b('b', n(1), r('b', n(1).typ()).union(r('b', n(1).typ())));
        let mut expr = b(
            'a',
            a.clone(),
            r('a', a.typ()).union(b('a', n(2), r('a', a.typ()))),
        );

        trace("IN unbind", &expr);
        Unbind::unbind(&mut expr);
        trace("OUT unbind", &expr);

        assert_eq!(expr, n(1).union(n(1)).union(n(2)))
    }

    fn extract_ids(expr: &RelationExpr) -> (char, RelationType, char, RelationType) {
        if let RelationExpr::Let {
            id: id1,
            value: value1,
            body,
        } = expr
        {
            if let RelationExpr::Let {
                id: id2,
                value: value2,
                ..
            } = &**body
            {
                (id1.into(), value1.typ(), id2.into(), value2.typ())
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
        let expr3 = expr1.union(expr2.clone());
        let expr4 = expr3.clone().union(n(5).distinct()).threshold();
        let expr5 = expr3.distinct().union(expr2);
        let mut expr = expr4.union(expr5);

        trace("IN deduplicate", &expr);
        Deduplicate::deduplicate(&mut expr);
        trace("OUT deduplicate", &expr);

        let b = force_bind;
        let (id1, t1, id2, t2) = extract_ids(&expr);

        let expected = r(id2.clone(), t2.clone())
            .union(n(5).distinct())
            .threshold();
        let expected2 = r(id2.clone(), t2)
            .distinct()
            .union(r(id1.clone(), t1.clone()));
        let expected = expected.union(expected2);
        let expected2 = n(1).negate().union(n(2)).union(r(id1.clone(), t1));
        let expected = b(id2, expected2, expected);
        let expected2 = n(3).negate().union(n(4));
        let expected = b(id1, expected2, expected);
        assert_eq!(expr, expected);
    }
}
