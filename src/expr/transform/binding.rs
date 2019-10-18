// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

//! # Optimizing Bindings
//!
//! This module provides optimizations for bindings, i.e., [`RelationExpr::Let`]
//! and [`RelationExpr::Get`] operators. The former associate the *value*
//! relation with a symbolic name that can be referenced in the *body* relation
//! through the latter. While eminently useful, bindings violate referential
//! transparency and thereby complicate the analysis, optimization, and
//! execution of dataflow graphs. Fundamentally, that is because __Let__ and
//! __Get__ introduce non-local dependencies, i.e., all __Get__ operators for
//! some `id` in the body of the closest __Let__ for `id` depend on that binding
//! and thereby its value. Any correct transformation of a dataflow must
//! preserve these dependencies.
//!
//! Notably, this module provides:
//!
//!   * [`IdGen`] for generating fresh identifiers. The struct relies on a
//!     counter so that identifier names are deterministic, which matters
//!     both for testing and for fixed-points.
//!   * [`Environment`] for storing the name, value pairs of bindings while
//!     processing dataflow graphs. Bindings are stored in an [`IndexMap`] to
//!     preserve their order and thus their dependencies, while also supporting
//!     speedy lookup by name.
//!   * [`Unbind`] for optimizing a dataflow graph by eliminating unnecessary
//!     bindings. That includes eliminating bindings that are not references,
//!     inlining bindings that are referenced once, and folding bindings to
//!     references.

use crate::RelationExpr;
use indexmap::IndexMap;
use repr::RelationType;
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

// =======================================================================================

/// The type of identifiers.
pub type Identifier = String;

/// A deterministic source of fresh identifiers.
#[derive(Debug)]
pub struct IdGen<'pre> {
    prefix: &'pre str,
    count: usize,
}

impl<'pre> IdGen<'pre> {
    /// Create a new identifier generator.
    pub fn new() -> Self {
        Self {
            prefix: "tmp",
            count: 0,
        }
    }

    /// Create a new identifier generator with the given prefix.
    pub fn with_prefix(prefix: &'pre str) -> Self {
        Self { prefix, count: 0 }
    }

    /// Create a new identifier generator with the given prefix and count.
    pub fn with_prefix_and_count(prefix: &'pre str, count: usize) -> Self {
        Self { prefix, count }
    }

    /// Create a new identifier.
    pub fn fresh_id(&mut self) -> Identifier {
        self.count += 1;
        format!("{}-{}", self.prefix, self.count)
    }
}

impl Default for IdGen<'_> {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------------------

/// A local binding that is automatically removed from its environment again.
#[derive(Debug)]
pub struct LocalBinding<'a> {
    env: &'a mut Environment,
    name: String,
    prior: Option<RelationExpr>,
}

impl AsRef<Environment> for LocalBinding<'_> {
    fn as_ref(&self) -> &Environment {
        self.env
    }
}

impl AsMut<Environment> for LocalBinding<'_> {
    fn as_mut(&mut self) -> &mut Environment {
        self.env
    }
}

impl Drop for LocalBinding<'_> {
    /// Drop this local binding by restoring the environment to its prior state.
    fn drop(&mut self) {
        if let Some(value) = self.prior.take() {
            let name = std::mem::replace(&mut self.name, String::new());
            self.env.bindings.insert(name, value);
        } else {
            self.env.bindings.pop();
        }
    }
}

/// An environment.
#[derive(Debug, Default)]
pub struct Environment {
    bindings: IndexMap<Identifier, RelationExpr>,
}

impl Environment {
    /// Create a new environment.
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a new binding for the given name and value to this environment.
    /// This method panics if this environment already contains a binding
    /// with the given name.
    pub fn bind(&mut self, name: Identifier, value: RelationExpr) {
        if self.bindings.contains_key(&name) {
            panic!("environment already contains binding for {}", name);
        }
        self.bindings.insert(name, value);
    }

    /// Add a new, scoped binding for the given name and value to this
    /// environment. This method correctly handles shadowed identifiers but
    /// to do so it bindings only persist as long as the returned guard object.
    pub fn bind_local(&mut self, name: Identifier, value: RelationExpr) -> LocalBinding {
        let prior = self.bindings.insert(name.clone(), value);
        LocalBinding {
            env: self,
            name,
            prior,
        }
    }

    /// Determine whether the name is bound in this environment.
    #[allow(clippy::ptr_arg)]
    pub fn is_bound(&self, name: &Identifier) -> bool {
        self.bindings.contains_key(name)
    }

    /// Look up the given name in this environment.
    #[allow(clippy::ptr_arg)]
    pub fn lookup(&self, name: &Identifier) -> Option<&RelationExpr> {
        self.bindings.get(name)
    }

    /// Use this environment's bindings in the given expression. This method
    /// folds the bindings from the right by creating [`RelationExpr::Let`]
    /// instances. The given expression serves as initial value for the
    /// accumulation.
    pub fn use_in(self, body: RelationExpr) -> RelationExpr {
        self.bindings
            .into_iter()
            .rev()
            .fold(body, |body, (name, value)| RelationExpr::Let {
                name,
                value: Box::new(value),
                body: Box::new(body),
            })
    }

    /// Inject this environment's bindings into the given dataflow graph. This
    /// method is the imperative version of [`Environment::use_in`].
    pub fn inject(self, expr: &mut RelationExpr) {
        *expr = self.use_in(expr.take_dangerous());
    }
}

// =======================================================================================

/// # Unbind
///
/// Optimize a relation expression by eliminating superfluous bindings:
///
///  1. Remove `Let` expressions without corresponding `Get`.
///  2. Eliminate indirection for `Let` expressions binding a `Get`.
///  3. Inline value of `Let` expressions with a single `Get`.
///
/// This optimization may generate new opportunities for itself. Notably, the
/// elimination of indirections may create new opportunities for inlining. For
/// this reason, it should be invoked twice to yield a fixed point.
///
/// # Panics
///
/// This optimization panics if the dataflow relation contains more than one
/// binding for the same identifier. It does not matter whether those bindings
/// are independent from each other, i.e., in distinct subgraphs, or nested
/// within each other, i.e., one shadowing the other.
#[derive(Debug)]
pub struct Unbind;

impl Unbind {
    /// Unbind unnecessary bindings.
    pub fn unbind(relation: &mut RelationExpr) {
        struct Metadata {
            count: usize,
            name: Identifier,
            value: Option<RelationExpr>,
        }

        fn inspect(
            relation: &RelationExpr,
            census: &mut HashMap<Identifier, Rc<RefCell<Metadata>>>,
        ) {
            if let RelationExpr::Let { name, value, .. } = relation {
                if census.contains_key(name) {
                    panic!("duplicate binding for \"{}\"", name)
                } else if let RelationExpr::Get { name: other, .. } = &**value {
                    census.insert(name.clone(), census.get(other).unwrap().clone());
                } else {
                    census.insert(
                        name.clone(),
                        Rc::new(RefCell::new(Metadata {
                            count: 0,
                            name: name.clone(),
                            value: None,
                        })),
                    );
                }
            } else if let RelationExpr::Get { name, .. } = relation {
                if census.contains_key(name) {
                    let mut metadata = census.get(name).unwrap().borrow_mut();
                    metadata.count += 1;
                }
            }

            relation.visit1(|expr| inspect(expr, census));
        }

        fn update(
            relation: &mut RelationExpr,
            census: &mut HashMap<Identifier, Rc<RefCell<Metadata>>>,
        ) {
            if let RelationExpr::Let { name, .. } = relation {
                let (count, is_alias) = {
                    let metadata = census.get(name).unwrap().borrow();
                    (metadata.count, metadata.name != *name)
                };

                if count <= 1 || is_alias {
                    if let RelationExpr::Let {
                        name,
                        mut value,
                        body,
                    } = relation.take_dangerous()
                    {
                        update(&mut value, census);
                        if count == 1 && !is_alias {
                            census.get(&name).unwrap().borrow_mut().value = Some(*value);
                        }
                        *relation = *body; // Makes up for take_dangerous!
                        update(relation, census);
                    } else {
                        unreachable!();
                    }
                } else {
                    relation.visit1_mut(|expr| update(expr, census));
                }
            } else if let RelationExpr::Get { name, .. } = relation {
                if census.contains_key(name) {
                    let mut metadata = census.get(name).unwrap().borrow_mut();

                    // NB: if's are *not* mutually exclusive, but ordered.
                    if *name != metadata.name {
                        *name = metadata.name.clone();
                    }
                    if let Some(value) = metadata.value.take() {
                        *relation = value;
                    }
                }
            } else {
                relation.visit1_mut(|expr| update(expr, census));
            }
        }

        let mut census = HashMap::new();
        inspect(relation, &mut census);
        update(relation, &mut census);
    }
}

impl super::Transform for Unbind {
    fn transform(&self, relation: &mut RelationExpr) {
        Unbind::unbind(relation);
    }
}

// =======================================================================================

mod dedup_helper {
    use super::{IdGen, Identifier, RelationExpr, RelationType};

    /// The metadata associated with a relation expression during deduplication.
    /// If it has some relation type, then it also must have some identifier.
    /// The existence of a relation type indicates that the binding for a
    /// repeated relation expression has been emitted.
    #[derive(Debug)]
    pub(super) struct Metadata {
        /// The number of times the corresponding relation expression appears in
        /// the larger dataflow.
        count: usize,
        /// The name when binding a repeated relation expression.
        name: Option<Identifier>,
        /// The type when binding a repeated relation expression.
        typ: Option<RelationType>,
    }

    impl Metadata {
        /// Create a new metadata record.
        pub fn new() -> Self {
            Self {
                count: 0,
                name: None,
                typ: None,
            }
        }

        /// Increment the count for this metadata record.
        pub fn incr(&mut self) {
            self.count += 1;
        }

        /// Determine whether the relation expression occurs more than once.
        pub fn is_repeated(&self) -> bool {
            self.count >= 2
        }

        /// Set the name if it has not yet been set.
        pub fn maybe_set_name<F>(&mut self, name: F)
        where
            F: FnOnce() -> Identifier,
        {
            if self.name.is_none() {
                self.name = Some(name());
            }
        }

        /// Determine whether the relation expression has a relation type.
        pub fn has_type(&self) -> bool {
            self.typ.is_some()
        }

        /// Set the relation type. This method panics if the type has already
        /// been set.
        pub fn set_type(&mut self, relation: &RelationExpr) {
            match self.typ {
                None => self.typ = Some(relation.typ()),
                Some(_) => panic!("already set type for {}", relation.pretty()),
            }
        }

        /// Prepare the metadata for emitting a binding and corresponding
        /// references. This method sets the type to that of the given relation
        /// expression and the name if it is not already set.
        pub fn prepare_binding(
            &mut self,
            id_gen: &mut IdGen,
            relation: &RelationExpr,
        ) -> (Identifier, RelationExpr) {
            self.maybe_set_name(|| id_gen.fresh_id());
            self.set_type(relation);
            (self.name.clone().unwrap(), self.new_reference())
        }

        /// Create a new __Get__ operator for the named binding.
        pub fn new_reference(&self) -> RelationExpr {
            RelationExpr::Get {
                name: self.name.clone().unwrap(),
                typ: self.typ.clone().unwrap(),
            }
        }
    }
}

/// # Deduplicate
///
/// This optimization eliminates duplicate or repeated relation expressions
/// from a larger dataflow graph by capturing the value on its first occurrence
/// in a __Let__ binding and replacing later occurrences with __Get__ operators.
/// The optimization correctly picks the largest common subexpression for
/// deduplication. It also preserves the identifiers of existing bindings, even
/// if the same expression appears first unbound and later again as the value of
/// a __Let__ binding.
///
/// However, to ensure that newly introduced __Get__ operators are in scope of
/// their bindings, this optimization also hoists bindings to the top of the
/// dataflow graph, i.e., closest to the output. For correctness, it does that
/// for newly created bindings and existing bindings alike. In other words, in
/// the absence of duplicated expressions, this optimization is equivalent to a
/// hoist optimization by itself.
///
/// This optimization may create new opportunities for [`Unbind`] (also defined
/// in this module).
#[derive(Debug)]
pub struct Deduplicate;

impl Deduplicate {
    pub fn deduplicate(relation: &mut RelationExpr) {
        use dedup_helper::Metadata;

        /// Determine whether the relation is a __Let__ expression. Correctly
        /// processing such expressions during Deduplicate is quite a bit more
        /// complicated than that for other relation expressions. That includes
        /// how __Let__ expressions are traversed, hence this helper function.
        fn is_let_relation(relation: &RelationExpr) -> bool {
            match relation {
                RelationExpr::Let { .. } => true,
                _ => false,
            }
        }

        fn inspect<'rel>(
            relation: &'rel RelationExpr,
            census: &mut HashMap<&'rel RelationExpr, Metadata>,
        ) {
            match relation {
                RelationExpr::Constant { .. } | RelationExpr::Get { .. } => {
                    // Dolce far niente!
                }
                RelationExpr::Let { name, value, body } => {
                    let metadata = census.entry(value).or_insert_with(Metadata::new);
                    // Record the name (unless we have already done so). This
                    // lets us reuse existing names instead of obscuring them
                    // with freshly created ones.
                    metadata.maybe_set_name(|| name.clone());
                    metadata.incr();
                    if !metadata.is_repeated() {
                        // Don't count Lets. Also don't double-count value.
                        if is_let_relation(value) {
                            inspect(value, census);
                        } else {
                            value.visit1(|expr| inspect(expr, census));
                        }
                    }

                    // Independent of type, we still need to account for body.
                    inspect(body, census);
                }
                _ => {
                    let metadata = census.entry(relation).or_insert_with(Metadata::new);
                    metadata.incr();
                    if !metadata.is_repeated() {
                        relation.visit1(|expr| inspect(expr, census));
                    }
                }
            }
        }

        fn update(
            relation: &mut RelationExpr,
            census: &mut HashMap<&RelationExpr, Metadata>,
            id_gen: &mut IdGen,
            env: &mut Environment,
        ) {
            match relation {
                RelationExpr::Constant { .. } | RelationExpr::Get { .. } => {
                    // Dolce far niente!
                }
                RelationExpr::Let { .. } => {
                    // The dangerously taken value is replaced at the very end of this block.
                    if let RelationExpr::Let {
                        name,
                        mut value,
                        mut body,
                    } = relation.take_dangerous()
                    {
                        let metadata = census.get_mut(&*value).unwrap();
                        if !metadata.is_repeated() {
                            // >>> A pre-existing binding. Hoist it. <<<
                            if is_let_relation(&*value) {
                                // Correctly handle value being another Let.
                                update(&mut *value, census, id_gen, env);
                            } else {
                                value.visit1_mut(|expr| update(expr, census, id_gen, env));
                            }
                            env.bind(name, *value);
                        } else if !metadata.has_type() {
                            // >>> A binding with duplicated value. Hoist it & reuse it. <<<
                            metadata.set_type(&*value);
                            if is_let_relation(&*value) {
                                // Correctly handle value being another Let.
                                update(&mut *value, census, id_gen, env);
                            } else {
                                value.visit1_mut(|expr| update(expr, census, id_gen, env));
                            }
                            env.bind(name, *value);
                        } else {
                            // >>> A binding with duplicated value. Hoisted already. <<<
                        }

                        // Process body. Replace Let with body.
                        update(&mut *body, census, id_gen, env);
                        *relation = *body; // Fixes take_dangerous() above!
                    } else {
                        unreachable!();
                    }
                }
                _ => {
                    let metadata = census.get_mut(relation).unwrap();
                    if !metadata.is_repeated() {
                        relation.visit1_mut(|expr| update(expr, census, id_gen, env));
                    } else if !metadata.has_type() {
                        let (name, reference) = metadata.prepare_binding(id_gen, relation);
                        relation.visit1_mut(|expr| update(expr, census, id_gen, env));
                        let value = std::mem::replace(relation, reference);
                        env.bind(name, value);
                    } else {
                        *relation = metadata.new_reference();
                    }
                }
            }
        }

        let mut census = HashMap::new();
        let relation_prime = relation.clone();
        inspect(&relation_prime, &mut census);

        let mut id_gen = IdGen::new();
        let mut env = Environment::new();
        update(relation, &mut census, &mut id_gen, &mut env);
        env.inject(relation);
    }
}

impl super::Transform for Deduplicate {
    fn transform(&self, relation: &mut RelationExpr) {
        Deduplicate::deduplicate(relation);
    }
}

// =======================================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use repr::Datum;

    /// Trace a label and relation expression.
    pub fn trace(label: &str, expr: &RelationExpr) {
        // Emit all text with a single println!() because otherwise the test
        // harness may interleave output of different trace() invocations.
        println!(
            "{}\n{}\n{}\n{}\n",
            "━".repeat(80),
            label,
            "┈".repeat(80),
            expr.pretty()
        );
    }

    /// Fix the types of [`RelationExpr::Get`] operators in the given relation
    /// expression. This function patches a dataflow graph created via the
    /// helper functions [`i`], [`b`], and [`r`] below to use correct types.
    /// In turn, that ensures that assertions on transformed relations do not
    /// fail because of incorrect types.
    pub fn retype(relation: RelationExpr) -> RelationExpr {
        fn do_retype(relation: &mut RelationExpr, env: &mut Environment) {
            match relation {
                RelationExpr::Let { name, value, body } => {
                    do_retype(value, env);

                    let mut local = env.bind_local(name.clone(), *value.clone());
                    do_retype(body, local.as_mut());
                }
                RelationExpr::Get { name, typ } => match env.lookup(name) {
                    Some(value) => *typ = value.typ(),
                    None => (),
                },
                _ => {
                    relation.visit1_mut(|expr| {
                        do_retype(expr, env);
                    });
                }
            }
        }

        let mut relation = relation;
        let mut env = Environment::new();
        do_retype(&mut relation, &mut env);
        relation
    }

    /// Create an *Integer* constant.
    pub fn i(i: i32) -> RelationExpr {
        RelationExpr::constant(vec![vec![Datum::Int32(i)]], RelationType::new(vec![]))
    }

    /// Create a *Binding* for the given name and value.
    pub fn b<ID>(name: ID, value: RelationExpr, body: RelationExpr) -> RelationExpr
    where
        ID: Into<Identifier>,
    {
        RelationExpr::Let {
            name: name.into(),
            value: Box::new(value),
            body: Box::new(body),
        }
    }

    /// Create a *Reference* a binding.
    pub fn r<ID>(name: ID) -> RelationExpr
    where
        ID: Into<Identifier>,
    {
        RelationExpr::Get {
            name: name.into(),
            typ: RelationType::new(vec![]),
        }
    }

    // -----------------------------------------------------------------------------------

    #[test]
    fn test_unbind1() {
        let mut expr = b(
            "id-1",
            i(665),
            b(
                "id-2",
                r("id-1"),
                b("id-3", r("id-2"), r("id-3").union(r("id-3"))),
            ),
        );

        trace("IN unbind #1", &expr);
        Unbind::unbind(&mut expr);
        trace("OUT unbind #1", &expr);
        assert_eq!(expr, b("id-1", i(665), r("id-1").union(r("id-1"))));
    }

    #[test]
    fn test_unbind2() {
        let mut expr = b(
            "id-1",
            i(665),
            b("id-2", r("id-1"), b("id-3", r("id-2"), r("id-3"))),
        );

        trace("IN unbind #2", &expr);
        Unbind::unbind(&mut expr);
        trace("OUT unbind #2, round 1", &expr);
        //assert_eq!(e2, b("id-1", i(655), r("id-1")));

        Unbind::unbind(&mut expr);
        trace("OUT unbind #2, round 2", &expr);
        assert_eq!(expr, i(665));
    }

    #[test]
    fn test_unbind3() {
        let mut expr = b("id-2", i(42), b("id-1", i(13), i(665)));

        trace("IN unbind #3", &expr);
        Unbind::unbind(&mut expr);
        trace("OUT unbind #3", &expr);
        assert_eq!(expr, i(665));
    }

    // -----------------------------------------------------------------------------------

    #[test]
    fn test_deduplicate1() {
        // This test was originally written for the Hoist optimization. That
        // optimization no longer exists, since hoisting of existing bindings
        // cannot be separated from hoisting newly introduced bindings during
        // Deduplicate. Hence the new and improved version of Deduplicate
        // integrates Hoist and the test now exercises Deduplicate.
        let mut expr = b(
            "h",
            b(
                "d",
                b("b", b("a", i(1), i(2)), b("c", i(3), i(4))),
                b("f", b("e", i(5), i(6)), b("g", i(7), i(8))),
            ),
            b("i", i(9), b("j", i(10), i(11)).distinct().negate()),
        );

        trace("IN deduplicate #1", &expr);
        Deduplicate::deduplicate(&mut expr);
        trace("OUT deduplicate #1", &expr);
        assert_eq!(
            expr,
            b(
                "a",
                i(1),
                b(
                    "b",
                    i(2),
                    b(
                        "c",
                        i(3),
                        b(
                            "d",
                            i(4),
                            b(
                                "e",
                                i(5),
                                b(
                                    "f",
                                    i(6),
                                    b(
                                        "g",
                                        i(7),
                                        b(
                                            "h",
                                            i(8),
                                            b("i", i(9), b("j", i(10), i(11).distinct().negate()))
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
    fn test_deduplicate2() {
        let mut expr = retype(b(
            "preexisting",
            i(665).distinct().negate(),
            r("preexisting").union(i(665).distinct().negate().threshold()),
        ));

        trace("IN deduplicate #2", &expr);
        Deduplicate::deduplicate(&mut expr);
        trace("OUT deduplicate #2", &expr);
        assert_eq!(
            expr,
            retype(b(
                "preexisting",
                i(665).distinct().negate(),
                r("preexisting").union(r("preexisting").threshold())
            ))
        );
    }

    #[test]
    fn test_deduplicate3() {
        let mut expr = retype(i(665).distinct().negate().union(b(
            "preexisting",
            i(665).distinct().negate(),
            r("preexisting").threshold(),
        )));

        trace("IN deduplicate #3", &expr);
        Deduplicate::deduplicate(&mut expr);
        trace("OUT deduplicate #3", &expr);
        assert_eq!(
            expr,
            retype(b(
                "preexisting",
                i(665).distinct().negate(),
                r("preexisting").union(r("preexisting").threshold())
            ))
        );
    }

    #[test]
    fn test_deduplicate4() {
        let expr1 = i(1).negate().union(i(2));
        let expr2 = i(3).negate().union(i(4));
        let expr3 = expr1.clone().union(expr2.clone());
        let expr4 = expr3.clone().union(i(5).distinct()).threshold();
        let expr5 = expr3.clone().distinct().union(expr2.clone());
        let mut expr = retype(expr4.union(expr5));

        trace("IN deduplicate #4", &expr);
        Deduplicate::deduplicate(&mut expr);
        trace("OUT deduplicate #4", &expr);

        let expected = r("tmp-1").union(i(5).distinct()).threshold();
        let expected2 = r("tmp-1").distinct().union(r("tmp-2"));
        let expected = expected.union(expected2);
        let expected2 = i(1).negate().union(i(2)).union(r("tmp-2"));
        let expected = b("tmp-1", expected2, expected);
        let expected2 = i(3).negate().union(i(4));
        let expected = retype(b("tmp-2", expected2, expected));
        assert_eq!(expr, expected);
    }
}
