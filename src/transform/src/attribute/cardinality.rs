// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Definition and helper structs for the [`Cardinality`] attribute.

use std::collections::{BTreeMap, BTreeSet};

use mz_expr::{
    BinaryFunc, Id, JoinImplementation, MirRelationExpr, MirScalarExpr, TableFunc, UnaryFunc,
    VariadicFunc,
};
use mz_ore::cast::CastLossy;
use mz_repr::GlobalId;

use mz_repr::explain::ExprHumanizer;
use ordered_float::OrderedFloat;

use crate::attribute::subtree_size::SubtreeSize;
use crate::attribute::unique_keys::UniqueKeys;
use crate::attribute::{Attribute, DerivedAttributes, Env, RequiredAttributes};
use crate::symbolic::SymbolicExpression;

use super::Arity;

/// Compute the estimated cardinality of each subtree of a [MirRelationExpr] from the bottom up.
#[allow(missing_debug_implementations)]
pub struct Cardinality {
    /// Environment of computed values for this attribute
    env: Env<Self>,
    /// A vector of results for all nodes in the visited tree in
    /// post-visit order
    pub results: Vec<SymExp>,
    /// A factorizer for generating appropriating scaling factors
    pub factorize: Box<dyn Factorizer + Send + Sync>,
}

impl Default for Cardinality {
    fn default() -> Self {
        Cardinality {
            env: Env::default(),
            results: Vec::new(),
            factorize: Box::new(WorstCaseFactorizer {
                cardinalities: BTreeMap::new(),
            }),
        }
    }
}

/// The variables used in symbolic expressions representing cardinality
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum FactorizerVariable {
    /// The total cardinality of a given global id
    Id(GlobalId),
    /// The inverse of the number of distinct keys in the index held in the given column, i.e., 1/|# of distinct keys|
    ///
    /// TODO(mgree): need to correlate this back to a given global table, to feed in statistics (or have a sub-attribute for collecting this)
    Index(usize),
    /// An unbound local or other unknown quantity
    Unknown,
}

/// SymbolicExpressions specialized to factorizer variables
pub type SymExp = SymbolicExpression<FactorizerVariable>;

/// A `Factorizer` computes selectivity factors
pub trait Factorizer {
    /// Compute selectivity for the flat map of `tf`
    fn flat_map(&self, tf: &TableFunc, input: &SymExp) -> SymExp;
    /// Computes selectivity of the predicate `expr`, given that `unique_columns` are indexed/unique
    ///
    /// The result should be in the range (0, 1.0]
    fn predicate(&self, expr: &MirScalarExpr, unique_columns: &BTreeSet<usize>) -> SymExp;
    /// Computes selectivity for a filter
    fn filter(
        &self,
        predicates: &Vec<MirScalarExpr>,
        keys: &Vec<Vec<usize>>,
        input: &SymExp,
    ) -> SymExp;
    /// Computes selectivity for a join; the cardinality estimate for each input is paired with the keys on that input
    ///
    /// `unique_columns` maps column references (that are indexed/unique) to their relation's index in `inputs`
    fn join(
        &self,
        equivalences: &Vec<Vec<MirScalarExpr>>,
        implementation: &JoinImplementation,
        unique_columns: BTreeMap<usize, usize>,
        inputs: Vec<&SymExp>,
    ) -> SymExp;
    /// Computes selectivity for a reduce
    fn reduce(
        &self,
        group_key: &Vec<MirScalarExpr>,
        expected_group_size: &Option<u64>,
        input: &SymExp,
    ) -> SymExp;
    /// Computes selectivity for a topk
    fn topk(
        &self,
        group_key: &Vec<usize>,
        limit: &Option<usize>,
        expected_group_size: &Option<u64>,
        input: &SymExp,
    ) -> SymExp;
    /// Computes slectivity for a threshold
    fn threshold(&self, input: &SymExp) -> SymExp;
}

/// The simplest possible `Factorizer` that aims to generate worst-case, upper-bound cardinalities
#[derive(Debug)]
pub struct WorstCaseFactorizer {
    /// cardinalities for each `GlobalId` and its unique values
    pub cardinalities: BTreeMap<FactorizerVariable, usize>,
}

/// The default selectivity for predicates we know nothing about.
///
/// It is safe to use this instead of `FactorizerVariable::Index(col)`.
pub const WORST_CASE_SELECTIVITY: f64 = 0.1;

impl Factorizer for WorstCaseFactorizer {
    fn flat_map(&self, tf: &TableFunc, input: &SymExp) -> SymExp {
        match tf {
            TableFunc::Wrap { types, width } => {
                input * (f64::cast_lossy(types.len()) / f64::cast_lossy(*width))
            }
            _ => {
                // TODO(mgree) what explosion factor should we make up?
                input * &SymExp::from(4.0)
            }
        }
    }

    fn predicate(&self, expr: &MirScalarExpr, unique_columns: &BTreeSet<usize>) -> SymExp {
        let index_cardinality = |expr: &MirScalarExpr| -> Option<SymExp> {
            match expr {
                MirScalarExpr::Column(col) => {
                    if unique_columns.contains(col) {
                        Some(SymbolicExpression::symbolic(FactorizerVariable::Index(
                            *col,
                        )))
                    } else {
                        None
                    }
                }
                _ => None,
            }
        };

        match expr {
            MirScalarExpr::Column(_)
            | MirScalarExpr::Literal(_, _)
            | MirScalarExpr::CallUnmaterializable(_) => SymExp::from(1.0),
            MirScalarExpr::CallUnary { func, expr } => match func {
                UnaryFunc::Not(_) => 1.0 - self.predicate(expr, unique_columns),
                UnaryFunc::IsTrue(_) | UnaryFunc::IsFalse(_) => SymExp::from(0.5),
                UnaryFunc::IsNull(_) => {
                    if let Some(icard) = index_cardinality(expr) {
                        icard
                    } else {
                        SymExp::from(WORST_CASE_SELECTIVITY)
                    }
                }
                _ => SymExp::from(WORST_CASE_SELECTIVITY),
            },
            MirScalarExpr::CallBinary { func, expr1, expr2 } => {
                match func {
                    BinaryFunc::Eq => match (index_cardinality(expr1), index_cardinality(expr2)) {
                        (Some(icard1), Some(icard2)) => SymbolicExpression::max(icard1, icard2),
                        (Some(icard), None) | (None, Some(icard)) => icard,
                        (None, None) => SymExp::from(WORST_CASE_SELECTIVITY),
                    },
                    // 1.0 - the Eq case
                    BinaryFunc::NotEq => match (index_cardinality(expr1), index_cardinality(expr2))
                    {
                        (Some(icard1), Some(icard2)) => {
                            1.0 - SymbolicExpression::max(icard1, icard2)
                        }
                        (Some(icard), None) | (None, Some(icard)) => 1.0 - icard,
                        (None, None) => SymExp::from(1.0 - WORST_CASE_SELECTIVITY),
                    },
                    BinaryFunc::Lt | BinaryFunc::Lte | BinaryFunc::Gt | BinaryFunc::Gte => {
                        // TODO(mgree) if we have high/low key values and one of the columns is an index, we can do better
                        SymExp::from(0.33)
                    }
                    _ => SymExp::from(1.0), // TOOD(mgree): are there other interesting cases?
                }
            }
            MirScalarExpr::CallVariadic { func, exprs } => match func {
                VariadicFunc::And => {
                    // can't use SymExp::product because it expects a vector of references :/
                    let mut factor = SymExp::from(1.0);

                    for expr in exprs {
                        factor = factor * self.predicate(expr, unique_columns);
                    }

                    factor
                }
                VariadicFunc::Or => {
                    // TODO(mgree): BETWEEN will get compiled down to an OR of appropriate bounds---we could try to detect it and be clever

                    // F(expr1 OR expr2) = F(expr1) + F(expr2) - F(expr1) * F(expr2), but generalized
                    let mut exprs = exprs.into_iter();

                    let mut expr1;

                    if let Some(first) = exprs.next() {
                        expr1 = self.predicate(first, unique_columns);
                    } else {
                        return SymExp::from(1.0);
                    }

                    for expr2 in exprs {
                        let expr2 = self.predicate(expr2, unique_columns);

                        // TODO(mgree) a big expression! two things could help: hash-consing and simplification
                        expr1 = expr1.clone() + expr2.clone() - expr1.clone() * expr2;
                    }
                    expr1
                }
                _ => SymExp::from(1.0),
            },
            MirScalarExpr::If { cond: _, then, els } => SymExp::max(
                self.predicate(then, unique_columns),
                self.predicate(els, unique_columns),
            ),
        }
    }

    fn filter(
        &self,
        predicates: &Vec<MirScalarExpr>,
        keys: &Vec<Vec<usize>>,
        input: &SymExp,
    ) -> SymExp {
        // TODO(mgree): should we try to do something for indices built on multiple columns?
        let mut unique_columns = BTreeSet::new();
        for key in keys {
            if key.len() == 1 {
                unique_columns.insert(key[0]);
            }
        }

        // worst case scaling factor is 1
        let mut factor = SymExp::from(1.0);

        for expr in predicates {
            let predicate_scaling_factor = self.predicate(expr, &unique_columns);

            // constant scaling factors should be in (0,1]
            debug_assert!(match predicate_scaling_factor {
                SymExp::Constant(OrderedFloat(n)) => 0.0 < n && n <= 1.0,
                _ => true,
            });

            factor = factor * predicate_scaling_factor;
        }

        input.clone() * factor
    }

    fn join(
        &self,
        equivalences: &Vec<Vec<MirScalarExpr>>,
        _implementation: &JoinImplementation,
        unique_columns: BTreeMap<usize, usize>,
        inputs: Vec<&SymExp>,
    ) -> SymExp {
        let mut inputs = inputs.into_iter().cloned().collect::<Vec<_>>();

        for equiv in equivalences {
            // those sources which have a unique key
            let mut unique_sources = BTreeSet::new();
            let mut all_unique = true;

            for expr in equiv {
                if let MirScalarExpr::Column(col) = expr {
                    if let Some(idx) = unique_columns.get(col) {
                        unique_sources.insert(*idx);
                    } else {
                        all_unique = false;
                    }
                } else {
                    all_unique = false;
                }
            }

            // no unique columns in this equivalence
            if unique_sources.is_empty() {
                continue;
            }

            // ALL unique columns in this equivalence
            if all_unique {
                // these inputs have unique keys for _all_ of the equivalence, so they're a bound on how many rows we'll get from those sources
                // we'll find the leftmost such input and use it to hold the minimum; the other sources we set to 1.0 (so they have no effect)
                let mut sources = unique_sources.iter();

                let lhs_idx = *sources.next().unwrap();
                let mut lhs = std::mem::replace(&mut inputs[lhs_idx], SymExp::f64(1.0));
                for &rhs_idx in sources {
                    let rhs = std::mem::replace(&mut inputs[rhs_idx], SymExp::f64(1.0));
                    lhs = SymExp::min(lhs, rhs);
                }

                inputs[lhs_idx] = lhs;

                // best option! go look at the next equivalence
                continue;
            }

            // some unique columns in this equivalence
            for idx in unique_sources {
                // when joining R and S on R.x = S.x, if R.x is unique and S.x is not, we're bounded above by the cardinality of S
                inputs[idx] = SymExp::f64(1.0);
            }
        }

        SymbolicExpression::product(inputs)
    }

    fn reduce(
        &self,
        group_key: &Vec<MirScalarExpr>,
        expected_group_size: &Option<u64>,
        input: &SymExp,
    ) -> SymExp {
        // TODO(mgree): if no `group_key` is present, we can do way better

        if let Some(group_size) = expected_group_size {
            input / f64::cast_lossy(*group_size)
        } else if group_key.is_empty() {
            SymExp::from(1)
        } else {
            // in the worst case, every row is its own group
            input.clone()
        }
    }

    fn topk(
        &self,
        group_key: &Vec<usize>,
        limit: &Option<usize>,
        expected_group_size: &Option<u64>,
        input: &SymExp,
    ) -> SymExp {
        let k = limit.unwrap_or(1);

        if let Some(group_size) = expected_group_size {
            input * (f64::cast_lossy(k) / f64::cast_lossy(*group_size))
        } else if group_key.is_empty() {
            SymExp::from(k)
        } else {
            // in the worst case, every row is its own group
            input.clone()
        }
    }

    fn threshold(&self, input: &SymExp) -> SymExp {
        // worst case scaling factor is 1
        input.clone()
    }
}

impl Attribute for Cardinality {
    type Value = SymExp;

    fn derive(&mut self, expr: &MirRelationExpr, deps: &DerivedAttributes) {
        use MirRelationExpr::*;
        let n = self.results.len();

        match expr {
            Constant { rows, .. } => self
                .results
                .push(SymExp::from(rows.as_ref().map_or_else(|_| 0, |v| v.len()))),
            Get { id, .. } => match id {
                Id::Local(id) => match self.env.get(id) {
                    Some(value) => self.results.push(value.clone()),
                    None => {
                        // TODO(mgree) when will we meet an unbound local?
                        self.results
                            .push(SymExp::symbolic(FactorizerVariable::Unknown));
                    }
                },
                Id::Global(id) => self
                    .results
                    .push(SymbolicExpression::symbolic(FactorizerVariable::Id(*id))),
            },
            Let { .. } | Project { .. } | Map { .. } | ArrangeBy { .. } | Negate { .. } => {
                let input = self.results[n - 1].clone();
                self.results.push(input);
            }
            LetRec { .. } =>
            // TODO(mgree): implement a recurrence-based approach (or at least identify common idioms, e.g. transitive closure)
            {
                self.results
                    .push(SymbolicExpression::symbolic(FactorizerVariable::Unknown));
            }
            Union { base: _, inputs } => {
                let mut branches = Vec::with_capacity(inputs.len() + 1);
                let mut offset = 1;
                for _ in 0..inputs.len() {
                    branches.push(self.results[n - offset].clone());
                    offset += deps.get_results::<SubtreeSize>()[n - offset];
                }
                branches.push(self.results[n - offset].clone());

                self.results.push(SymbolicExpression::sum(branches));
            }
            FlatMap { func, .. } => {
                let input = &self.results[n - 1];
                self.results.push(self.factorize.flat_map(func, input));
            }
            Filter { predicates, .. } => {
                let input = &self.results[n - 1];
                let keys = &deps.get_results::<UniqueKeys>()[n - 1];
                self.results
                    .push(self.factorize.filter(predicates, keys, input));
            }
            Join {
                equivalences,
                implementation,
                inputs,
                ..
            } => {
                let mut input_results = Vec::with_capacity(inputs.len());

                // maps a column to the index in `inputs` that it belongs to
                let mut unique_columns = BTreeMap::new();
                let mut key_offset = 0;

                let mut offset = 1;
                for idx in 0..inputs.len() {
                    let input = &self.results[n - offset];
                    input_results.push(input);

                    let arity = deps.get_results::<Arity>()[n - offset];
                    let keys = &deps.get_results::<UniqueKeys>()[n - offset];
                    for key in keys {
                        if key.len() == 1 {
                            unique_columns.insert(key_offset + key[0], idx);
                        }
                    }
                    key_offset += arity;

                    offset += &deps.get_results::<SubtreeSize>()[n - offset];
                }

                self.results.push(self.factorize.join(
                    equivalences,
                    implementation,
                    unique_columns,
                    input_results,
                ));
            }
            Reduce {
                group_key,
                expected_group_size,
                ..
            } => {
                let input = &self.results[n - 1];
                self.results
                    .push(self.factorize.reduce(group_key, expected_group_size, input));
            }
            TopK {
                group_key,
                limit,
                expected_group_size,
                ..
            } => {
                let input = &self.results[n - 1];
                self.results.push(self.factorize.topk(
                    group_key,
                    limit,
                    expected_group_size,
                    input,
                ));
            }
            Threshold { .. } => {
                let input = &self.results[n - 1];
                self.results.push(self.factorize.threshold(input));
            }
        }
    }

    fn schedule_env_tasks(&mut self, expr: &MirRelationExpr) {
        self.env.schedule_tasks(expr);
    }

    fn handle_env_tasks(&mut self) {
        self.env.handle_tasks(&self.results);
    }

    fn add_dependencies(builder: &mut RequiredAttributes)
    where
        Self: Sized,
    {
        builder.require::<SubtreeSize>();
        builder.require::<Arity>();
        builder.require::<UniqueKeys>();
    }

    fn get_results(&self) -> &Vec<Self::Value> {
        &self.results
    }

    fn get_results_mut(&mut self) -> &mut Vec<Self::Value> {
        &mut self.results
    }

    fn take(self) -> Vec<Self::Value> {
        self.results
    }
}

impl SymExp {
    /// Render a symbolic expression nicely
    pub fn humanize(
        &self,
        h: &dyn ExprHumanizer,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        self.humanize_factor(h, f)
    }

    fn humanize_factor(
        &self,
        h: &dyn ExprHumanizer,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        use SymbolicExpression::*;
        match self {
            Sum(ss) => {
                assert!(ss.len() >= 2);

                let mut ss = ss.iter();
                ss.next().unwrap().humanize_factor(h, f)?;
                for s in ss {
                    write!(f, " + ")?;
                    s.humanize_factor(h, f)?;
                }
                Ok(())
            }
            _ => self.humanize_term(h, f),
        }
    }

    fn humanize_term(
        &self,
        h: &dyn ExprHumanizer,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        use SymbolicExpression::*;
        match self {
            Product(ps) => {
                assert!(ps.len() >= 2);

                let mut ps = ps.iter();
                ps.next().unwrap().humanize_term(h, f)?;
                for p in ps {
                    write!(f, " * ")?;
                    p.humanize_term(h, f)?;
                }
                Ok(())
            }
            _ => self.humanize_atom(h, f),
        }
    }

    fn humanize_atom(
        &self,
        h: &dyn ExprHumanizer,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        use SymbolicExpression::*;
        match self {
            Constant(OrderedFloat::<f64>(n)) => write!(f, "{n}"),
            Symbolic(FactorizerVariable::Id(v), n) => {
                let id = h.humanize_id(*v).unwrap_or_else(|| format!("{v:?}"));
                write!(f, "{id}")?;

                if *n > 1 {
                    write!(f, "^{n}")?;
                }

                Ok(())
            }
            Symbolic(FactorizerVariable::Index(col), n) => {
                write!(f, "icard(#{col})^{n}")
            }
            Symbolic(FactorizerVariable::Unknown, n) => {
                write!(f, "unknown^{n}")
            }
            Max(e1, e2) => {
                write!(f, "max(")?;
                e1.humanize_factor(h, f)?;
                write!(f, ", ")?;
                e2.humanize_factor(h, f)?;
                write!(f, ")")
            }
            Min(e1, e2) => {
                write!(f, "min(")?;
                e1.humanize_factor(h, f)?;
                write!(f, ", ")?;
                e2.humanize_factor(h, f)?;
                write!(f, ")")
            }
            Sum(_) | Product(_) => {
                write!(f, "(")?;
                self.humanize_factor(h, f)?;
                write!(f, ")")
            }
        }
    }
}

impl std::fmt::Display for SymExp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.humanize(&mz_repr::explain::DummyHumanizer, f)
    }
}

/// Wrapping struct for pretty printing of symbolic expressions
#[allow(missing_debug_implementations)]
pub struct HumanizedSymbolicExpression<'a, 'b> {
    expr: &'a SymExp,
    humanizer: &'b dyn ExprHumanizer,
}

impl<'a, 'b> HumanizedSymbolicExpression<'a, 'b> {
    /// Pairs a symbolic expression with a way to render GlobalIds
    pub fn new(expr: &'a SymExp, humanizer: &'b dyn ExprHumanizer) -> Self {
        Self { expr, humanizer }
    }
}

impl<'a, 'b> std::fmt::Display for HumanizedSymbolicExpression<'a, 'b> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.expr.normalize().humanize(self.humanizer, f)
    }
}
