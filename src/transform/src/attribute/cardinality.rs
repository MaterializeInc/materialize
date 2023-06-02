// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Definition and helper structs for the [`Cardinality`] attribute.

use std::collections::BTreeMap;

use mz_expr::{Id, JoinImplementation, MirRelationExpr, MirScalarExpr, TableFunc};
use mz_ore::cast::CastLossy;
use mz_ore::iter::IteratorExt;
use mz_repr::GlobalId;

use mz_repr::explain::ExprHumanizer;
use ordered_float::OrderedFloat;

use crate::attribute::subtree_size::SubtreeSize;
use crate::attribute::{Attribute, DerivedAttributes, Env, RequiredAttributes};

/// Compute the estimated cardinality of each subtree of a [MirRelationExpr] from the bottom up.
#[allow(missing_debug_implementations)]
pub struct Cardinality {
    /// Environment of computed values for this attribute
    env: Env<Self>,
    /// A vector of results for all nodes in the visited tree in
    /// post-visit order
    pub results: Vec<SymbolicExpression<GlobalId>>,
    /// A factorizer for generating appropriating scaling factors
    pub factorize: Box<dyn Factorizer<GlobalId> + Send + Sync>,
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

/// Symbolic algebraic expressions over variables `V`
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum SymbolicExpression<V> {
    /// A constant expression
    Constant(OrderedFloat<f64>),
    /// `Variable(x, n)` represents `x^n`
    Variable(V, usize),
    /// `Sum([e_1, ..., e_m])` represents e_1 + ... + e_m
    Sum(Vec<SymbolicExpression<V>>),
    /// `Product([e_1, ..., e_m])` represents e_1 * ... * e_m
    Product(Vec<SymbolicExpression<V>>),
}

/// A `Factorizer` computes selectivity factors
pub trait Factorizer<V> {
    fn flat_map(&self, tf: &TableFunc, input: &SymbolicExpression<V>) -> SymbolicExpression<V>;
    fn filter(
        &self,
        predicates: &Vec<MirScalarExpr>,
        input: &SymbolicExpression<V>,
    ) -> SymbolicExpression<V>;
    fn join(
        &self,
        equivalences: &Vec<Vec<MirScalarExpr>>,
        implementation: &JoinImplementation,
        inputs: Vec<&SymbolicExpression<V>>,
    ) -> SymbolicExpression<V>;
    fn reduce(
        &self,
        group_key: &Vec<MirScalarExpr>,
        expected_group_size: &Option<u64>,
        input: &SymbolicExpression<V>,
    ) -> SymbolicExpression<V>;
    fn topk(
        &self,
        group_key: &Vec<usize>,
        limit: &Option<usize>,
        expected_group_size: &Option<u64>,
        input: &SymbolicExpression<V>,
    ) -> SymbolicExpression<V>;
    fn threshold(&self, input: &SymbolicExpression<V>) -> SymbolicExpression<V>;
}

pub struct WorstCaseFactorizer<V> {
    pub cardinalities: BTreeMap<V, usize>,
}

impl<V> Factorizer<V> for WorstCaseFactorizer<V>
where
    V: Clone + Ord,
{
    fn flat_map(&self, tf: &TableFunc, input: &SymbolicExpression<V>) -> SymbolicExpression<V> {
        match tf {
            TableFunc::GenerateSeriesInt32
            | TableFunc::GenerateSeriesInt64
            | TableFunc::GenerateSeriesTimestamp
            | TableFunc::GenerateSeriesTimestampTz => input.clone(),
            TableFunc::Wrap { types, width } => {
                input
                    * &SymbolicExpression::f64(
                        f64::cast_lossy(types.len()) / f64::cast_lossy(*width),
                    )
            }
            _ => {
                // TODO(mgree) what explosion factor should we make up?
                input * &SymbolicExpression::f64(4.0)
            }
        }
    }

    fn filter(
        &self,
        _predicates: &Vec<MirScalarExpr>,
        input: &SymbolicExpression<V>,
    ) -> SymbolicExpression<V> {
        // worst case scaling factor is 1
        input.clone()
    }

    fn join(
        &self,
        _equivalences: &Vec<Vec<MirScalarExpr>>,
        _implementation: &JoinImplementation,
        inputs: Vec<&SymbolicExpression<V>>,
    ) -> SymbolicExpression<V> {
        // TODO(mgree): some knowledge about these equivalences (and the indices) will let us give a better scaling factor
        SymbolicExpression::product(inputs)
    }

    fn reduce(
        &self,
        group_key: &Vec<MirScalarExpr>,
        expected_group_size: &Option<u64>,
        input: &SymbolicExpression<V>,
    ) -> SymbolicExpression<V> {
        // TODO(mgree): if no `group_key` is present, we can do way better

        if let Some(group_size) = expected_group_size {
            input * &SymbolicExpression::f64(1.0 / f64::cast_lossy(*group_size))
        } else if group_key.is_empty() {
            SymbolicExpression::usize(1)
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
        input: &SymbolicExpression<V>,
    ) -> SymbolicExpression<V> {
        let k = limit.unwrap_or(1);

        if let Some(group_size) = expected_group_size {
            input * &SymbolicExpression::f64(f64::cast_lossy(k) / f64::cast_lossy(*group_size))
        } else if group_key.is_empty() {
            SymbolicExpression::usize(k)
        } else {
            // in the worst case, every row is its own group
            input.clone()
        }
    }

    fn threshold(&self, input: &SymbolicExpression<V>) -> SymbolicExpression<V> {
        // worst case scaling factor is 1
        input.clone()
    }
}

impl<V> SymbolicExpression<V> {
    pub fn usize(n: usize) -> Self {
        Self::Constant(OrderedFloat(f64::cast_lossy(n)))
    }

    pub fn f64(n: f64) -> Self {
        Self::Constant(OrderedFloat(n))
    }

    /// References a variable (with a default exponent of 1)
    pub fn var(v: V) -> Self
    where
        V: Ord,
    {
        Self::Variable(v, 1)
    }

    /// Computes the n-ary sum of symbolic expressions, yielding a slightly more compact/normalized term than repeated addition
    pub fn sum(es: Vec<&Self>) -> Self
    where
        V: Clone + Eq + Ord,
    {
        use SymbolicExpression::*;

        let mut constant = OrderedFloat(0.0);
        let mut variables = BTreeMap::new();
        let mut products = BTreeMap::new();

        let mut summands = es;
        while let Some(e) = summands.pop() {
            match e {
                Constant(n) => constant += n,
                Variable(v, n) => {
                    variables.entry((v, n)).and_modify(|e| *e += 1).or_insert(1);
                }
                Sum(ss) => summands.extend(ss),
                p @ Product(_) => {
                    products.entry(p).and_modify(|e| *e += 1).or_insert(1);
                }
            }
        }

        let mut result = Vec::with_capacity(1 + variables.len() + products.len());

        result.extend(
            products
                .into_iter()
                .map(|(p, scalar)| p * &SymbolicExpression::usize(scalar)),
        );
        result.extend(variables.into_iter().map(|((v, n), scalar)| {
            SymbolicExpression::Product(vec![
                Variable(v.clone(), *n),
                SymbolicExpression::usize(scalar),
            ])
        }));
        result.push(Constant(constant));

        Sum(result)
    }

    /// Computes the n-ary product of symbolic expressions, yielding a slightly more compact/normalized term than repeated multiplication
    pub fn product(es: Vec<&Self>) -> Self
    where
        V: Clone + Eq + Ord,
    {
        use SymbolicExpression::*;

        let mut constant = OrderedFloat(0.0);
        let mut variables = BTreeMap::new();
        let mut sums = BTreeMap::new();

        let mut products = es;
        while let Some(e) = products.pop() {
            match e {
                Constant(n) => constant *= n,
                Variable(v, n) => {
                    variables.entry((v, n)).and_modify(|e| *e += 1).or_insert(1);
                }
                Product(ps) => products.extend(ps),
                s @ Sum(_) => {
                    sums.entry(s).and_modify(|e| *e += 1).or_insert(1);
                }
            }
        }

        let mut result = Vec::with_capacity(1 + variables.len() + sums.len());

        result.extend(
            sums.into_iter()
                .map(|(p, scalar)| p * &SymbolicExpression::usize(scalar)),
        );
        result.extend(variables.into_iter().map(|((v, n), scalar)| {
            SymbolicExpression::Product(vec![
                Variable(v.clone(), *n),
                SymbolicExpression::usize(scalar),
            ])
        }));
        result.push(Constant(constant));

        Product(result)
    }
}

impl<V> Default for SymbolicExpression<V> {
    fn default() -> Self {
        SymbolicExpression::f64(0.0)
    }
}

impl<V> std::ops::Add for &SymbolicExpression<V>
where
    V: Clone + Eq,
{
    type Output = SymbolicExpression<V>;

    fn add(self, rhs: Self) -> Self::Output {
        use SymbolicExpression::*;
        match (self, rhs) {
            (Constant(OrderedFloat(n1)), Constant(OrderedFloat(n2))) => {
                SymbolicExpression::f64(n1 + n2)
            }
            (Constant(OrderedFloat(n)), factor) | (factor, Constant(OrderedFloat(n)))
                if *n == 0.0 =>
            {
                factor.clone()
            }
            (Sum(ss1), Sum(ss2)) => Sum(ss1.iter().chain(ss2.iter()).cloned().collect()),
            (summand, Sum(ss)) | (Sum(ss), summand) => {
                Sum(ss.iter().chain_one(summand).cloned().collect())
            }
            (lhs, rhs) => Sum(vec![lhs.clone(), rhs.clone()]),
        }
    }
}

impl<V> std::ops::Mul for &SymbolicExpression<V>
where
    V: Clone + Eq,
{
    type Output = SymbolicExpression<V>;

    fn mul(self, rhs: Self) -> Self::Output {
        use SymbolicExpression::*;
        match (self, rhs) {
            (Constant(OrderedFloat(n1)), Constant(OrderedFloat(n2))) => {
                SymbolicExpression::f64(n1 * n2)
            }
            (Constant(OrderedFloat(n)), factor) | (factor, Constant(OrderedFloat(n)))
                if *n == 1.0 =>
            {
                factor.clone()
            }
            (Constant(OrderedFloat(n)), _) | (_, Constant(OrderedFloat(n))) if *n == 0.0 => {
                SymbolicExpression::f64(0.0)
            }
            (Variable(v1, n1), Variable(v2, n2)) if v1 == v2 => {
                SymbolicExpression::Variable(v1.clone(), n1 + n2)
            }
            (Product(ps1), Product(ps2)) => {
                Product(ps1.iter().chain(ps2.iter()).cloned().collect())
            }
            (factor, Product(ps)) | (Product(ps), factor) => {
                Product(ps.iter().chain_one(factor).cloned().collect())
            }
            (factor, Sum(ss)) | (Sum(ss), factor) => Sum(ss.iter().map(|s| factor * s).collect()),
            (lhs, rhs) => Product(vec![lhs.clone(), rhs.clone()]),
        }
    }
}

impl Attribute for Cardinality {
    type Value = SymbolicExpression<GlobalId>;

    fn derive(&mut self, expr: &MirRelationExpr, deps: &DerivedAttributes) {
        use MirRelationExpr::*;
        let n = self.results.len();

        match expr {
            Constant { rows, .. } => self.results.push(SymbolicExpression::usize(
                rows.as_ref().map_or_else(|_| 0, |v| v.len()),
            )),
            Get { id, .. } => match id {
                Id::Local(id) => match self.env.get(id) {
                    Some(value) => self.results.push(value.clone()),
                    None => {
                        // TODO(mgree) when will we meet an unbound local?
                        unimplemented!()
                    }
                },
                Id::Global(id) => self.results.push(SymbolicExpression::var(*id)),
            },
            Let { .. } | Project { .. } | Map { .. } | ArrangeBy { .. } | Negate { .. } => {
                let input = self.results[n - 1].clone();
                self.results.push(input);
            }
            LetRec { .. } => unimplemented!("cardinality analysis does not yet support recursion"),
            Union { base: _, inputs } => {
                let mut branches = Vec::with_capacity(inputs.len() + 1);
                let mut offset = 1;
                for _ in 0..inputs.len() {
                    branches.push(&self.results[n - offset]);
                    offset += deps.get_results::<SubtreeSize>()[n - offset];
                }
                branches.push(&self.results[n - offset]);

                self.results.push(SymbolicExpression::sum(branches));
            }
            FlatMap { func, .. } => {
                let input = &self.results[n - 1];
                self.results.push(self.factorize.flat_map(func, input));
            }
            Filter { predicates, .. } => {
                let input = &self.results[n - 1];
                self.results.push(self.factorize.filter(predicates, input));
            }
            Join {
                equivalences,
                implementation,
                inputs,
                ..
            } => {
                // TODO(mgree): we can give better answers for some kinds of joins if we have index information... here or elsewhere?

                let mut input_results = Vec::with_capacity(inputs.len());
                let mut offset = 1;
                for _ in 0..inputs.len() {
                    input_results.push(&self.results[n - offset]);
                    offset += &deps.get_results::<SubtreeSize>()[n - offset];
                }

                self.results.push(
                    self.factorize
                        .join(equivalences, implementation, input_results),
                );
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
                // TODO(mgree) if we add `SymbolicExpression::max` we can express `limit` nicely

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

impl SymbolicExpression<GlobalId> {
    pub fn humanize<H>(&self, h: &H, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result
    where
        H: ExprHumanizer,
    {
        self.humanize_factor(h, f)
    }

    fn humanize_factor<H>(&self, h: &H, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result
    where
        H: ExprHumanizer,
    {
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

    fn humanize_term<H>(&self, h: &H, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result
    where
        H: ExprHumanizer,
    {
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

    fn humanize_atom<H>(&self, h: &H, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result
    where
        H: ExprHumanizer,
    {
        use SymbolicExpression::*;
        match self {
            Constant(OrderedFloat(n)) => write!(f, "{n}"),
            Variable(v, n) => {
                let id = h.humanize_id(*v).unwrap_or_else(|| format!("{v:?}"));
                write!(f, "{id}")?;

                if *n > 1 {
                    write!(f, "^{n}")?;
                }

                Ok(())
            }
            Sum(_) | Product(_) => {
                write!(f, "(")?;
                self.humanize_factor(h, f)?;
                write!(f, ")")
            }
        }
    }
}

impl std::fmt::Display for SymbolicExpression<GlobalId> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.humanize(&mz_repr::explain::DummyHumanizer, f)
    }
}
