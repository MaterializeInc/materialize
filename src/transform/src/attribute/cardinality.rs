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
use mz_repr::GlobalId;

use crate::attribute::subtree_size::SubtreeSize;
use crate::attribute::{Attribute, DerivedAttributes, Env, RequiredAttributes};

/// Compute the estimated cardinality of each subtree of a [MirRelationExpr] from the bottom up.
#[derive(Default)]
#[allow(missing_debug_implementations)]
pub struct Cardinality {
    /// Environment of computed values for this attribute
    env: Env<Self>,
    /// A vector of results for all nodes in the visited tree in
    /// post-visit order
    pub results: Vec<SymbolicExpression<GlobalId>>,
}

/// Symbolic algebraic expressions over variables `V`
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum SymbolicExpression<V> {
    /// A constant expression
    Constant(usize),
    /// `Variable(x, n)` represents `x^n`
    Variable(V, usize),
    /// `Sum([e_1, ..., e_m])` represents e_1 + ... + e_m
    Sum(Vec<SymbolicExpression<V>>),
    /// `Product([e_1, ..., e_m])` represents e_1 * ... * e_m
    Product(Vec<SymbolicExpression<V>>),
    /// A symbolic (i.e., unresolved) scaling factor on an expression
    ScalingFactor(SymbolicScalingFactor, Box<SymbolicExpression<V>>),
}

/// A structure characterizing operations that have _some_ effect on cardinality that is hard to statically determine
///
/// e.g., `SELECT * FROM t WHERE t.x = 1` may greatly restrict our output (only a few rows have `x = 1`)  or not at all (every row has `x = 1`)
///
/// Rather than making something up, we'll record this information symbolically and resolve it when we have more information
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum SymbolicScalingFactor {
    FlatMap(TableFunc),
    Filter(Vec<MirScalarExpr>),
    Join {
        equivalences: Vec<Vec<MirScalarExpr>>,
        implementation: JoinImplementation,
    },
    Reduce {
        expected_group_size: Option<u64>,
    },
    TopK {
        expected_group_size: Option<u64>,
    },
    Threshold,
}

/// A `Concretizer` can give concrete (integral) values to variables and concrete (floating point) values to scaling factors
///
/// Given `c: Concretizer`, you can call `SymbolicExpression::concretize(&c)` to fully evaluate a symbolic expression
pub trait Concretizer<V> {
    fn concretize_variable(&self, v: &V) -> usize;
    fn concretize_scaling_factor(&self, sf: &SymbolicScalingFactor) -> f64;
}

pub struct WorstCaseConcretizer<V> {
    pub cardinalities: BTreeMap<V, usize>,
}

impl<V> Concretizer<V> for WorstCaseConcretizer<V>
where
    V: Ord,
{
    fn concretize_variable(&self, v: &V) -> usize {
        *self.cardinalities.get(v).expect("unknown variable")
    }

    fn concretize_scaling_factor(&self, sf: &SymbolicScalingFactor) -> f64 {
        use SymbolicScalingFactor::*;
        match sf {
            Filter(_) => 1.0,
            Join {
                equivalences: _,
                implementation: _,
            } => {
                // this scaling factor is applied to the worst-case, i.e, the cross-join/full cartesian product cardinality
                // TODO(mgree): some knowledge about these equivalences (and the indices) will let us give a better scaling factor

                1.0
            }
            FlatMap(
                TableFunc::GenerateSeriesInt32
                | TableFunc::GenerateSeriesInt64
                | TableFunc::GenerateSeriesTimestamp
                | TableFunc::GenerateSeriesTimestampTz,
            ) => 1.0,
            FlatMap(TableFunc::Wrap { types, width }) => {
                f64::cast_lossy(types.len()) / f64::cast_lossy(*width)
            }
            FlatMap(_) => {
                // TODO(mgree) what explosion factor should we make up?
                4.0
            }
            Reduce {
                expected_group_size: Some(group_size),
            } => 1.0 / f64::cast_lossy(*group_size),
            Reduce {
                expected_group_size: None,
            } => {
                // in the worst case, every row is its own group
                1.0
            }
            TopK {
                expected_group_size: Some(group_size),
            } => 1.0 / f64::cast_lossy(*group_size),
            TopK {
                expected_group_size: None,
            } => {
                // in the worst case, every row is its own group
                1.0
            }
            Threshold => 1.0,
        }
    }
}

impl<V> SymbolicExpression<V> {
    /// References a variable (with a default exponent of 1)
    pub fn var(v: V) -> Self
    where
        V: Ord,
    {
        Self::Variable(v, 1)
    }

    /// Applies a `SymbolicScalingFactor` to this symbolic expression
    pub fn scale(self, factor: SymbolicScalingFactor) -> Self {
        Self::ScalingFactor(factor, Box::new(self))
    }

    /// Computes the n-ary sum of symbolic expressions, yielding a slightly more compact/normalized term than repeated addition
    pub fn sum(es: Vec<Self>) -> Self
    where
        V: Clone + PartialEq + Ord,
    {
        use SymbolicExpression::*;

        let mut constant = 0;
        let mut variables = BTreeMap::new();
        let mut products = BTreeMap::new();
        let mut scales = BTreeMap::new();

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
                sf @ ScalingFactor(..) => {
                    scales.entry(sf).and_modify(|e| *e += 1).or_insert(1);
                }
            }
        }

        let mut result = Vec::with_capacity(1 + variables.len() + products.len() + scales.len());

        result.extend(products.into_iter().map(|(p, scalar)| p * Constant(scalar)));
        result.extend(scales.into_iter().map(|(s, scalar)| s * Constant(scalar)));
        result.extend(
            variables
                .into_iter()
                .map(|((v, n), scalar)| Variable(v, n) * Constant(scalar)),
        );
        result.push(Constant(constant));

        Sum(result)
    }

    pub fn concretize<C>(&self, c: &C) -> f64
    where
        C: Concretizer<V>,
    {
        use SymbolicExpression::*;

        match self {
            Constant(n) => f64::cast_lossy(*n),
            Variable(v, n) => {
                let v = c.concretize_variable(v);
                f64::powf(f64::cast_lossy(v), f64::cast_lossy(*n))
            }
            Sum(ss) => ss.iter().map(|s| s.concretize(c)).sum(),
            Product(ps) => ps.iter().map(|p| p.concretize(c)).product(),
            ScalingFactor(sf, e) => {
                let sf = c.concretize_scaling_factor(sf);
                sf * e.concretize(c)
            }
        }
    }
}

impl<V> Default for SymbolicExpression<V> {
    fn default() -> Self {
        SymbolicExpression::Constant(0)
    }
}

impl<V> std::ops::Add for SymbolicExpression<V>
where
    V: Clone + Eq,
{
    type Output = SymbolicExpression<V>;

    fn add(self, rhs: Self) -> Self::Output {
        use SymbolicExpression::*;
        match (self, rhs) {
            (Constant(n1), Constant(n2)) => SymbolicExpression::Constant(n1 + n2),
            (Constant(0), factor) | (factor, Constant(0)) => factor,
            (Sum(mut ss1), Sum(ss2)) => {
                ss1.extend(ss2);
                Sum(ss1)
            }
            (summand, Sum(mut ss)) | (Sum(mut ss), summand) => {
                ss.push(summand);
                Sum(ss)
            }
            (lhs, rhs) => Sum(vec![lhs, rhs]),
        }
    }
}

impl<V> std::ops::Mul for SymbolicExpression<V>
where
    V: Clone + Eq,
{
    type Output = SymbolicExpression<V>;

    fn mul(self, rhs: Self) -> Self::Output {
        use SymbolicExpression::*;
        match (self, rhs) {
            (Constant(n1), Constant(n2)) => SymbolicExpression::Constant(n1 * n2),
            (Constant(1), factor) | (factor, Constant(1)) => factor,
            (Constant(0), _) | (_, Constant(0)) => Constant(0),
            (Variable(v1, n1), Variable(v2, n2)) if v1 == v2 => {
                SymbolicExpression::Variable(v1, n1 + n2)
            }
            (Product(mut ps1), Product(ps2)) => {
                ps1.extend(ps2);
                Product(ps1)
            }
            (factor, Product(mut ps)) | (Product(mut ps), factor) => {
                ps.push(factor);
                Product(ps)
            }
            (factor, Sum(ss)) | (Sum(ss), factor) => {
                Sum(ss.into_iter().map(|s| factor.clone() * s).collect())
            }
            (lhs, rhs) => Product(vec![lhs, rhs]),
        }
    }
}

impl Attribute for Cardinality {
    type Value = SymbolicExpression<GlobalId>;

    fn derive(&mut self, expr: &MirRelationExpr, deps: &DerivedAttributes) {
        use MirRelationExpr::*;
        let n = self.results.len();

        match expr {
            Constant { rows, .. } => self.results.push(SymbolicExpression::Constant(rows.as_ref().map_or_else(|_| 0, |v| v.len()))),
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
            Let { .. }
            | Project { .. }
            | Map { .. }
            | ArrangeBy { .. }
            | Negate { .. }
            | LetRec { .. } // TODO(mgree) this is almost certainly not correct for a letrec
            => {
                let input = self.results[n - 1].clone();
                self.results.push(input);
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
                let input = self.results[n - 1].clone();
                self.results.push(input.scale(SymbolicScalingFactor::FlatMap(func.clone())));
            }
            Filter { predicates, .. } => {
                let input = self.results[n - 1].clone();
                self.results.push(input.scale(SymbolicScalingFactor::Filter(predicates.clone())));
            }
            Join { equivalences, implementation, inputs, .. } => {
                // TODO(mgree): we can give better answers for some kinds of joins if we have index information... here or elsewhere?

                let mut result = SymbolicExpression::Constant(1);
                let mut offset = 1;
                for _ in 0..inputs.len() {
                    result = result * self.results[n - offset].clone();
                    offset += &deps.get_results::<SubtreeSize>()[n - offset];
                }

                self.results.push(result.scale(SymbolicScalingFactor::Join {
                    equivalences: equivalences.clone(),
                    implementation: implementation.clone() }));
            }
            Reduce {expected_group_size, .. }  => {
                let input = self.results[n - 1].clone();
                self.results.push(input.scale(SymbolicScalingFactor::Reduce {
                    expected_group_size: expected_group_size.clone()
                }));
            }
            TopK { limit: _, expected_group_size, ..}  => {
                // TODO(mgree) if we add `SymbolicExpression::max` we can express `limit` nicely

                let input = self.results[n - 1].clone();
                self.results.push(input.scale(SymbolicScalingFactor::TopK {
                    expected_group_size: *expected_group_size,
                }));
            }
            Threshold { .. } => {
                let input = self.results[n - 1].clone();
                self.results.push(input.scale(SymbolicScalingFactor::Threshold));
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
