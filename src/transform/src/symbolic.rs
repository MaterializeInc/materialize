// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Definition and trait instances for working with symbolic algebraic expressions, [`SymbolicExpression`]

use mz_ore::cast::CastLossy;

use std::collections::BTreeMap;

use ordered_float::OrderedFloat;

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
    /// The maximum value of two expressions
    Max(Box<SymbolicExpression<V>>, Box<SymbolicExpression<V>>),
    /// The minimum value of two expressions
    Min(Box<SymbolicExpression<V>>, Box<SymbolicExpression<V>>),
}

impl<V> SymbolicExpression<V> {
    /// Walks the entire term, simplifying away redundancies that may have accumulated
    pub fn normalize(&self) -> Self
    where
        V: Clone + Ord + Eq,
    {
        use SymbolicExpression::*;
        match self {
            Constant(_) | Variable(_, _) => self.clone(),
            Sum(ss) => SymbolicExpression::sum(ss.into_iter().map(|s| s.normalize()).collect()),
            Product(ps) => {
                SymbolicExpression::product(ps.into_iter().map(|p| p.normalize()).collect())
            }
            Max(e1, e2) => SymbolicExpression::max(e1.normalize(), e2.normalize()),
            Min(e1, e2) => SymbolicExpression::min(e1.normalize(), e2.normalize()),
        }
    }

    /// Generate a symbolic expression from a `usize` (may be lossy for high values)
    pub fn usize(n: usize) -> Self {
        Self::Constant(OrderedFloat(f64::cast_lossy(n)))
    }

    /// Generate a symbolic expression from a float
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

    /// Computes the maximum of two symbolic expressions
    pub fn max(e1: Self, e2: Self) -> Self
    where
        V: PartialOrd + PartialEq,
    {
        match (e1, e2) {
            (SymbolicExpression::Constant(n1), SymbolicExpression::Constant(n2)) => {
                SymbolicExpression::Constant(OrderedFloat::max(n1, n2))
            }
            (e1, e2) => {
                // canonical ordering (not doing the max computation itself!)
                if e1 < e2 {
                    SymbolicExpression::Max(Box::new(e1), Box::new(e2))
                } else if e1 == e2 {
                    e1
                } else {
                    SymbolicExpression::Max(Box::new(e2), Box::new(e1))
                }
            }
        }
    }

    /// Computes the minimum of two symbolic expressions
    pub fn min(e1: Self, e2: Self) -> Self
    where
        V: PartialOrd + PartialEq,
    {
        match (e1, e2) {
            (SymbolicExpression::Constant(n1), SymbolicExpression::Constant(n2)) => {
                SymbolicExpression::Constant(OrderedFloat::min(n1, n2))
            }
            (e1, e2) => {
                // canonical ordering (not doing the max computation itself!)
                if e1 < e2 {
                    SymbolicExpression::Min(Box::new(e1), Box::new(e2))
                } else if e1 == e2 {
                    e1
                } else {
                    SymbolicExpression::Min(Box::new(e2), Box::new(e1))
                }
            }
        }
    }

    /// Computes the n-ary sum of symbolic expressions, yielding a slightly more compact/normalized term than repeated addition
    pub fn sum(es: Vec<Self>) -> Self
    where
        V: Clone + Eq + Ord,
    {
        use SymbolicExpression::*;

        let mut constant = OrderedFloat(0.0);
        let mut variables = BTreeMap::new();
        let mut minmaxes = BTreeMap::new();
        let mut products = BTreeMap::new();

        let mut summands = es;
        while let Some(e) = summands.pop() {
            match e {
                Constant(n) => constant += n,
                Variable(v, n) => {
                    variables.entry((v, n)).and_modify(|e| *e += 1).or_insert(1);
                }
                Max(_, _) | Min(_, _) => {
                    minmaxes.entry(e).and_modify(|e| *e += 1).or_insert(1);
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
                .map(|(p, scalar)| p * SymbolicExpression::usize(scalar)),
        );
        result.extend(
            minmaxes
                .into_iter()
                .map(|(m, scalar)| m * SymbolicExpression::usize(scalar)),
        );
        result.extend(
            variables
                .into_iter()
                .map(|((v, n), scalar)| Variable(v, n) * SymbolicExpression::usize(scalar)),
        );

        if constant.0 != 0.0 {
            result.push(Constant(constant));
        }

        if result.len() > 1 {
            Sum(result)
        } else if result.len() == 1 {
            result.swap_remove(0)
        } else {
            Self::f64(0.0)
        }
    }

    /// Computes the n-ary product of symbolic expressions, yielding a slightly more compact/normalized term than repeated multiplication
    pub fn product(es: Vec<Self>) -> Self
    where
        V: Clone + Eq + Ord,
    {
        use SymbolicExpression::*;

        let mut constant = OrderedFloat(1.0);
        let mut variables = BTreeMap::new();
        let mut minmaxes = BTreeMap::new();
        let mut sums = BTreeMap::new();

        let mut products = es;
        while let Some(e) = products.pop() {
            match e {
                Constant(n) => constant *= n,
                Variable(v, n) => {
                    variables.entry((v, n)).and_modify(|e| *e += 1).or_insert(1);
                }
                Max(_, _) | Min(_, _) => {
                    minmaxes.entry(e).and_modify(|e| *e += 1).or_insert(1);
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
                .map(|(p, scalar)| p * SymbolicExpression::usize(scalar)),
        );
        result.extend(
            minmaxes
                .into_iter()
                .map(|(m, scalar)| m * SymbolicExpression::usize(scalar)),
        );
        result.extend(
            variables
                .into_iter()
                .map(|((v, n), scalar)| Variable(v, n) * SymbolicExpression::usize(scalar)),
        );

        if constant.0 == 0.0 {
            return Self::f64(0.0);
        }

        if constant.0 != 1.0 {
            result.push(Constant(constant));
        }

        if result.len() > 1 {
            Product(result)
        } else if result.len() == 1 {
            result.swap_remove(0)
        } else {
            Self::f64(1.0)
        }
    }
}

impl<V> Default for SymbolicExpression<V> {
    fn default() -> Self {
        SymbolicExpression::f64(0.0)
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
            (Constant(OrderedFloat(n1)), Constant(OrderedFloat(n2))) => {
                SymbolicExpression::f64(n1 + n2)
            }
            (Constant(OrderedFloat(n)), factor) | (factor, Constant(OrderedFloat(n)))
                if n == 0.0 =>
            {
                factor
            }
            (Sum(mut ss1), Sum(ss2)) => {
                ss1.extend(ss2);
                Sum(ss1)
            }
            (Sum(mut ss), summand) | (summand, Sum(mut ss)) => {
                ss.push(summand);
                Sum(ss)
            }
            (lhs, rhs) => Sum(vec![lhs, rhs]),
        }
    }
}

impl<V> std::ops::Add for &SymbolicExpression<V>
where
    V: Clone + Eq,
{
    type Output = SymbolicExpression<V>;

    fn add(self, rhs: Self) -> Self::Output {
        self.clone() + rhs.clone()
    }
}

impl<V> std::ops::Add<f64> for SymbolicExpression<V>
where
    V: Clone + Eq,
{
    type Output = SymbolicExpression<V>;

    fn add(self, rhs: f64) -> Self::Output {
        self + SymbolicExpression::f64(rhs)
    }
}

impl<V> std::ops::Add<SymbolicExpression<V>> for f64
where
    V: Clone + Eq,
{
    type Output = SymbolicExpression<V>;

    fn add(self, rhs: SymbolicExpression<V>) -> Self::Output {
        SymbolicExpression::f64(self) + rhs
    }
}

impl<V> std::ops::Sub for SymbolicExpression<V>
where
    V: Clone + Eq,
{
    type Output = SymbolicExpression<V>;

    fn sub(self, rhs: Self) -> Self::Output {
        self + (-1.0 * rhs)
    }
}

impl<V> std::ops::Sub<f64> for SymbolicExpression<V>
where
    V: Clone + Eq,
{
    type Output = SymbolicExpression<V>;

    fn sub(self, rhs: f64) -> Self::Output {
        self + (-1.0 * rhs)
    }
}

impl<V> std::ops::Sub<SymbolicExpression<V>> for f64
where
    V: Clone + Eq,
{
    type Output = SymbolicExpression<V>;

    fn sub(self, rhs: SymbolicExpression<V>) -> Self::Output {
        self + (-1.0 * rhs)
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
            (Constant(OrderedFloat(mut n1)), Constant(OrderedFloat(n2))) => {
                n1 *= n2;
                Constant(OrderedFloat(n1))
            }
            (Constant(OrderedFloat(n)), factor) | (factor, Constant(OrderedFloat(n)))
                if n == 1.0 =>
            {
                factor
            }
            (Constant(OrderedFloat(n)), _) | (_, Constant(OrderedFloat(n))) if n == 0.0 => {
                Constant(OrderedFloat(n))
            }
            (Variable(v1, mut n1), Variable(v2, n2)) if v1 == v2 => {
                n1 += n2;
                Variable(v1, n1)
            }
            (Product(mut ps1), Product(ps2)) => {
                // TODO(mgree): we could check for exponent increases on variables, but whatever
                ps1.extend(ps2);
                Product(ps1)
            }
            (Product(mut ps), factor) | (factor, Product(mut ps)) => {
                // TODO(mgree): we could check for exponent increases on variables, but whatever
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

impl<V> std::ops::Mul for &SymbolicExpression<V>
where
    V: Clone + Eq,
{
    type Output = SymbolicExpression<V>;

    fn mul(self, rhs: Self) -> Self::Output {
        self.clone() * rhs.clone()
    }
}

impl<V> std::ops::Mul<f64> for SymbolicExpression<V>
where
    V: Clone + Eq,
{
    type Output = SymbolicExpression<V>;

    fn mul(self, rhs: f64) -> Self::Output {
        self * SymbolicExpression::f64(rhs)
    }
}

impl<V> std::ops::Mul<SymbolicExpression<V>> for f64
where
    V: Clone + Eq,
{
    type Output = SymbolicExpression<V>;

    fn mul(self, rhs: SymbolicExpression<V>) -> Self::Output {
        SymbolicExpression::f64(self) * rhs
    }
}

impl<V> std::ops::Mul<f64> for &SymbolicExpression<V>
where
    V: Clone + Eq,
{
    type Output = SymbolicExpression<V>;

    fn mul(self, rhs: f64) -> Self::Output {
        self * &SymbolicExpression::f64(rhs)
    }
}

impl<V> std::ops::Mul<&SymbolicExpression<V>> for f64
where
    V: Clone + Eq,
{
    type Output = SymbolicExpression<V>;

    fn mul(self, rhs: &SymbolicExpression<V>) -> Self::Output {
        &SymbolicExpression::f64(self) * rhs
    }
}

impl<V> std::ops::Div<f64> for SymbolicExpression<V>
where
    V: Clone + Eq,
{
    type Output = SymbolicExpression<V>;

    fn div(self, rhs: f64) -> Self::Output {
        self * (1.0 / rhs)
    }
}

impl<V> std::ops::Div<f64> for &SymbolicExpression<V>
where
    V: Clone + Eq,
{
    type Output = SymbolicExpression<V>;

    fn div(self, rhs: f64) -> Self::Output {
        self * (1.0 / rhs)
    }
}

impl<V> From<usize> for SymbolicExpression<V> {
    fn from(value: usize) -> Self {
        SymbolicExpression::usize(value)
    }
}

impl<V> From<f64> for SymbolicExpression<V> {
    fn from(value: f64) -> Self {
        SymbolicExpression::f64(value)
    }
}
