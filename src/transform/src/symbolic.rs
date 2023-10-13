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

use std::collections::{BTreeMap, BTreeSet};

use ordered_float::OrderedFloat;

/// Symbolic algebraic expressions over variables `V`
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum SymbolicExpression<V> {
    /// A constant expression
    Constant(OrderedFloat<f64>),
    /// `Variable(x, n)` represents `x^n`
    Symbolic(V, usize),
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
    /// Evaluates a symbolic expression, given a way to `concretize` its symbolic parts
    pub fn evaluate<F>(&self, concretize: &F) -> f64
    where
        F: Fn(&V) -> f64,
    {
        use SymbolicExpression::*;
        match self {
            Constant(OrderedFloat(n)) => *n,
            Symbolic(v, n) => f64::powi(
                concretize(v),
                i32::try_from(*n).expect("symbolic exponent overflow"),
            ),
            Sum(ss) => ss.into_iter().map(|s| s.evaluate(concretize)).sum(),
            Product(ps) => ps.into_iter().map(|p| p.evaluate(concretize)).product(),
            Max(e1, e2) => f64::max(e1.evaluate(concretize), e2.evaluate(concretize)),
            Min(e1, e2) => f64::min(e1.evaluate(concretize), e2.evaluate(concretize)),
        }
    }

    /// Computes the order of a symbolic expression
    ///
    /// ```
    /// use mz_transform::symbolic::SymbolicExpression;
    ///
    /// let x = SymbolicExpression::symbolic("x".to_string());
    /// let y = SymbolicExpression::symbolic("y".to_string());
    /// // x^3 + xy + 1000000
    /// let e = SymbolicExpression::sum(vec![SymbolicExpression::product(vec![x.clone(); 3]), SymbolicExpression::product(vec![x, y]), SymbolicExpression::f64(1000000.0)]);
    /// // has order 3
    /// assert_eq!(e.order(), 3);
    /// ```
    pub fn order(&self) -> usize {
        use SymbolicExpression::*;
        match self {
            Constant(_) => 0,
            Symbolic(_, n) => *n,
            Sum(ss) => ss.into_iter().map(|s| s.order()).max().unwrap_or(0),
            Product(ps) => ps.into_iter().map(|p| p.order()).sum(),
            Max(e1, e2) => usize::max(e1.order(), e2.order()),
            Min(e1, e2) => usize::min(e1.order(), e2.order()),
        }
    }

    /// Collects all symbolic values in the expression
    ///
    /// ```
    /// use mz_transform::symbolic::SymbolicExpression;
    ///
    /// let x = SymbolicExpression::symbolic("x".to_string());
    /// let y = SymbolicExpression::symbolic("y".to_string());
    /// // x^3 + xy + 1000000
    /// let e = SymbolicExpression::sum(vec![SymbolicExpression::product(vec![x.clone(); 3]), SymbolicExpression::product(vec![x, y]), SymbolicExpression::f64(1000000.0)]);
    /// // has order 3
    /// let mut symbolics = std::collections::BTreeSet::new();
    /// e.collect_symbolics(&mut symbolics);
    /// assert_eq!(symbolics.len(), 2);
    /// assert!(symbolics.contains(&"x".to_string()));
    /// assert!(symbolics.contains(&"y".to_string()));
    /// ```
    pub fn collect_symbolics(&self, symbolics: &mut BTreeSet<V>)
    where
        V: Clone + Ord + Eq,
    {
        use SymbolicExpression::*;
        match self {
            Constant(_) => (),
            Symbolic(v, _) => {
                symbolics.insert(v.clone());
            }
            Sum(es) | Product(es) => es
                .into_iter()
                .for_each(move |e| e.collect_symbolics(symbolics)),
            Max(e1, e2) | Min(e1, e2) => {
                e1.collect_symbolics(symbolics);
                e2.collect_symbolics(symbolics);
            }
        }
    }

    /// Walks the entire term, simplifying away redundancies that may have accumulated
    pub fn normalize(&self) -> Self
    where
        V: Clone + Ord + Eq,
    {
        use SymbolicExpression::*;
        match self {
            Constant(_) | Symbolic(_, _) => self.clone(),
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
    pub fn symbolic(v: V) -> Self
    where
        V: Ord,
    {
        Self::Symbolic(v, 1)
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
                Symbolic(v, n) => {
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
                .map(|((v, n), scalar)| Symbolic(v, n) * SymbolicExpression::usize(scalar)),
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
                Symbolic(v, n) => {
                    variables.entry(v).and_modify(|e| *e += n).or_insert(n);
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
        result.extend(variables.into_iter().map(|(v, n)| Symbolic(v, n)));

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
            (Symbolic(v1, mut n1), Symbolic(v2, n2)) if v1 == v2 => {
                n1 += n2;
                Symbolic(v1, n1)
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

#[cfg(test)]
mod test {
    use std::f64::consts::{E, PI};

    use super::*;

    #[mz_ore::test]
    fn test_evaluate() {
        type SymExp = SymbolicExpression<String>;

        fn concretize(s: &String) -> f64 {
            if s == "pi" {
                PI
            } else if s == "e" {
                E
            } else if s == "one" {
                1.0
            } else if s == "two" {
                2.0
            } else {
                0.0
            }
        }

        // normally its imprudent to use `assert_eq!`/`==` with f64
        // but we're doing the operations we expect `evaluate` to be doing! so we should IDENTICAL answers

        assert_eq!(SymExp::f64(0.0).evaluate(&concretize), 0.0);
        assert_eq!(SymExp::symbolic("pi".to_string()).evaluate(&concretize), PI);
        assert_eq!(SymExp::symbolic("e".to_string()).evaluate(&concretize), E);
        assert_eq!(
            SymExp::symbolic("one".to_string()).evaluate(&concretize),
            1.0
        );

        assert_eq!(
            SymExp::product(vec![
                SymExp::symbolic("one".to_string()),
                SymExp::symbolic("two".to_string())
            ])
            .evaluate(&concretize),
            2.0
        );

        let two_to_the_eighth = SymExp::product(vec![SymExp::symbolic("two".to_string()); 8]);
        match &two_to_the_eighth {
            SymExp::Symbolic(v, n) => {
                assert_eq!(v, "two");
                assert_eq!(*n, 8);
            }
            _ => assert!(false, "didn't use exponenents correctly"),
        };
        assert_eq!(two_to_the_eighth.evaluate(&concretize), 256.0);

        assert_eq!(
            SymExp::sum(vec![
                SymExp::symbolic("pi".to_string()),
                SymExp::symbolic("e".to_string()),
                SymExp::symbolic("two".to_string())
            ])
            .evaluate(&concretize),
            PI + E + 2.0
        );
    }
}
