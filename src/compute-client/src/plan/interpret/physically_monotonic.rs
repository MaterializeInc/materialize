// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Implementation of [crate::plan::interpret::Interpreter] for inference
//! of physical monotonicity in single-time dataflows.

use std::cmp::Reverse;
use std::marker::PhantomData;

use differential_dataflow::lattice::Lattice;
use mz_expr::{EvalError, Id, MapFilterProject, MirScalarExpr, TableFunc};
use mz_repr::{Diff, Row};
use timely::PartialOrder;

use crate::plan::interpret::{BoundedLattice, Context, Interpreter};
use crate::plan::join::JoinPlan;
use crate::plan::reduce::{KeyValPlan, ReducePlan};
use crate::plan::threshold::ThresholdPlan;
use crate::plan::top_k::TopKPlan;
use crate::plan::{AvailableCollections, GetPlan};

/// Represents a boolean physical monotonicity property, where the bottom value
/// is true (i.e., physically monotonic) and the top value is false (i.e. not
/// physically monotonic).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PhysicallyMonotonic(bool);

impl BoundedLattice for PhysicallyMonotonic {
    fn top() -> Self {
        PhysicallyMonotonic(false)
    }

    fn bottom() -> Self {
        PhysicallyMonotonic(true)
    }
}

impl Lattice for PhysicallyMonotonic {
    fn join(&self, other: &Self) -> Self {
        PhysicallyMonotonic(self.0 && other.0)
    }

    fn meet(&self, other: &Self) -> Self {
        PhysicallyMonotonic(self.0 || other.0)
    }
}

impl PartialOrder for PhysicallyMonotonic {
    fn less_equal(&self, other: &Self) -> bool {
        // We employ `Reverse` ordering for `bool` here to be consistent with
        // the choice of `top()` being false and `bottom()` being true.
        Reverse::<bool>(self.0) <= Reverse::<bool>(other.0)
    }
}

/// Provides a concrete implementation of an interpreter that determines if
/// the output of `Plan` expressions is physically monotonic in a single-time
/// dataflow, potentially taking into account judgments about its inputs. We
/// note that in a single-time dataflow, expressions in non-recursive contexts
/// (i.e., outside of `LetRec` values) process streams that are at a minimum
/// logically monotonic, i.e., may contain retractions but would cease to do
/// so if consolidated. Detecting physical monotonicity, i.e., the absence
/// of retractions in a stream, enables us to disable forced consolidation
/// whenever possible.
#[derive(Debug)]
pub struct SingleTimeMonotonic<T = mz_repr::Timestamp> {
    _phantom: PhantomData<T>,
}

impl<T> SingleTimeMonotonic<T> {
    /// Instantiates an interpreter for single-time physical monotonicity
    /// analysis.
    pub fn new() -> Self {
        SingleTimeMonotonic {
            _phantom: Default::default(),
        }
    }
}

impl<T> Interpreter<T> for SingleTimeMonotonic<T> {
    type Domain = PhysicallyMonotonic;

    fn constant(
        &self,
        _ctx: &Context<Self::Domain>,
        _rows: &Result<Vec<(Row, T, Diff)>, EvalError>,
    ) -> Self::Domain {
        PhysicallyMonotonic(false)
    }

    fn get(
        &self,
        _ctx: &Context<Self::Domain>,
        _id: &Id,
        _keys: &AvailableCollections,
        _plan: &GetPlan,
    ) -> Self::Domain {
        PhysicallyMonotonic(false)
    }

    fn mfp(
        &self,
        _ctx: &Context<Self::Domain>,
        _input: Self::Domain,
        _mfp: &MapFilterProject,
        _input_key_val: &Option<(Vec<MirScalarExpr>, Option<Row>)>,
    ) -> Self::Domain {
        PhysicallyMonotonic(false)
    }

    fn flat_map(
        &self,
        _ctx: &Context<Self::Domain>,
        _input: Self::Domain,
        _func: &TableFunc,
        _exprs: &Vec<MirScalarExpr>,
        _mfp: &MapFilterProject,
        _input_key: &Option<Vec<MirScalarExpr>>,
    ) -> Self::Domain {
        PhysicallyMonotonic(false)
    }

    fn join(
        &self,
        _ctx: &Context<Self::Domain>,
        _inputs: Vec<Self::Domain>,
        _plan: &JoinPlan,
    ) -> Self::Domain {
        PhysicallyMonotonic(false)
    }

    fn reduce(
        &self,
        _ctx: &Context<Self::Domain>,
        _input: Self::Domain,
        _key_val_plan: &KeyValPlan,
        _plan: &ReducePlan,
        _input_key: &Option<Vec<MirScalarExpr>>,
    ) -> Self::Domain {
        PhysicallyMonotonic(false)
    }

    fn top_k(
        &self,
        _ctx: &Context<Self::Domain>,
        _input: Self::Domain,
        _top_k_plan: &TopKPlan,
    ) -> Self::Domain {
        PhysicallyMonotonic(false)
    }

    fn negate(&self, _ctx: &Context<Self::Domain>, _input: Self::Domain) -> Self::Domain {
        PhysicallyMonotonic(false)
    }

    fn threshold(
        &self,
        _ctx: &Context<Self::Domain>,
        _input: Self::Domain,
        _threshold_plan: &ThresholdPlan,
    ) -> Self::Domain {
        PhysicallyMonotonic(false)
    }

    fn union(&self, _ctx: &Context<Self::Domain>, _inputs: Vec<Self::Domain>) -> Self::Domain {
        PhysicallyMonotonic(false)
    }

    fn arrange_by(
        &self,
        _ctx: &Context<Self::Domain>,
        _input: Self::Domain,
        _forms: &AvailableCollections,
        _input_key: &Option<Vec<MirScalarExpr>>,
        _input_mfp: &MapFilterProject,
    ) -> Self::Domain {
        PhysicallyMonotonic(false)
    }
}
