// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use proptest::prelude::any;
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};

use mz_expr::{MapFilterProject, MirScalarExpr};
use mz_proto::{ProtoType, RustType, TryFromProtoError};
use mz_repr::{Datum, RelationType};

include!(concat!(env!("OUT_DIR"), "/mz_storage.types.transforms.rs"));

// TODO: change contract to ensure that the operator is always applied to
// streams of rows
/// In-place restrictions that can be made to rows.
///
/// These fields indicate *optional* information that may applied to
/// streams of rows. Any row that does not satisfy all predicates may
/// be discarded, and any column not listed in the projection may be
/// replaced by a default value.
///
/// The intended order of operations is that the predicates are first
/// applied, and columns not in projection can then be overwritten with
/// default values. This allows the projection to avoid capturing columns
/// used by the predicates but not otherwise required.
#[derive(Arbitrary, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize, Hash)]
pub struct LinearOperator {
    /// Rows that do not pass all predicates may be discarded.
    #[proptest(strategy = "proptest::collection::vec(any::<MirScalarExpr>(), 0..2)")]
    pub predicates: Vec<MirScalarExpr>,
    /// Columns not present in `projection` may be replaced with
    /// default values.
    pub projection: Vec<usize>,
}

impl RustType<ProtoLinearOperator> for LinearOperator {
    fn into_proto(&self) -> ProtoLinearOperator {
        ProtoLinearOperator {
            predicates: self.predicates.into_proto(),
            projection: self.projection.into_proto(),
        }
    }

    fn from_proto(proto: ProtoLinearOperator) -> Result<Self, TryFromProtoError> {
        Ok(LinearOperator {
            predicates: proto.predicates.into_rust()?,
            projection: proto.projection.into_rust()?,
        })
    }
}

impl LinearOperator {
    /// Reports whether this linear operator is trivial when applied to an
    /// input of the specified arity.
    pub fn is_trivial(&self, arity: usize) -> bool {
        self.predicates.is_empty() && self.projection.iter().copied().eq(0..arity)
    }

    /// Converts linear operators to MapFilterProject instances.
    ///
    /// This method produces a `MapFilterProject` instance that first applies any predicates,
    /// and then introduces `Datum::Dummy` literals in columns that are not demanded.
    /// The `RelationType` is required so that we can fill in the correct type of `Datum::Dummy`.
    pub fn to_mfp(self, typ: &RelationType) -> MapFilterProject {
        let LinearOperator {
            predicates,
            projection,
        } = self;

        let arity = typ.arity();
        let mut dummies = Vec::new();
        let mut demand_projection = Vec::new();
        for (column, typ) in typ.column_types.iter().enumerate() {
            if projection.contains(&column) {
                demand_projection.push(column);
            } else {
                demand_projection.push(arity + dummies.len());
                dummies.push(MirScalarExpr::literal_ok(
                    Datum::Dummy,
                    typ.scalar_type.clone(),
                ));
            }
        }

        // First filter, then introduce and reposition `Datum::Dummy` values.
        MapFilterProject::new(arity)
            .filter(predicates)
            .map(dummies)
            .project(demand_projection)
    }
}
