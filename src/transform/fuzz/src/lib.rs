// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! libFuzzer bindings for the shared MIR generators and oracle.
//!
//! The generators and the `FoldConstants` oracle live in
//! [`mz_transform::mirgen`], because two consumers share them: these fuzz
//! targets, which check MIR-to-MIR equivalence under the optimizer, and the
//! compute surface suite, which renders the same generated plans on a real
//! replica and checks them against the same oracle. This crate contributes only
//! the libFuzzer end: an [`mirgen::Entropy`] implementation over `Unstructured`,
//! plus thin wrappers that pin the generic generators to it.
//!
//! # Byte-consumption fidelity
//!
//! [`FuzzEntropy`] delegates every method one-to-one to the `Unstructured` call
//! the generators previously made inline, in the same order. That is deliberate
//! and load-bearing: a corpus entry is just a byte string, and which plan it
//! denotes depends on how many bytes each draw consumes. Release qualification
//! carries a minimized corpus between runs (`--corpus-sync`), so changing the
//! draw sequence would silently remap every stored entry to a different plan and
//! throw away the accumulated coverage.

use libfuzzer_sys::arbitrary::{self, Arbitrary, Unstructured};
use mz_expr::MirRelationExpr;
use mz_transform::mirgen::{self, Entropy};

pub use mz_transform::mirgen::{
    Ty, apply_recursively, fold_to_multiset, nullable_relation_type, optimize,
};

/// An [`Entropy`] source over libFuzzer's `Unstructured`.
///
/// Each method is a direct delegation, so the byte consumption matches what the
/// generators drew when they called `Unstructured` inline (see the module docs).
#[derive(Debug)]
pub struct FuzzEntropy<'a, 'b>(pub &'b mut Unstructured<'a>);

impl Entropy for FuzzEntropy<'_, '_> {
    type Error = arbitrary::Error;

    fn int_in_range_u8(&mut self, range: std::ops::RangeInclusive<u8>) -> arbitrary::Result<u8> {
        self.0.int_in_range(range)
    }

    fn int_in_range_usize(
        &mut self,
        range: std::ops::RangeInclusive<usize>,
    ) -> arbitrary::Result<usize> {
        self.0.int_in_range(range)
    }

    fn int_in_range_i64(&mut self, range: std::ops::RangeInclusive<i64>) -> arbitrary::Result<i64> {
        self.0.int_in_range(range)
    }

    fn ratio(&mut self, numerator: u8, denominator: u8) -> arbitrary::Result<bool> {
        self.0.ratio(numerator, denominator)
    }

    fn any_bool(&mut self) -> arbitrary::Result<bool> {
        bool::arbitrary(self.0)
    }

    fn any_i32(&mut self) -> arbitrary::Result<i32> {
        i32::arbitrary(self.0)
    }

    fn any_i64(&mut self) -> arbitrary::Result<i64> {
        i64::arbitrary(self.0)
    }
}

/// Pick a random column type.
pub fn rand_ty(u: &mut Unstructured) -> arbitrary::Result<Ty> {
    mirgen::rand_ty(&mut FuzzEntropy(u))
}

/// A well-typed scalar expression of type `ty` over columns `schema`.
/// See [`mirgen::gen_scalar`].
pub fn gen_scalar(
    u: &mut Unstructured,
    ty: Ty,
    schema: &[Ty],
    depth: u32,
) -> arbitrary::Result<mz_expr::MirScalarExpr> {
    mirgen::gen_scalar(&mut FuzzEntropy(u), ty, schema, depth)
}

/// A random literal `Constant` collection with its column schema.
/// See [`mirgen::gen_constant`].
pub fn gen_constant(u: &mut Unstructured) -> arbitrary::Result<(MirRelationExpr, Vec<Ty>)> {
    mirgen::gen_constant(&mut FuzzEntropy(u))
}

/// A random schema of 1-3 columns. See [`mirgen::gen_schema`].
pub fn gen_schema(u: &mut Unstructured) -> arbitrary::Result<Vec<Ty>> {
    mirgen::gen_schema(&mut FuzzEntropy(u))
}

/// Random rows matching `schema`. See [`mirgen::gen_rows`].
pub fn gen_rows(
    u: &mut Unstructured,
    schema: &[Ty],
) -> arbitrary::Result<Vec<Vec<mz_repr::Datum<'static>>>> {
    mirgen::gen_rows(&mut FuzzEntropy(u), schema)
}

/// A random relation over the bug-rich relational operators, its column schema,
/// and whether its multiplicities are guaranteed non-negative.
///
/// `leaf` produces the base relations, so a target can root the plan at literal
/// `Constant`s or at opaque `Get`s. See [`mirgen::gen_rel`].
pub fn gen_rel<F>(
    u: &mut Unstructured,
    depth: u32,
    leaf: &mut F,
) -> arbitrary::Result<(MirRelationExpr, Vec<Ty>, bool)>
where
    F: FnMut(&mut Unstructured) -> arbitrary::Result<(MirRelationExpr, Vec<Ty>)>,
{
    // Re-borrow the `Unstructured` out of the wrapper for the caller's `leaf`, so
    // a target's leaf closure keeps its plain `Unstructured` signature.
    mirgen::gen_rel(&mut FuzzEntropy(u), depth, &mut |e: &mut FuzzEntropy| {
        leaf(e.0)
    })
}
