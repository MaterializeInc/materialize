// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! The scalar equality-saturation canonicalizer's shared substrate and
//! production entry point.
//!
//! [`node`] and [`lang`] define the scalar e-node and its embedding into the
//! combined relational+scalar `Language`; [`lower`] bridges a `MirScalarExpr`
//! into it; [`analysis`] tracks the per-class `could_error`/`literal` facts the
//! rewrite rules read as guards. The rules themselves and the saturation loop
//! live in `crate::eqsat::rules` and `crate::eqsat::scalar_saturate`, which run
//! them over the combined e-graph (`CombinedLang`) alongside the relational
//! rules. [`canonicalize_predicates`] is the single point through which
//! `mz-transform` reaches that machinery.
//!
//! See `doc/developer/design/20260624_eqsat/20260625_eqsat_scalar_expressions.md`.

pub mod analysis;
pub mod lang;
pub mod lower;
pub mod node;

use mz_expr::MirScalarExpr;
use mz_repr::ReprColumnType;

use crate::eqsat::scalar_saturate::canonicalize_combined;

/// Canonicalize a filter's `predicates`, selecting the per-predicate scalar
/// canonicalizer with `enable_eqsat_scalar`.
///
/// When set, step 1 of [`mz_expr::canonicalize::canonicalize_predicates`] is
/// performed by the equality-saturation scalar canonicalizer
/// ([`canonicalize_combined`]) instead of `MirScalarExpr::reduce`. When clear,
/// this delegates to the unmodified
/// [`mz_expr::canonicalize::canonicalize_predicates`], so the flag-off path is
/// byte-identical to calling that function directly.
///
/// This is the single injection point through which the `mz-transform` callers
/// reach the `mz-expr` predicate canonicalizer, since `mz-expr` cannot depend on
/// the eqsat engine.
pub(crate) fn canonicalize_predicates(
    predicates: &mut Vec<MirScalarExpr>,
    col_types: &[ReprColumnType],
    enable_eqsat_scalar: bool,
) {
    if enable_eqsat_scalar {
        mz_expr::canonicalize::canonicalize_predicates_with(
            predicates,
            col_types,
            Some(&|e: &mut MirScalarExpr, ct: &[ReprColumnType]| {
                *e = canonicalize_combined(e, ct);
            }),
        );
    } else {
        mz_expr::canonicalize::canonicalize_predicates(predicates, col_types);
    }
}
