// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Demonstrates Step B of the integer repr-type unification: same-sign integer
//! *widening* casts are eliminated during HIR->MIR lowering (because they are
//! the identity on the unified `Datum::Int`/`UInt`), while *narrowing* casts are
//! preserved (they perform a real range check).
//!
//! Runs fully in-process: builds HIR cast expressions and lowers them with and
//! without cast elimination, printing the resulting MIR. No database required.

use mz_expr::MirScalarExpr;
use mz_expr::func::{CastInt32ToInt64, CastInt64ToInt32, UnaryFunc};
use mz_repr::{Datum, SqlScalarType};
use mz_sql::plan::{HirScalarExpr, HirToMirConfig};

fn lower(hir: HirScalarExpr, eliminate: bool) -> MirScalarExpr {
    hir.lower_uncorrelated(HirToMirConfig {
        enable_cast_elimination: eliminate,
        ..Default::default()
    })
    .expect("lowering an uncorrelated cast should succeed")
}

#[test]
fn int_widening_cast_is_eliminated() {
    // CAST(5::int4 AS int8) -- a same-sign widening cast.
    let widening = || {
        HirScalarExpr::literal(Datum::Int(5), SqlScalarType::Int32)
            .call_unary(UnaryFunc::CastInt32ToInt64(CastInt32ToInt64))
    };

    let with = lower(widening(), true);
    let without = lower(widening(), false);

    println!("\n== CAST(5::int4 AS int8)  (widening) ==");
    println!("  cast elimination ON : {with}");
    println!("  cast elimination OFF: {without}");

    // With elimination, the cast is gone: the plan is just the bare literal.
    assert!(
        matches!(with, MirScalarExpr::Literal(..)),
        "widening cast should be eliminated, got: {with}"
    );
    // Without elimination, the cast survives as a CallUnary.
    assert!(
        matches!(without, MirScalarExpr::CallUnary { .. }),
        "with elimination off the cast should remain, got: {without}"
    );
}

#[test]
fn int_narrowing_cast_is_preserved() {
    // CAST(5::int8 AS int4) -- a narrowing cast that performs a real range check.
    let narrowing = || {
        HirScalarExpr::literal(Datum::Int(5), SqlScalarType::Int64)
            .call_unary(UnaryFunc::CastInt64ToInt32(CastInt64ToInt32))
    };

    let with = lower(narrowing(), true);

    println!("\n== CAST(5::int8 AS int4)  (narrowing) ==");
    println!("  cast elimination ON : {with}");

    // Even with elimination enabled, the narrowing cast must remain.
    assert!(
        matches!(with, MirScalarExpr::CallUnary { .. }),
        "narrowing cast must NOT be eliminated, got: {with}"
    );
}
