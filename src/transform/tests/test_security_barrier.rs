// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Tests for security barrier views
//! ([`mz_transform::dataflow::optimize_dataflow_filters_inner`]).
//!
//! The fixture in each test is a two-object dataflow shaped like the access
//! control pattern: `mine` filters `orders` down to one tenant, and `q` is an
//! untrusted consumer that reads `mine`. Column `#0` is the secret the reader
//! must not learn and `#1` is the tenant the view filters on.

use std::collections::{BTreeMap, BTreeSet};

use mz_expr::{Id, MirRelationExpr, MirScalarExpr};
use mz_expr_parser::{TestCatalog, try_parse_mir};
use mz_repr::explain::ExplainConfig;
use mz_repr::{GlobalId, SqlColumnType, SqlRelationType, SqlScalarType};
use mz_transform::dataflow::optimize_dataflow_filters_inner;

/// A fallible predicate. `DivInt64` errors on a zero divisor, and the error
/// message names the row, so this is the shape a reader uses as an oracle.
const LEAKY: &str = "(100 / #0) > 0";

/// An infallible predicate over the same secret column.
const LEAKPROOF: &str = "#0 = 5";

struct Fixture {
    catalog: TestCatalog,
    orders: GlobalId,
    /// `(id, plan)` for `mine` then `q`, in dependency order.
    objects: Vec<(GlobalId, MirRelationExpr)>,
}

/// Builds the two-object dataflow with `user_predicate` applied by the consumer.
fn fixture(user_predicate: &str) -> Fixture {
    let mut catalog = TestCatalog::default();

    let column_types = (0..2)
        .map(|_| SqlColumnType {
            scalar_type: SqlScalarType::Int64,
            nullable: true,
        })
        .collect();
    let orders = catalog
        .insert(
            "orders",
            vec!["secret".into(), "tenant".into()],
            SqlRelationType {
                column_types,
                keys: vec![],
            },
            false,
        )
        .unwrap();

    let specs = [
        ("mine", "Filter (#1 = 7)\n  Get orders".to_string()),
        ("q", format!("Filter ({user_predicate})\n  Get mine")),
    ];
    let objects = specs
        .iter()
        .map(|(name, spec)| {
            let rel = try_parse_mir(&catalog, spec).unwrap();
            let cols = (0..rel.arity()).map(|i| format!("c{i}")).collect();
            let id = catalog.insert(name, cols, rel.sql_typ(), false).unwrap();
            (id, rel)
        })
        .collect();

    Fixture {
        catalog,
        orders,
        objects,
    }
}

/// Runs cross-view filter pushdown, treating `barriers` as security barriers.
/// Returns the predicates that reached the `orders` source import.
fn push_filters(f: &mut Fixture, barriers: &BTreeSet<GlobalId>) -> Vec<String> {
    let mut predicates = BTreeMap::<Id, BTreeSet<MirScalarExpr>>::new();
    optimize_dataflow_filters_inner(
        f.objects
            .iter_mut()
            .map(|(id, rel)| (Id::Global(*id), rel))
            .rev(),
        &mut predicates,
        barriers,
    )
    .unwrap();
    predicates
        .get(&Id::Global(f.orders))
        .map(|list| list.iter().map(|p| p.to_string()).collect())
        .unwrap_or_default()
}

fn plan(f: &Fixture, which: usize) -> String {
    f.objects[which]
        .1
        .debug_explain(&ExplainConfig::default(), Some(&f.catalog))
        .trim()
        .to_string()
}

/// Without a barrier, a fallible reader predicate is spliced into the view's
/// own plan and propagates to the base collection. This pins the exposure the
/// barrier exists to close; it is expected to change if the default changes.
#[mz_ore::test]
#[cfg_attr(miri, ignore)] // can't call foreign function `rust_psm_stack_pointer` on OS `linux`
fn unprotected_view_leaks_reader_predicate() {
    let mut f = fixture(LEAKY);
    let at_source = push_filters(&mut f, &BTreeSet::new());

    assert_eq!(
        plan(&f, 0),
        "Filter (#1 = 7) AND ((100 / #0) > 0)\n  Get orders"
    );
    assert_eq!(at_source, vec!["(#1 = 7)", "((100 / #0) > 0)"]);
}

/// With a barrier, the fallible predicate stays above the consumer's `Get`, so
/// it is evaluated only against rows the tenant filter already admitted, and it
/// never reaches the base collection.
#[mz_ore::test]
#[cfg_attr(miri, ignore)] // can't call foreign function `rust_psm_stack_pointer` on OS `linux`
fn barrier_view_retains_reader_predicate() {
    let mut f = fixture(LEAKY);
    let barriers = BTreeSet::from([f.objects[0].0]);
    let at_source = push_filters(&mut f, &barriers);

    assert_eq!(plan(&f, 0), "Filter (#1 = 7)\n  Get orders");
    assert_eq!(plan(&f, 1), "Filter ((100 / #0) > 0)\n  Get mine");
    assert_eq!(at_source, vec!["(#1 = 7)"]);
}

/// A barrier is not a wall. An infallible predicate has no channel to leak
/// through, so it still crosses and still reaches the source import, where
/// persist filter pushdown can use it.
#[mz_ore::test]
#[cfg_attr(miri, ignore)] // can't call foreign function `rust_psm_stack_pointer` on OS `linux`
fn barrier_view_admits_leakproof_predicate() {
    let mut f = fixture(LEAKPROOF);
    let barriers = BTreeSet::from([f.objects[0].0]);
    let at_source = push_filters(&mut f, &barriers);

    assert_eq!(plan(&f, 0), "Filter (#0 = 5) AND (#1 = 7)\n  Get orders");
    assert_eq!(at_source, vec!["(#0 = 5)", "(#1 = 7)"]);
}
