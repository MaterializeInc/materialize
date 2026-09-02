// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Tests for cross-view demand pushdown
//! ([`mz_transform::dataflow::optimize_dataflow_demand_inner`]).

use std::collections::{BTreeMap, BTreeSet};

use mz_expr::{Id, MirRelationExpr};
use mz_expr_parser::{TestCatalog, try_parse_mir};
use mz_repr::explain::ExplainConfig;
use mz_repr::{GlobalId, SqlColumnType, SqlRelationType, SqlScalarType};
use mz_transform::dataflow::optimize_dataflow_demand_inner;

/// Registers a source of `arity` nullable integer columns in the catalog.
fn define_source(catalog: &mut TestCatalog, name: &str, arity: usize) -> GlobalId {
    let column_types = (0..arity)
        .map(|_| SqlColumnType {
            scalar_type: SqlScalarType::Int32,
            nullable: true,
        })
        .collect();
    let cols = (0..arity).map(|i| format!("c{i}")).collect();
    let typ = SqlRelationType {
        column_types,
        keys: vec![],
    };
    catalog.insert(name, cols, typ, false).unwrap()
}

/// Parses each view against the catalog and registers it, so later views can
/// refer to earlier ones by name.
fn build_dataflow(
    catalog: &mut TestCatalog,
    views: &[(&str, &str)],
) -> Vec<(GlobalId, MirRelationExpr)> {
    views
        .iter()
        .map(|(name, spec)| {
            let rel = try_parse_mir(catalog, spec).unwrap();
            let cols = (0..rel.arity()).map(|i| format!("c{i}")).collect();
            let id = catalog.insert(name, cols, rel.sql_typ(), false).unwrap();
            (id, rel)
        })
        .collect()
}

/// Runs demand pushdown over the dataflow, with full demand on the last view.
/// Returns the demand pushed down to sources outside the dataflow.
fn push_demand(dataflow: &mut [(GlobalId, MirRelationExpr)]) -> BTreeMap<Id, BTreeSet<usize>> {
    let mut demand = BTreeMap::new();
    let (last_id, last_rel) = dataflow.last().expect("nonempty dataflow");
    demand.insert(Id::Global(*last_id), (0..last_rel.arity()).collect());
    optimize_dataflow_demand_inner(
        dataflow
            .iter_mut()
            .map(|(id, rel)| (Id::Global(*id), rel))
            .rev(),
        &mut demand,
    )
    .unwrap();
    demand
}

fn assert_view(catalog: &TestCatalog, rel: &MirRelationExpr, expected: &str) {
    let actual = rel.debug_explain(&ExplainConfig::default(), Some(catalog));
    assert_eq!(actual.trim(), expected.trim());
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // can't call foreign function `rust_psm_stack_pointer` on OS `linux`
fn demand_pushdown_across_projects() {
    let mut catalog = TestCatalog::default();
    let x = define_source(&mut catalog, "x", 3);

    let mut dataflow = build_dataflow(
        &mut catalog,
        &[
            ("a", "Project (#2, #1)\n  Get x"),
            ("b", "Project (#0)\n  Get a"),
        ],
    );

    let demand = push_demand(&mut dataflow);
    assert_eq!(demand[&Id::Global(x)], BTreeSet::from([2]));

    assert_view(&catalog, &dataflow[0].1, "Project (#2)\n  Get x");
    assert_view(&catalog, &dataflow[1].1, "Get a");
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // can't call foreign function `rust_psm_stack_pointer` on OS `linux`
fn demand_pushdown_across_map_and_join() {
    let mut catalog = TestCatalog::default();
    let x = define_source(&mut catalog, "x", 3);

    let mut dataflow = build_dataflow(
        &mut catalog,
        &[
            ("a", "Map (neg_int32(#2))\n  Get x"),
            ("b", "Project (#2)\n  Get a"),
            (
                "c",
                "Project (#1, #4)\n  Join on=(#1 = #4)\n    Get a\n    Get b",
            ),
        ],
    );

    let demand = push_demand(&mut dataflow);
    assert_eq!(demand[&Id::Global(x)], BTreeSet::from([1, 2]));

    assert_view(
        &catalog,
        &dataflow[0].1,
        "Map ()\n  Project (#1, #2)\n    Get x",
    );
    assert_view(&catalog, &dataflow[1].1, "Project (#1)\n  Get a");
    assert_view(
        &catalog,
        &dataflow[2].1,
        "Join on=(#0 = #1)\n  Project (#0)\n    Get a\n  Get b",
    );
}
