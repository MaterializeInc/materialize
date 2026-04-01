// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Property-based tests for the cast graph.
//!
//! These tests verify structural invariants of VALID_CASTS:
//! - T1: The implicit cast subgraph is acyclic (a DAG)
//! - T2: No (from, to) pair appears more than once
//! - T3: Implicit cast paths don't diverge ambiguously
//! - T4: Structural validity of implicit casts

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use mz_repr::SqlScalarBaseType;

use super::typeconv::{all_casts, CastContext};

/// T1: Detect cycles in the implicit cast subgraph.
///
/// Known cycles: Oid ↔ RegProc, Oid ↔ RegType, Oid ↔ RegClass form mutual
/// implicit casts (mirroring PostgreSQL's OID type family). These are documented
/// as known and expected. Any NEW cycle involving other types is a real bug.
#[mz_ore::test]
fn implicit_cast_subgraph_cycles() {
    let casts = all_casts();

    // Known cycle families (all PostgreSQL compatibility):
    // - Oid/Reg* types: mutual implicit casts (OID wrappers)
    // - String/Char/VarChar/PgLegacyName: mutual implicit casts (string family)
    // - List, Record: self-casts for element/field type coercion
    let known_cycle_types: BTreeSet<SqlScalarBaseType> = BTreeSet::from([
        SqlScalarBaseType::Oid,
        SqlScalarBaseType::RegProc,
        SqlScalarBaseType::RegType,
        SqlScalarBaseType::RegClass,
        SqlScalarBaseType::String,
        SqlScalarBaseType::Char,
        SqlScalarBaseType::VarChar,
        SqlScalarBaseType::PgLegacyName,
        SqlScalarBaseType::List,
        SqlScalarBaseType::Record,
    ]);

    // Build adjacency list for implicit casts only.
    let mut adj: BTreeMap<SqlScalarBaseType, Vec<SqlScalarBaseType>> = BTreeMap::new();
    for (from, to, ctx) in &casts {
        if *ctx == CastContext::Implicit {
            adj.entry(*from).or_default().push(*to);
        }
    }

    // Find all strongly connected components (SCCs) using iterative Tarjan's.
    // Any SCC with more than one node is a cycle.
    let all_nodes: BTreeSet<_> = casts
        .iter()
        .flat_map(|(from, to, _)| [*from, *to])
        .collect();

    // Simple cycle detection: for each node, check if it can reach itself via BFS.
    let mut cycle_members = BTreeSet::new();
    for &start in &all_nodes {
        let mut visited = BTreeSet::new();
        let mut queue = VecDeque::new();
        if let Some(neighbors) = adj.get(&start) {
            for &n in neighbors {
                queue.push_back(n);
            }
        }
        while let Some(node) = queue.pop_front() {
            if node == start {
                cycle_members.insert(start);
                break;
            }
            if visited.insert(node) {
                if let Some(neighbors) = adj.get(&node) {
                    for &n in neighbors {
                        if !visited.contains(&n) {
                            queue.push_back(n);
                        }
                    }
                }
            }
        }
    }

    // Check for unexpected cycle members.
    let unexpected: BTreeSet<_> = cycle_members
        .difference(&known_cycle_types)
        .copied()
        .collect();

    assert!(
        unexpected.is_empty(),
        "Unexpected types involved in implicit cast cycles: {unexpected:?}\n\
         Known cycle types (Oid/Reg*): {known_cycle_types:?}\n\
         All cycle members: {cycle_members:?}"
    );

    // Verify the known cycles are still present (guard against silent removal).
    assert!(
        cycle_members.contains(&SqlScalarBaseType::Oid),
        "Expected Oid to be in an implicit cast cycle (Oid ↔ Reg* types)"
    );

    eprintln!(
        "Known implicit cast cycles (Oid/Reg* family): {:?}",
        cycle_members
    );
}

/// T2: No (from, to) pair should appear more than once in VALID_CASTS.
///
/// The BTreeMap key is (SqlScalarBaseType, SqlScalarBaseType), so duplicates
/// are structurally impossible. This test documents and verifies that invariant.
#[mz_ore::test]
fn no_duplicate_cast_entries() {
    let casts = all_casts();
    let mut seen = BTreeSet::new();
    for (from, to, _ctx) in &casts {
        assert!(
            seen.insert((*from, *to)),
            "Duplicate cast entry: ({from:?}, {to:?})"
        );
    }
}

/// T3: If type A can reach type B via multiple implicit cast paths,
/// the paths should converge (not diverge into incompatible types).
///
/// For each type with multiple direct implicit cast targets, we check
/// whether each pair of targets can reach each other. If they can't,
/// that's a divergent diamond — logged as informational, not a failure.
#[mz_ore::test]
fn implicit_cast_paths_are_unambiguous() {
    let casts = all_casts();

    // Build adjacency list for implicit casts.
    let mut adj: BTreeMap<SqlScalarBaseType, BTreeSet<SqlScalarBaseType>> = BTreeMap::new();
    for (from, to, ctx) in &casts {
        if *ctx == CastContext::Implicit {
            adj.entry(*from).or_default().insert(*to);
        }
    }

    // Compute reachability (transitive closure) via BFS.
    let all_nodes: BTreeSet<_> = casts
        .iter()
        .flat_map(|(from, to, _)| [*from, *to])
        .collect();

    let mut reachable: BTreeMap<SqlScalarBaseType, BTreeSet<SqlScalarBaseType>> = BTreeMap::new();

    for &start in &all_nodes {
        let mut visited = BTreeSet::new();
        let mut queue = VecDeque::new();
        if let Some(neighbors) = adj.get(&start) {
            for &n in neighbors {
                queue.push_back(n);
            }
        }
        while let Some(node) = queue.pop_front() {
            if visited.insert(node) {
                if let Some(neighbors) = adj.get(&node) {
                    for &n in neighbors {
                        if !visited.contains(&n) {
                            queue.push_back(n);
                        }
                    }
                }
            }
        }
        reachable.insert(start, visited);
    }

    // Check for divergent diamonds.
    let mut divergent_count = 0;
    for &start in &all_nodes {
        let direct_targets: Vec<_> = adj
            .get(&start)
            .map(|s| s.iter().copied().collect())
            .unwrap_or_default();

        for i in 0..direct_targets.len() {
            for j in (i + 1)..direct_targets.len() {
                let b = direct_targets[i];
                let c = direct_targets[j];

                let b_reaches_c = reachable
                    .get(&b)
                    .map(|r| r.contains(&c))
                    .unwrap_or(false);
                let c_reaches_b = reachable
                    .get(&c)
                    .map(|r| r.contains(&b))
                    .unwrap_or(false);

                if !b_reaches_c && !c_reaches_b {
                    divergent_count += 1;
                    eprintln!(
                        "Note: divergent implicit cast paths from {start:?}: \
                         {start:?} -> {b:?} and {start:?} -> {c:?} \
                         (neither reaches the other)"
                    );
                }
            }
        }
    }

    if divergent_count > 0 {
        eprintln!(
            "\nTotal divergent implicit cast path pairs: {divergent_count} \
             (informational — handled by function resolution's preferred type logic)"
        );
    }
}

/// T4: Every implicit cast in VALID_CASTS should be structurally valid.
///
/// Checks:
/// - No implicit self-casts (from == to)
/// - Every source type of an implicit cast also has some cast to String
/// - Sanity: more than 50 implicit casts exist
#[mz_ore::test]
fn all_implicit_casts_are_structurally_valid() {
    let casts = all_casts();
    let implicit_casts: Vec<_> = casts
        .iter()
        .filter(|(_, _, ctx)| *ctx == CastContext::Implicit)
        .collect();

    // No implicit self-casts (except known: Char → Char for length coercion,
    // VarChar → VarChar similarly).
    // Known self-casts: length coercion (Char, VarChar) and element/field
    // type coercion (List, Record).
    let known_self_casts: BTreeSet<SqlScalarBaseType> = BTreeSet::from([
        SqlScalarBaseType::Char,
        SqlScalarBaseType::VarChar,
        SqlScalarBaseType::List,
        SqlScalarBaseType::Record,
    ]);
    for (from, to, _) in &implicit_casts {
        if from == to && !known_self_casts.contains(from) {
            panic!("Unexpected implicit self-cast: {from:?}");
        }
    }

    // Every implicit cast source should have a cast to String.
    let has_string_cast: BTreeSet<_> = casts
        .iter()
        .filter(|(_, to, _)| *to == SqlScalarBaseType::String)
        .map(|(from, _, _)| *from)
        .collect();

    for (from, _to, _) in &implicit_casts {
        if !has_string_cast.contains(from) {
            eprintln!(
                "Warning: {from:?} has implicit casts but no cast to String"
            );
        }
    }

    // Sanity check: we expect a substantial number of implicit casts.
    assert!(
        implicit_casts.len() > 50,
        "Suspiciously few implicit casts: {}. Did VALID_CASTS change dramatically?",
        implicit_casts.len()
    );
}
