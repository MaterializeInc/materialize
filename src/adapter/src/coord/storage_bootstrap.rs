// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, BTreeSet};

/// Orders storage registration batches by catalog prerequisites, co-registering versions and
/// shared-shard aliases. Only dependencies within a strongly connected component may share a batch.
///
/// `dependencies` includes non-storage items so paths through views and functions retain their
/// ordering. `collections` supplies (collection ID, catalog item ID, committed shard ID) triples.
/// Every item and prerequisite must be present in `dependencies`. Ordering within a batch is left
/// to storage, which knows the primary ordering of table and MV versions.
pub(super) fn registration_batches<I: Copy + Ord, G, S: Ord>(
    dependencies: &BTreeMap<I, BTreeSet<I>>,
    collections: impl IntoIterator<Item = (G, I, S)>,
) -> Vec<Vec<G>> {
    let items: BTreeMap<_, _> = dependencies
        .keys()
        .enumerate()
        .map(|(index, id)| (*id, index))
        .collect();
    let mut edges = vec![Vec::new(); items.len()];
    for (id, deps) in dependencies {
        edges[items[id]].extend(deps.iter().map(|dep| items[dep]));
    }

    let mut shard_items = BTreeMap::new();
    let mut item_collections: Vec<Vec<G>> = (0..items.len()).map(|_| Vec::new()).collect();
    for (gid, item, shard) in collections {
        let item = items[&item];
        item_collections[item].push(gid);
        let representative = *shard_items.entry(shard).or_insert(item);
        if item != representative {
            // Bidirectional edges make all aliases one registration unit without a clique.
            // A pending replacement adds its FOR and query dependencies to this unit. Those
            // dependencies can induce cycles between otherwise unrelated shards.
            edges[item].push(representative);
            edges[representative].push(item);
        }
    }

    // Iterative Kosaraju avoids a call-stack limit on catalog depth. Both passes visit each
    // vertex and edge once. Edges point from an item to its prerequisites.
    let mut reverse_edges = vec![Vec::new(); items.len()];
    for (item, deps) in edges.iter().enumerate() {
        for dep in deps {
            reverse_edges[*dep].push(item);
        }
    }
    let mut visited = vec![false; items.len()];
    let mut finished = Vec::with_capacity(items.len());
    let mut stack = Vec::new();
    for item in 0..items.len() {
        if visited[item] {
            continue;
        }
        visited[item] = true;
        stack.push((item, 0));
        while let Some((item, next)) = stack.last_mut() {
            if let Some(dep) = edges[*item].get(*next).copied() {
                *next += 1;
                if !visited[dep] {
                    visited[dep] = true;
                    stack.push((dep, 0));
                }
            } else {
                finished.push(*item);
                stack.pop();
            }
        }
    }

    let mut component_of = vec![None; items.len()];
    let mut components = Vec::new();
    let mut stack = Vec::new();
    for item in finished.into_iter().rev() {
        if component_of[item].is_some() {
            continue;
        }
        let component_id = components.len();
        let mut component = Vec::new();
        component_of[item] = Some(component_id);
        stack.push(item);
        while let Some(item) = stack.pop() {
            component.push(item);
            for dep in &reverse_edges[item] {
                if component_of[*dep].is_none() {
                    component_of[*dep] = Some(component_id);
                    stack.push(*dep);
                }
            }
        }
        components.push(component);
    }

    // Kosaraju emits dependents before prerequisites, so reverse iteration can assign each
    // component a batch after its external storage prerequisites. Non-storage items propagate
    // ordering without adding registration rounds. Downstream components remain separate even
    // when a cycle prevents ordering the members of an upstream component.
    // In read-only bootstrap this also lets storage recover leased input readability before
    // registering outputs. No ordering is inferred from committed compaction permissions.
    let mut next_layers = vec![0; components.len()];
    let mut batches: Vec<Vec<G>> = Vec::new();
    for (component_id, component) in components.into_iter().enumerate().rev() {
        let layer = component
            .iter()
            .flat_map(|item| &edges[*item])
            .map(|dep| component_of[*dep].expect("all items have a component"))
            .filter(|dep| *dep != component_id)
            .map(|dep| next_layers[dep])
            .max()
            .unwrap_or(0);
        let has_collections = component
            .iter()
            .any(|item| !item_collections[*item].is_empty());
        next_layers[component_id] = layer + usize::from(has_collections);
        batches.resize_with(batches.len().max(layer + 1), Vec::new);
        for item in component {
            batches[layer].append(&mut item_collections[item]);
        }
    }
    batches.retain(|batch| !batch.is_empty());
    batches
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use super::registration_batches;

    #[mz_ore::test]
    fn prerequisites_versions_and_pending_aliases() {
        let dependencies = BTreeMap::from([
            ("table", BTreeSet::new()),
            ("view", BTreeSet::from(["table"])),
            ("a", BTreeSet::from(["view"])),
            ("b", BTreeSet::from(["table"])),
            ("ar", BTreeSet::from(["a", "b"])),
            ("downstream", BTreeSet::from(["ar"])),
        ]);
        let batches = registration_batches(
            &dependencies,
            [
                ("table_v1", "table", "table_shard"),
                ("table_v2", "table", "table_shard"),
                ("a_v1", "a", "a_shard"),
                ("a_v2", "a", "a_shard"),
                ("ar", "ar", "a_shard"),
                ("b", "b", "b_shard"),
                ("downstream", "downstream", "downstream_shard"),
            ],
        );
        let batches: Vec<BTreeSet<_>> = batches
            .into_iter()
            .map(|batch| batch.into_iter().collect())
            .collect();
        assert_eq!(
            batches,
            vec![
                BTreeSet::from(["table_v1", "table_v2"]),
                BTreeSet::from(["b"]),
                BTreeSet::from(["a_v1", "a_v2", "ar"]),
                BTreeSet::from(["downstream"]),
            ]
        );
    }

    #[mz_ore::test]
    fn cross_dependent_replacements_leave_downstream_separate() {
        // The catalog DAG is legal. Only grouping ar with a and br with b induces a cycle.
        let dependencies = BTreeMap::from([
            ("table", BTreeSet::new()),
            ("a", BTreeSet::from(["table"])),
            ("b", BTreeSet::from(["table"])),
            ("ar", BTreeSet::from(["a", "b"])),
            ("br", BTreeSet::from(["b", "a"])),
            ("view", BTreeSet::from(["ar", "br"])),
            ("downstream", BTreeSet::from(["view"])),
            ("further_downstream", BTreeSet::from(["downstream"])),
        ]);
        let batches = registration_batches(
            &dependencies,
            [
                ("table", "table", "table_shard"),
                ("a", "a", "a_shard"),
                ("ar", "ar", "a_shard"),
                ("b", "b", "b_shard"),
                ("br", "br", "b_shard"),
                ("downstream", "downstream", "downstream_shard"),
                ("further_downstream", "further_downstream", "further_shard"),
            ],
        );
        let batches: Vec<BTreeSet<_>> = batches
            .into_iter()
            .map(|batch| batch.into_iter().collect())
            .collect();
        assert_eq!(
            batches,
            vec![
                BTreeSet::from(["table"]),
                BTreeSet::from(["a", "ar", "b", "br"]),
                BTreeSet::from(["downstream"]),
                BTreeSet::from(["further_downstream"]),
            ]
        );
    }
}
