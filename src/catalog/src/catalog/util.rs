// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::durable::{DurableCatalogError, FenceError};
use mz_controller_types::ClusterId;
use mz_ore::{assert_none, exit};
use mz_repr::RelationDesc;
use mz_sql::ast::{CreateIndexStatement, Ident, Raw, RawClusterName, RawItemName};
use mz_sql::names::FullItemName;
use mz_sql_parser::ast::display::AstDisplay;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Debug;
pub fn index_sql(
    index_name: String,
    cluster_id: ClusterId,
    view_name: FullItemName,
    view_desc: &RelationDesc,
    keys: &[usize],
) -> String {
    use mz_sql::ast::{Expr, Value};

    CreateIndexStatement::<Raw> {
        name: Some(Ident::new_unchecked(index_name)),
        on_name: RawItemName::Name(mz_sql::normalize::unresolve(view_name)),
        in_cluster: Some(RawClusterName::Resolved(cluster_id.to_string())),
        key_parts: Some(
            keys.iter()
                .map(|i| match view_desc.get_unambiguous_name(*i) {
                    Some(n) => Expr::Identifier(vec![Ident::new_unchecked(n.to_string())]),
                    _ => Expr::Value(Value::Number((i + 1).to_string())),
                })
                .collect(),
        ),
        with_options: vec![],
        if_not_exists: false,
    }
    .to_ast_string_stable()
}

/// Sort items in dependency order using topological sort.
///
/// # Panics
///
/// Panics if `key_fn` produces non-unique keys for the provided `items`.
/// Panics if there is a dependency cycle among the provided `items`.
pub fn sort_topological<T, K, FK, FD>(items: &mut Vec<T>, key_fn: FK, dependencies_fn: FD)
where
    T: Debug,
    K: Debug + Copy + Ord,
    FK: Fn(&T) -> K,
    FD: Fn(&T) -> BTreeSet<K>,
{
    let mut items_by_key = BTreeMap::new();
    for item in items.drain(..) {
        let key = key_fn(&item);
        let prev = items_by_key.insert(key, item);
        assert_none!(prev);
    }

    // For each item, the number of unprocessed dependencies.
    let mut in_degree = BTreeMap::<K, usize>::new();
    // For each item, the keys of items depending on it.
    let mut dependents = BTreeMap::<K, Vec<K>>::new();
    // Items that have no unprocessed dependencies.
    let mut ready = Vec::<K>::new();

    // Build the graph.
    for (&key, item) in &items_by_key {
        let mut dependencies = dependencies_fn(item);
        // Remove any dependencies not contained in `items`, as well as self-references.
        dependencies.retain(|dep| items_by_key.contains_key(dep) && *dep != key);

        in_degree.insert(key, dependencies.len());

        for dep in &dependencies {
            dependents.entry(*dep).or_default().push(key);
        }

        if dependencies.is_empty() {
            ready.push(key);
        }
    }

    // Process items in topological order, pushing back into the input Vec.
    while let Some(id) = ready.pop() {
        let item = items_by_key.remove(&id).expect("must exist");
        items.push(item);

        if let Some(depts) = dependents.get(&id) {
            for dept in depts {
                let deg = in_degree.get_mut(dept).expect("must exist");
                *deg -= 1;
                if *deg == 0 {
                    ready.push(*dept);
                }
            }
        }
    }

    // Cycle detection: if we didn't process all items, there's a cycle.
    if !items_by_key.is_empty() {
        panic!("dependency cycle: {items_by_key:?}");
    }
}

pub trait ResultExt<T> {
    /// Like [`Result::expect`], but terminates the process with `halt` or
    /// exit code 0 instead of `panic` if the error indicates that it should
    /// cause a halt of graceful termination.
    fn unwrap_or_terminate(self, context: &str) -> T;

    /// Terminates the process with `halt` or exit code 0 if `self` is an
    /// error that should halt or cause graceful termination. Otherwise,
    /// does nothing.
    fn maybe_terminate(self, context: &str) -> Self;
}

impl<T, E> ResultExt<T> for Result<T, E>
where
    E: ShouldTerminateGracefully + Debug,
{
    fn unwrap_or_terminate(self, context: &str) -> T {
        match self {
            Ok(t) => t,
            Err(e) if e.should_terminate_gracefully() => exit!(0, "{context}: {e:?}"),
            Err(e) => panic!("{context}: {e:?}"),
        }
    }

    fn maybe_terminate(self, context: &str) -> Self {
        if let Err(e) = &self {
            if e.should_terminate_gracefully() {
                exit!(0, "{context}: {e:?}");
            }
        }

        self
    }
}

/// A trait for errors that should terminate gracefully rather than panic
/// the process.
trait ShouldTerminateGracefully {
    /// Reports whether the error should terminate the process gracefully
    /// rather than panic.
    fn should_terminate_gracefully(&self) -> bool;
}

impl ShouldTerminateGracefully for super::CatalogError {
    fn should_terminate_gracefully(&self) -> bool {
        match self {
            super::CatalogError::Catalog(e) => e.should_terminate_gracefully(),
            _ => false,
        }
    }
}

impl ShouldTerminateGracefully for crate::memory::error::Error {
    fn should_terminate_gracefully(&self) -> bool {
        match &self.kind {
            crate::memory::error::ErrorKind::Durable(e) => e.should_terminate_gracefully(),
            _ => false,
        }
    }
}

impl ShouldTerminateGracefully for crate::durable::CatalogError {
    fn should_terminate_gracefully(&self) -> bool {
        match &self {
            Self::Durable(e) => e.should_terminate_gracefully(),
            Self::Catalog(_) => false,
        }
    }
}

impl ShouldTerminateGracefully for DurableCatalogError {
    fn should_terminate_gracefully(&self) -> bool {
        match self {
            DurableCatalogError::Fence(err) => err.should_terminate_gracefully(),
            DurableCatalogError::CatalogOutOfSync { .. }
            | DurableCatalogError::RestartRequired { .. } => true,
            DurableCatalogError::IncompatibleDataVersion { .. }
            | DurableCatalogError::IncompatiblePersistVersion { .. }
            | DurableCatalogError::Proto(_)
            | DurableCatalogError::Uninitialized
            | DurableCatalogError::NotWritable(_)
            | DurableCatalogError::DryRunTransaction
            | DurableCatalogError::InvalidReadProtection(_)
            | DurableCatalogError::DuplicateKey
            | DurableCatalogError::UniquenessViolation
            | DurableCatalogError::Storage(_)
            | DurableCatalogError::Internal(_) => false,
        }
    }
}

impl ShouldTerminateGracefully for FenceError {
    fn should_terminate_gracefully(&self) -> bool {
        match self {
            FenceError::DeployGeneration { .. } => true,
            FenceError::Epoch { .. } | FenceError::MigrationUpper { .. } => false,
        }
    }
}
