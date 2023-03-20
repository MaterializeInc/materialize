//! Special cases related to the "introspection" of Materialize
//!
//! Every Materialize deployment has a pre-installed [`mz_introspection`] cluster, which
//! has several indexes to speed up common introspection queries. We also have a special
//! `mz_introspection` role, which can be used by support teams to diagnose a deployment.
//! For each of these use cases, we have some special restrictions we want to apply. The
//! logic around these restrictions is defined here.
//!
//!
//! [`mz_introspection`]: https://materialize.com/docs/sql/show-clusters/#mz_introspection-system-cluster

use mz_ore::collections::HashSet;
use mz_repr::GlobalId;
use mz_sql::catalog::SessionCatalog;
use once_cell::sync::Lazy;
use smallvec::SmallVec;

use crate::{
    catalog::builtin::{
        INFORMATION_SCHEMA, MZ_CATALOG_SCHEMA, MZ_INTERNAL_SCHEMA, MZ_INTROSPECTION_CLUSTER,
        PG_CATALOG_SCHEMA,
    },
    AdapterError,
};

/// The schema's a user is allowed to query from the `mz_introspection` cluster
static ALLOWED_SCHEMAS: Lazy<HashSet<&str>> = Lazy::new(|| {
    HashSet::from([
        MZ_CATALOG_SCHEMA,
        PG_CATALOG_SCHEMA,
        MZ_INTERNAL_SCHEMA,
        INFORMATION_SCHEMA,
    ])
});

/// Checks if we're currently running on the [`MZ_INTROSPECTION_CLUSTER`], and if so, do
/// we depend on any objects that we're not allowed to query from the cluster.
pub fn check_cluster_restrictions(
    catalog: &impl SessionCatalog,
    depends_on: &Vec<GlobalId>,
) -> Result<(), AdapterError> {
    // We only impose restrictions if the current cluster is the introspection cluster.
    let cluster = catalog.active_cluster();
    if cluster != MZ_INTROSPECTION_CLUSTER.name {
        return Ok(());
    }

    // Collect any items that are not allowed to be run on the introspection cluster.
    let unallowed_dependents: SmallVec<[String; 2]> = depends_on
        .iter()
        .filter_map(|id| {
            let item = catalog.get_item(id);
            let full_name = catalog.resolve_full_name(item.name());

            if !ALLOWED_SCHEMAS.contains(full_name.schema.as_str()) {
                Some(full_name.to_string())
            } else {
                None
            }
        })
        .collect();

    // If the query depends on unallowed items, error out.
    if !unallowed_dependents.is_empty() {
        Err(AdapterError::UnallowedOnCluster {
            depends_on: unallowed_dependents,
            cluster: MZ_INTROSPECTION_CLUSTER.name.to_string(),
        })
    } else {
        Ok(())
    }
}
