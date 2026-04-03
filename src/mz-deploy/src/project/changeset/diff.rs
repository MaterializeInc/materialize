//! Snapshot diff — finds objects whose hashes changed between two deployments.
//!
//! Compares the `objects` maps of two [`DeploymentSnapshot`]s by content hash.
//! An object is considered **changed** if:
//!
//! - It exists in both snapshots but the hashes differ (modified)
//! - It exists only in the new snapshot (added)
//! - It exists only in the old snapshot (deleted)
//!
//! Because hashes are computed from the normalized typed AST (not raw file
//! contents), formatting-only changes — whitespace, comment edits, identifier
//! casing — do **not** produce different hashes and therefore do not appear
//! in the diff. See [`super::super::deployment_snapshot::compute_typed_hash`]
//! for hash computation details.

use super::super::deployment_snapshot::DeploymentSnapshot;
use super::super::object_id::ObjectId;
use crate::verbose;
use owo_colors::OwoColorize;
use std::collections::BTreeSet;

/// Find changed objects by comparing snapshot hashes.
pub(super) fn find_changed_objects(
    old_snapshot: &DeploymentSnapshot,
    new_snapshot: &DeploymentSnapshot,
) -> BTreeSet<ObjectId> {
    verbose!(
        "{} {}",
        "▶".cyan(),
        "Comparing deployment snapshots...".cyan().bold()
    );
    let mut changed = BTreeSet::new();

    // Objects with different hashes or newly added
    for (object_id, new_hash) in &new_snapshot.objects {
        match old_snapshot.objects.get(object_id) {
            Some(old_hash) if old_hash != new_hash => {
                verbose!(
                    "  ├─ {}: {} ({} {} → {})",
                    "Changed".green(),
                    object_id.to_string().cyan(),
                    "hash".dimmed(),
                    old_hash[..8].to_string().dimmed(),
                    new_hash[..8].to_string().dimmed()
                );
                changed.insert(object_id.clone());
            }
            None => {
                verbose!(
                    "  ├─ {}: {} ({} {})",
                    "New".green(),
                    object_id.to_string().cyan(),
                    "hash".dimmed(),
                    new_hash[..8].to_string().dimmed()
                );
                changed.insert(object_id.clone());
            }
            _ => {}
        }
    }

    // Deleted objects
    for object_id in old_snapshot.objects.keys() {
        if !new_snapshot.objects.contains_key(object_id) {
            verbose!("  ├─ {}: {}", "Deleted".red(), object_id.to_string().cyan());
            changed.insert(object_id.clone());
        }
    }

    verbose!(
        "  └─ Found {} changed object(s)",
        changed.len().to_string().bold()
    );
    changed
}
