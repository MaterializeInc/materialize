// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! This module contains all the helpers and code paths for upgrading/migrating the `Catalog`.
//!
//! We facilitate migrations by keeping snapshots of the objects we previously stored, and relying
//! entirely on these snapshots. These snapshots exist in the [`mz_catalog_protos`] crate in the
//! form of `catalog-protos/protos/objects_vXX.proto`. By maintaining and relying on snapshots we
//! don't have to worry about changes elsewhere in the codebase effecting our migrations because
//! our application and serialization logic is decoupled, and the objects of the Catalog for a
//! given version are "frozen in time".
//!
//! > **Note**: The protobuf snapshot files themselves live in a separate crate because it takes a
//!             relatively significant amount of time to codegen and build them. By placing them in
//!             a separate crate we don't have to pay this compile time cost when building the
//!             catalog, allowing for faster iteration.
//!
//! You cannot make any changes to the following message or anything that they depend on:
//!
//!   - Config
//!   - Setting
//!   - FenceToken
//!   - AuditLog
//!
//! When you want to make a change to the `Catalog` you need to follow these steps:
//!
//! 1. Check the current [`CATALOG_VERSION`], make sure an `objects_v<CATALOG_VERSION>.proto` file
//!    exists. If one doesn't, copy and paste the current `objects.proto` file, renaming it to
//!    `objects_v<CATALOG_VERSION>.proto`.
//! 2. Bump [`CATALOG_VERSION`] by one.
//! 3. Make your changes to `objects.proto`.
//! 4. Copy and paste `objects.proto`, naming the copy `objects_v<CATALOG_VERSION>.proto`. Update
//!    the package name of the `.proto` to `package objects_v<CATALOG_VERSION>;`
//! 5. We should now have a copy of the protobuf objects as they currently exist, and a copy of
//!    how we want them to exist. For example, if the version of the Catalog before we made our
//!    changes was 15, we should now have `objects_v15.proto` and `objects_v16.proto`.
//! 6. Rebuild Materialize which will error because the hashes stored in
//!    `src/catalog-protos/protos/hashes.json` have now changed. Update these to match the new
//!    hashes for objects.proto and `objects_v<CATALOG_VERSION>.proto`.
//! 7. Add `v<CATALOG_VERSION>` to the call to the `objects!` macro in this file, and to the
//!    `proto_objects!` macro in the [`mz_catalog_protos`] crate.
//! 8. Add a new file to `catalog/src/durable/upgrade` which is where we'll put the new migration
//!    path.
//! 9. Write upgrade functions using the two versions of the protos we now have, e.g.
//!    `objects_v15.proto` and `objects_v16.proto`. In this migration code you __should not__
//!    import any defaults or constants from elsewhere in the codebase, because then a future
//!    change could then impact a previous migration.
//! 10. Add an import for your new module to this file: mod v<CATALOG_VERSION-1>_to_v<CATALOG_VERSION>;
//! 11. Call your upgrade function in [`run_upgrade()`].
//! 12. Generate a test file for the new version:
//!     ```ignore
//!     cargo test --package mz-catalog --lib durable::upgrade::tests::generate_missing_encodings -- --ignored
//!     ```
//!
//! When in doubt, reach out to the Surfaces team, and we'll be more than happy to help :)

pub mod json_compatible;
#[cfg(test)]
mod tests;

use mz_ore::{soft_assert_eq_or_log, soft_assert_ne_or_log};
use mz_repr::Diff;
use paste::paste;
#[cfg(test)]
use proptest::prelude::*;
#[cfg(test)]
use proptest::strategy::ValueTree;
#[cfg(test)]
use proptest_derive::Arbitrary;
use timely::progress::Timestamp as TimelyTimestamp;

use crate::durable::initialize::USER_VERSION_KEY;
use crate::durable::objects::serialization::proto;
use crate::durable::objects::state_update::{
    IntoStateUpdateKindJson, StateUpdate, StateUpdateKind, StateUpdateKindJson,
};
use crate::durable::persist::{Mode, Timestamp, UnopenedPersistCatalogState};
use crate::durable::{CatalogError, DurableCatalogError};

#[cfg(test)]
const ENCODED_TEST_CASES: usize = 100;

macro_rules! objects {
    ( $( $x:ident ),* ) => {
        paste! {
            $(
                pub(crate) mod [<objects_ $x>] {
                    pub use mz_catalog_protos::[<objects_ $x>]::*;

                    use crate::durable::objects::state_update::StateUpdateKindJson;

                    impl From<StateUpdateKind> for StateUpdateKindJson {
                        fn from(value: StateUpdateKind) -> Self {
                            let kind = value.kind.expect("kind should be set");
                            // TODO: This requires that the json->proto->json roundtrips
                            // exactly, see database-issues#7179.
                            StateUpdateKindJson::from_serde(&kind)
                        }
                    }

                    impl TryFrom<StateUpdateKindJson> for StateUpdateKind {
                        type Error = String;

                        fn try_from(value: StateUpdateKindJson) -> Result<Self, Self::Error> {
                            let kind: state_update_kind::Kind = value.to_serde();
                            Ok(StateUpdateKind { kind: Some(kind) })
                        }
                    }
                }
            )*

            // Generate test helpers for each version.

            #[cfg(test)]
            #[derive(Debug, Arbitrary)]
            enum AllVersionsStateUpdateKind {
                $(
                    [<$x:upper>](crate::durable::upgrade::[<objects_ $x>]::StateUpdateKind),
                )*
            }

            #[cfg(test)]
            impl AllVersionsStateUpdateKind {
                #[cfg(test)]
                fn arbitrary_vec(version: &str) -> Result<Vec<Self>, String> {
                    let mut runner = proptest::test_runner::TestRunner::deterministic();
                    std::iter::repeat(())
                        .filter_map(|_| AllVersionsStateUpdateKind::arbitrary(version, &mut runner).transpose())
                        .take(ENCODED_TEST_CASES)
                        .collect::<Result<_, _>>()
                }

                #[cfg(test)]
                fn arbitrary(
                    version: &str,
                    runner: &mut proptest::test_runner::TestRunner,
                ) -> Result<Option<Self>, String> {
                    match version {
                        $(
                            concat!("objects_", stringify!($x)) => {
                                let arbitrary_data =
                                    crate::durable::upgrade::[<objects_ $x>]::StateUpdateKind::arbitrary()
                                        .new_tree(runner)
                                        .expect("unable to create arbitrary data")
                                        .current();
                                // Skip over generated data where kind is None because they are not interesting or
                                // possible in production. Unfortunately any of the inner fields can still be None,
                                // which is also not possible in production.
                                // TODO(jkosh44) See if there's an arbitrary config that forces Some.
                                let arbitrary_data = if arbitrary_data.kind.is_some() {
                                    Some(Self::[<$x:upper>](arbitrary_data))
                                } else {
                                    None
                                };
                                Ok(arbitrary_data)
                            }
                        )*
                        _ => Err(format!("unrecognized version {version} add enum variant")),
                    }
                }

                #[cfg(test)]
                fn try_from_raw(version: &str, raw: StateUpdateKindJson) -> Result<Self, String> {
                    match version {
                        $(
                            concat!("objects_", stringify!($x)) => Ok(Self::[<$x:upper>](raw.try_into()?)),
                        )*
                        _ => Err(format!("unrecognized version {version} add enum variant")),
                    }
                }

                #[cfg(test)]
                fn raw(self) -> StateUpdateKindJson {
                    match self {
                        $(
                            Self::[<$x:upper>](kind) => kind.into(),
                        )*
                    }
                }
            }
        }
    }
}

objects!(v67, v68, v69, v70, v71, v72, v73, v74, v75, v76, v77, v78);

/// The current version of the `Catalog`.
pub use mz_catalog_protos::CATALOG_VERSION;
/// The minimum `Catalog` version number that we support migrating from.
pub use mz_catalog_protos::MIN_CATALOG_VERSION;

// Note(parkmycar): Ideally we wouldn't have to define these extra constants,
// but const expressions aren't yet supported in match statements.
const TOO_OLD_VERSION: u64 = MIN_CATALOG_VERSION - 1;
const FUTURE_VERSION: u64 = CATALOG_VERSION + 1;

mod v67_to_v68;
mod v68_to_v69;
mod v69_to_v70;
mod v70_to_v71;
mod v71_to_v72;
mod v72_to_v73;
mod v73_to_v74;
mod v74_to_v75;
mod v75_to_v76;
mod v76_to_v77;
mod v77_to_v78;

/// Describes a single action to take during a migration from `V1` to `V2`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum MigrationAction<V1: IntoStateUpdateKindJson, V2: IntoStateUpdateKindJson> {
    /// Deletes the provided key.
    #[allow(unused)]
    Delete(V1),
    /// Inserts the provided key-value pair. The key must not currently exist!
    #[allow(unused)]
    Insert(V2),
    /// Update the key-value pair for the provided key.
    #[allow(unused)]
    Update(V1, V2),
}

impl<V1: IntoStateUpdateKindJson, V2: IntoStateUpdateKindJson> MigrationAction<V1, V2> {
    /// Converts `self` into a `Vec<StateUpdate<StateUpdateKindBinary>>` that can be appended
    /// to persist.
    fn into_updates(self) -> Vec<(StateUpdateKindJson, Diff)> {
        match self {
            MigrationAction::Delete(kind) => {
                vec![(kind.into(), Diff::MINUS_ONE)]
            }
            MigrationAction::Insert(kind) => {
                vec![(kind.into(), Diff::ONE)]
            }
            MigrationAction::Update(old_kind, new_kind) => {
                vec![
                    (old_kind.into(), Diff::MINUS_ONE),
                    (new_kind.into(), Diff::ONE),
                ]
            }
        }
    }
}

/// Upgrades the data in the catalog to version [`CATALOG_VERSION`].
///
/// Returns the current upper after all migrations have executed.
#[mz_ore::instrument(name = "persist::upgrade", level = "debug")]
pub(crate) async fn upgrade(
    persist_handle: &mut UnopenedPersistCatalogState,
    mut commit_ts: Timestamp,
) -> Result<Timestamp, CatalogError> {
    soft_assert_ne_or_log!(
        persist_handle.upper,
        Timestamp::minimum(),
        "cannot upgrade uninitialized catalog"
    );

    // Consolidate to avoid migrating old state.
    persist_handle.consolidate();
    let mut version = persist_handle
        .get_user_version()
        .await?
        .expect("initialized catalog must have a version");
    // Run migrations until we're up-to-date.
    while version < CATALOG_VERSION {
        (version, commit_ts) = run_upgrade(persist_handle, version, commit_ts).await?;
    }

    Ok(commit_ts)
}

/// Determines which upgrade to run for the `version` and executes it.
///
/// Returns the new version and upper.
async fn run_upgrade(
    unopened_catalog_state: &mut UnopenedPersistCatalogState,
    version: u64,
    commit_ts: Timestamp,
) -> Result<(u64, Timestamp), CatalogError> {
    let incompatible = DurableCatalogError::IncompatibleDataVersion {
        found_version: version,
        min_catalog_version: MIN_CATALOG_VERSION,
        catalog_version: CATALOG_VERSION,
    }
    .into();

    match version {
        ..=TOO_OLD_VERSION => Err(incompatible),

        67 => {
            run_versioned_upgrade(
                unopened_catalog_state,
                version,
                commit_ts,
                v67_to_v68::upgrade,
            )
            .await
        }
        68 => {
            run_versioned_upgrade(
                unopened_catalog_state,
                version,
                commit_ts,
                v68_to_v69::upgrade,
            )
            .await
        }
        69 => {
            run_versioned_upgrade(
                unopened_catalog_state,
                version,
                commit_ts,
                v69_to_v70::upgrade,
            )
            .await
        }
        70 => {
            run_versioned_upgrade(
                unopened_catalog_state,
                version,
                commit_ts,
                v70_to_v71::upgrade,
            )
            .await
        }
        71 => {
            run_versioned_upgrade(
                unopened_catalog_state,
                version,
                commit_ts,
                v71_to_v72::upgrade,
            )
            .await
        }
        72 => {
            run_versioned_upgrade(
                unopened_catalog_state,
                version,
                commit_ts,
                v72_to_v73::upgrade,
            )
            .await
        }
        73 => {
            run_versioned_upgrade(
                unopened_catalog_state,
                version,
                commit_ts,
                v73_to_v74::upgrade,
            )
            .await
        }
        74 => {
            run_versioned_upgrade(
                unopened_catalog_state,
                version,
                commit_ts,
                v74_to_v75::upgrade,
            )
            .await
        }
        75 => {
            run_versioned_upgrade(
                unopened_catalog_state,
                version,
                commit_ts,
                v75_to_v76::upgrade,
            )
            .await
        }
        76 => {
            run_versioned_upgrade(
                unopened_catalog_state,
                version,
                commit_ts,
                v76_to_v77::upgrade,
            )
            .await
        }
        77 => {
            run_versioned_upgrade(
                unopened_catalog_state,
                version,
                commit_ts,
                v77_to_v78::upgrade,
            )
            .await
        }

        // Up-to-date, no migration needed!
        CATALOG_VERSION => Ok((CATALOG_VERSION, commit_ts)),
        FUTURE_VERSION.. => Err(incompatible),
    }
}

/// Runs `migration_logic` on the contents of the current catalog assuming a current version of
/// `current_version`.
///
/// Returns the new version and upper.
async fn run_versioned_upgrade<V1: IntoStateUpdateKindJson, V2: IntoStateUpdateKindJson>(
    unopened_catalog_state: &mut UnopenedPersistCatalogState,
    current_version: u64,
    mut commit_ts: Timestamp,
    migration_logic: impl FnOnce(Vec<V1>) -> Vec<MigrationAction<V1, V2>>,
) -> Result<(u64, Timestamp), CatalogError> {
    tracing::info!(current_version, "running versioned Catalog upgrade");

    // 1. Use the V1 to deserialize the contents of the current snapshot.
    let snapshot: Vec<_> = unopened_catalog_state
        .snapshot
        .iter()
        .map(|(kind, ts, diff)| {
            soft_assert_eq_or_log!(
                *diff,
                Diff::ONE,
                "snapshot is consolidated, ({kind:?}, {ts:?}, {diff:?})"
            );
            V1::try_from(kind.clone()).expect("invalid catalog data persisted")
        })
        .collect();

    // 2. Generate updates from version specific migration logic.
    let migration_actions = migration_logic(snapshot);
    let mut updates: Vec<_> = migration_actions
        .into_iter()
        .flat_map(|action| action.into_updates().into_iter())
        .collect();
    // Validate that we're not migrating an un-migratable collection.
    for (update, _) in &updates {
        if update.is_always_deserializable() {
            panic!("migration to un-migratable collection: {update:?}\nall updates: {updates:?}");
        }
    }

    // 3. Add a retraction for old version and insertion for new version into updates.
    let next_version = current_version + 1;
    let version_retraction = (version_update_kind(current_version), Diff::MINUS_ONE);
    updates.push(version_retraction);
    let version_insertion = (version_update_kind(next_version), Diff::ONE);
    updates.push(version_insertion);

    // 4. Apply migration to catalog.
    if matches!(unopened_catalog_state.mode, Mode::Writable) {
        commit_ts = unopened_catalog_state
            .compare_and_append(updates, commit_ts)
            .await
            .map_err(|e| e.unwrap_fence_error())?;
    } else {
        let ts = commit_ts;
        let updates = updates
            .into_iter()
            .map(|(kind, diff)| StateUpdate { kind, ts, diff });
        commit_ts = commit_ts.step_forward();
        unopened_catalog_state.apply_updates(updates)?;
    }

    // 5. Consolidate snapshot to remove old versions.
    unopened_catalog_state.consolidate();

    Ok((next_version, commit_ts))
}

/// Generates a [`proto::StateUpdateKind`] to update the user version.
fn version_update_kind(version: u64) -> StateUpdateKindJson {
    // We can use the current version because Configs can never be migrated and are always wire
    // compatible.
    StateUpdateKind::Config(
        proto::ConfigKey {
            key: USER_VERSION_KEY.to_string(),
        },
        proto::ConfigValue { value: version },
    )
    .into()
}
