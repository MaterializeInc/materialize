// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! This module contains all of the helpers and code paths for upgrading/migrating the `Catalog`.
//!
//! We facilitate migrations by keeping snapshots of the objects we previously stored, and relying
//! entirely on these snapshots. These exist in the form of `catalog/protos/objects_vXX.proto`. By
//! maintaining and relying on snapshots we don't have to worry about changes elsewhere in the
//! codebase effecting our migrations because our application and serialization logic is decoupled,
//! and the objects of the Catalog for a given version are "frozen in time".
//!
//! When you want to make a change to the `Catalog` you need to follow these steps:
//!
//! 1. Check the current [`CATALOG_VERSION`], make sure an `objects_v<CATALOG_VERSION>.proto` file
//!    exists. If one doesn't, copy and paste the current `objects.proto` file, renaming it to
//!    `objects_v<CATALOG_VERSION>.proto`.
//! 2. Bump [`CATALOG_VERSION`] by one.
//! 3. Make your changes to `objects.proto`.
//! 4. Copy and paste `objects.proto`, naming the copy `objects_v<CATALOG_VERSION>.proto`.
//! 5. We should now have a copy of the protobuf objects as they currently exist, and a copy of
//!    how we want them to exist. For example, if the version of the Catalog before we made our
//!    changes was 15, we should now have `objects_v15.proto` and `objects_v16.proto`.
//! 6. Add `v<CATALOG_VERSION>` to the call to the `objects!` macro in this file.
//! 7. Add a new file to `catalog/src/durable/upgrade/persist` and
//!    `catalog/src/durable/upgrade/stash`, which is where we'll put the new migration path.
//! 8. Write upgrade functions using the the two versions of the protos we now have, e.g.
//!    `objects_v15.proto` and `objects_v16.proto`. In this migration code you __should not__
//!    import any defaults or constants from elsewhere in the codebase, because then a future
//!    change could then impact a previous migration. You need to write separate upgrade functions
//!    for the stash catalog and the persist catalog.
//! 9. Call your upgrade function in [`stash::upgrade()`] and [`persist::upgrade()`].
//! 10. Run `cargo test --package mz-catalog --lib \
//!     durable::upgrade::tests::generate_missing_snapshots -- --ignored` to generate a test file
//!     for the new version.
//!
//! When in doubt, reach out to the Surfaces team, and we'll be more than happy to help :)

#[cfg(test)]
mod tests;

use paste::paste;
#[cfg(test)]
use proptest::prelude::*;
#[cfg(test)]
use proptest::strategy::ValueTree;
#[cfg(test)]
use proptest_derive::Arbitrary;

#[cfg(test)]
use crate::durable::impls::persist::state_update::StateUpdateKindRaw;

#[cfg(test)]
const ENCODED_TEST_CASES: usize = 100;

macro_rules! objects {
    ( $( $x:ident ),* ) => {
        paste! {
            $(
                pub mod [<objects_ $x>] {
                    include!(concat!(env!("OUT_DIR"), "/objects_", stringify!($x), ".rs"));

                    use crate::durable::impls::persist::state_update::StateUpdateKindRaw;

                    impl From<StateUpdateKind> for StateUpdateKindRaw {
                        fn from(value: StateUpdateKind) -> Self {
                            // TODO: This requires that the json->proto->json roundtrips
                            // exactly, see #23908.
                            StateUpdateKindRaw::from_serde(&value)
                        }
                    }

                    impl TryFrom<StateUpdateKindRaw> for StateUpdateKind {
                        type Error = String;

                        fn try_from(value: StateUpdateKindRaw) -> Result<Self, Self::Error> {
                            Ok(value.to_serde::<Self>())
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
                fn try_from_raw(version: &str, raw: StateUpdateKindRaw) -> Result<Self, String> {
                    match version {
                        $(
                            concat!("objects_", stringify!($x)) => Ok(Self::[<$x:upper>](raw.try_into()?)),
                        )*
                        _ => Err(format!("unrecognized version {version} add enum variant")),
                    }
                }

                #[cfg(test)]
                fn raw(self) -> StateUpdateKindRaw {
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

objects!(v42, v43, v44, v45);

/// The current version of the `Catalog`.
///
/// We will initialize new `Catalog`es with this version, and migrate existing `Catalog`es to this
/// version. Whenever the `Catalog` changes, e.g. the protobufs we serialize in the `Catalog`
/// change, we need to bump this version.
pub(crate) const CATALOG_VERSION: u64 = 45;

/// The minimum `Catalog` version number that we support migrating from.
///
/// After bumping this we can delete the old migrations.
pub(crate) const MIN_CATALOG_VERSION: u64 = 42;

// Note(parkmycar): Ideally we wouldn't have to define these extra constants,
// but const expressions aren't yet supported in match statements.
const TOO_OLD_VERSION: u64 = MIN_CATALOG_VERSION - 1;
const FUTURE_VERSION: u64 = CATALOG_VERSION + 1;

pub(crate) mod stash {
    use futures::FutureExt;
    use mz_stash::Stash;
    use mz_stash_types::{InternalStashError, StashError};

    use crate::durable::initialize::USER_VERSION_KEY;
    use crate::durable::objects::serialization::proto;
    use crate::durable::upgrade::{
        CATALOG_VERSION, FUTURE_VERSION, MIN_CATALOG_VERSION, TOO_OLD_VERSION,
    };
    use crate::durable::CONFIG_COLLECTION;

    mod v42_to_v43;
    mod v43_to_v44;
    mod v44_to_v45;

    #[tracing::instrument(name = "stash::upgrade", level = "debug", skip_all)]
    pub(crate) async fn upgrade(stash: &mut Stash) -> Result<(), StashError> {
        // Run migrations until we're up-to-date.
        while run_upgrade(stash).await? < CATALOG_VERSION {}

        async fn run_upgrade(stash: &mut Stash) -> Result<u64, StashError> {
            stash
                .with_transaction(move |tx| {
                    async move {
                        let version = version(&tx).await?;

                        let incompatible = StashError {
                            inner: InternalStashError::IncompatibleVersion {
                                found_version: version,
                                min_stash_version: MIN_CATALOG_VERSION,
                                stash_version: CATALOG_VERSION,
                            },
                        };

                        match version {
                            ..=TOO_OLD_VERSION => return Err(incompatible),

                            42 => v42_to_v43::upgrade(),
                            43 => v43_to_v44::upgrade(),
                            44 => v44_to_v45::upgrade(&tx).await?,

                            // Up-to-date, no migration needed!
                            CATALOG_VERSION => return Ok(CATALOG_VERSION),
                            FUTURE_VERSION.. => return Err(incompatible),
                        };
                        // Set the new version.
                        let new_version = version + 1;
                        set_version(&tx, new_version).await?;

                        Ok(new_version)
                    }
                    .boxed()
                })
                .await
        }

        Ok(())
    }

    async fn version(tx: &mz_stash::Transaction<'_>) -> Result<u64, StashError> {
        let key = proto::ConfigKey {
            key: USER_VERSION_KEY.to_string(),
        };
        let config = CONFIG_COLLECTION.from_tx(tx).await?;
        let version = tx
            .peek_key_one(config, &key)
            .await?
            .ok_or_else(|| StashError {
                inner: InternalStashError::Uninitialized,
            })?;

        Ok(version.value)
    }

    async fn set_version(tx: &mz_stash::Transaction<'_>, version: u64) -> Result<(), StashError> {
        let key = proto::ConfigKey {
            key: USER_VERSION_KEY.to_string(),
        };
        let value = proto::ConfigValue { value: version };

        // Either insert a new version, or bump the old version.
        CONFIG_COLLECTION
            .migrate_to(tx, |entries| {
                let action = if entries.contains_key(&key) {
                    mz_stash::upgrade::MigrationAction::Update(key.clone(), (key, value))
                } else {
                    mz_stash::upgrade::MigrationAction::Insert(key, value)
                };

                vec![action]
            })
            .await?;

        Ok(())
    }
}

pub(crate) mod persist {
    use mz_ore::{soft_assert_eq_or_log, soft_assert_ne_or_log};
    use timely::progress::Timestamp as TimelyTimestamp;

    use crate::durable::impls::persist::state_update::{
        IntoStateUpdateKindRaw, StateUpdateKindRaw,
    };
    use crate::durable::impls::persist::{StateUpdate, Timestamp, UnopenedPersistCatalogState};
    use crate::durable::initialize::USER_VERSION_KEY;
    use crate::durable::objects::serialization::proto;
    use crate::durable::upgrade::{
        CATALOG_VERSION, FUTURE_VERSION, MIN_CATALOG_VERSION, TOO_OLD_VERSION,
    };
    use crate::durable::{CatalogError, DurableCatalogError};

    mod v42_to_v43;
    mod v43_to_v44;
    mod v44_to_v45;

    /// Describes a single action to take during a migration from `V1` to `V2`.
    #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
    enum MigrationAction<V1: IntoStateUpdateKindRaw, V2: IntoStateUpdateKindRaw> {
        /// Deletes the provided key.
        Delete(V1),
        /// Inserts the provided key-value pair. The key must not currently exist!
        Insert(V2),
        /// Update the key-value pair for the provided key.
        #[allow(unused)]
        Update(V1, V2),
    }

    impl<V1: IntoStateUpdateKindRaw, V2: IntoStateUpdateKindRaw> MigrationAction<V1, V2> {
        /// Converts `self` into a `Vec<StateUpdate<StateUpdateKindBinary>>` that can be appended
        /// to persist.
        fn into_updates(self, upper: Timestamp) -> Vec<StateUpdate<StateUpdateKindRaw>> {
            match self {
                MigrationAction::Delete(kind) => {
                    vec![StateUpdate {
                        kind: kind.into(),
                        ts: upper,
                        diff: -1,
                    }]
                }
                MigrationAction::Insert(kind) => {
                    vec![StateUpdate {
                        kind: kind.into(),
                        ts: upper,
                        diff: 1,
                    }]
                }
                MigrationAction::Update(old_kind, new_kind) => {
                    vec![
                        StateUpdate {
                            kind: old_kind.into(),
                            ts: upper,
                            diff: -1,
                        },
                        StateUpdate {
                            kind: new_kind.into(),
                            ts: upper,
                            diff: 1,
                        },
                    ]
                }
            }
        }
    }

    /// Upgrades the data in the catalog to version [`CATALOG_VERSION`].
    ///
    /// Returns the current upper after all migrations have executed.
    #[tracing::instrument(name = "persist::upgrade", level = "debug", skip_all)]
    pub(crate) async fn upgrade(
        persist_handle: &mut UnopenedPersistCatalogState,
        mut upper: Timestamp,
    ) -> Result<Timestamp, CatalogError> {
        soft_assert_ne_or_log!(
            upper,
            Timestamp::minimum(),
            "cannot upgrade uninitialized catalog"
        );

        let as_of = persist_handle.as_of(upper);
        let mut version = persist_handle
            .get_user_version(as_of)
            .await?
            .expect("initialized catalog must have a version");
        // Run migrations until we're up-to-date.
        while version < CATALOG_VERSION {
            let (new_version, new_upper) = run_upgrade(persist_handle, upper, version).await?;
            version = new_version;
            upper = new_upper;
        }

        /// Determines which upgrade to run for the `version` and executes it.
        ///
        /// Returns the new version and upper.
        async fn run_upgrade(
            unopened_catalog_state: &mut UnopenedPersistCatalogState,
            upper: Timestamp,
            version: u64,
        ) -> Result<(u64, Timestamp), CatalogError> {
            let incompatible = DurableCatalogError::IncompatibleVersion {
                found_version: version,
                min_catalog_version: MIN_CATALOG_VERSION,
                catalog_version: CATALOG_VERSION,
            }
            .into();

            match version {
                ..=TOO_OLD_VERSION => Err(incompatible),

                42 => {
                    run_versioned_upgrade(
                        unopened_catalog_state,
                        upper,
                        version,
                        v42_to_v43::upgrade,
                    )
                    .await
                }
                43 => {
                    run_versioned_upgrade(
                        unopened_catalog_state,
                        upper,
                        version,
                        v43_to_v44::upgrade,
                    )
                    .await
                }
                44 => {
                    run_versioned_upgrade(
                        unopened_catalog_state,
                        upper,
                        version,
                        v44_to_v45::upgrade,
                    )
                    .await
                }

                // Up-to-date, no migration needed!
                CATALOG_VERSION => Ok((CATALOG_VERSION, upper)),
                FUTURE_VERSION.. => Err(incompatible),
            }
        }

        Ok(upper)
    }

    /// Runs `migration_logic` on the contents of the current catalog assuming a current version of
    /// `current_version`.
    ///
    /// Returns the new version and upper.
    async fn run_versioned_upgrade<V1: IntoStateUpdateKindRaw, V2: IntoStateUpdateKindRaw>(
        unopened_catalog_state: &mut UnopenedPersistCatalogState,
        upper: Timestamp,
        current_version: u64,
        migration_logic: impl FnOnce(Vec<V1>) -> Vec<MigrationAction<V1, V2>>,
    ) -> Result<(u64, Timestamp), CatalogError> {
        // 1. Get a snapshot of the current catalog, using the V1 to deserialize the
        // contents.
        let as_of = unopened_catalog_state.as_of(upper);
        let snapshot = unopened_catalog_state.snapshot_binary(as_of).await;
        let snapshot: Vec<_> = snapshot
            .into_iter()
            .map(|update| {
                soft_assert_eq_or_log!(1, update.diff, "snapshot is consolidated");
                V1::try_from(update.clone().kind).expect("invalid catalog data persisted")
            })
            .collect();

        // 2. Generate updates from version specific migration logic.
        let migration_actions = migration_logic(snapshot);
        let mut updates: Vec<_> = migration_actions
            .into_iter()
            .flat_map(|action| action.into_updates(upper).into_iter())
            .collect();

        // 3. Add a retraction for old version and insertion for new version into updates.
        let next_version = current_version + 1;
        let version_retraction = StateUpdate {
            kind: version_update_kind(current_version),
            ts: upper,
            diff: -1,
        };
        updates.push(version_retraction);
        let version_insertion = StateUpdate {
            kind: version_update_kind(next_version),
            ts: upper,
            diff: 1,
        };
        updates.push(version_insertion);

        // 4. Compare and append migration into persist shard.
        let next_upper = upper.step_forward();
        unopened_catalog_state
            .compare_and_append(updates, upper, next_upper)
            .await?;

        Ok((next_version, next_upper))
    }

    /// Generates a [`proto::StateUpdateKind`] to update the user version.
    fn version_update_kind(version: u64) -> StateUpdateKindRaw {
        // We can use the current protobuf versions because Configs can never be migrated and are
        // always wire compatible.
        proto::StateUpdateKind {
            kind: Some(proto::state_update_kind::Kind::Config(
                proto::state_update_kind::Config {
                    key: Some(proto::ConfigKey {
                        key: USER_VERSION_KEY.to_string(),
                    }),
                    value: Some(proto::ConfigValue { value: version }),
                },
            )),
        }
        .into()
    }
}
