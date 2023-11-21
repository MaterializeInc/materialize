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
//! 7. Add a new file to `catalog/src/durable/upgrade`, which is where we'll put the new migration
//!    path.
//! 8. Write upgrade functions using the the two versions of the protos we now have, e.g.
//!    `objects_v15.proto` and `objects_v16.proto`. In this migration code you __should not__
//!    import any defaults or constants from elsewhere in the codebase, because then a future
//!    change could then impact a previous migration. You will need to write one upgrade function
//!    for the Stash and one for Persist.
//! 9. Call your upgrade function in [`crate::durable::upgrade::stash::upgrade()`] and
//!    [`crate::durable::upgrade::persist::upgrade()`].
//!
//! When in doubt, reach out to the Surfaces team, and we'll be more than happy to help :)

//! TODO(jkosh44) Make this generic over the catalog instead of specific to the stash
//! implementation.

use paste::paste;

macro_rules! objects {
        ( $( $x:ident ),* ) => {
            paste! {
                $(
                    pub mod [<objects_ $x>] {
                        include!(concat!(env!("OUT_DIR"), "/objects_", stringify!($x), ".rs"));
                    }
                )*
            }
        }
    }

objects!(v39, v40, v41, v42, v43, v44);

/// The current version of the `Catalog`.
///
/// We will initialize new `Catalog`es with this version, and migrate existing `Catalog`es to this
/// version. Whenever the `Catalog` changes, e.g. the protobufs we serialize in the `Catalog`
/// change, we need to bump this version.
pub(crate) const CATALOG_VERSION: u64 = 44;

/// The minimum `Catalog` version number that we support migrating from.
///
/// After bumping this we can delete the old migrations.
pub(crate) const MIN_CATALOG_VERSION: u64 = 39;
// Note(parkmycar): Ideally we wouldn't have to define these extra constants,
// but const expressions aren't yet supported in match statements.
const TOO_OLD_VERSION: u64 = MIN_CATALOG_VERSION - 1;
const FUTURE_VERSION: u64 = CATALOG_VERSION + 1;

pub(crate) mod stash {
    use crate::durable::initialize::USER_VERSION_KEY;
    use crate::durable::objects::serialization::proto;
    use crate::durable::CONFIG_COLLECTION;
    use futures::FutureExt;
    use mz_stash::Stash;
    use mz_stash_types::{InternalStashError, StashError};

    use crate::durable::upgrade::{
        CATALOG_VERSION, FUTURE_VERSION, MIN_CATALOG_VERSION, TOO_OLD_VERSION,
    };

    mod v39_to_v40;
    mod v40_to_v41;
    mod v41_to_v42;
    mod v42_to_v43;
    mod v43_to_v44;

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

                            39 => v39_to_v40::upgrade(&tx).await?,
                            40 => v40_to_v41::upgrade(),
                            41 => v41_to_v42::upgrade(),
                            42 => v42_to_v43::upgrade(),
                            43 => v43_to_v44::upgrade(),

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
    use crate::durable::impls::persist::state_update::StateUpdateKindBinary;
    use crate::durable::impls::persist::{PersistHandle, Timestamp};
    use crate::durable::upgrade::{
        CATALOG_VERSION, FUTURE_VERSION, MIN_CATALOG_VERSION, TOO_OLD_VERSION,
    };
    use crate::durable::DurableCatalogError;
    use mz_ore::soft_assert;

    #[tracing::instrument(name = "persist::upgrade", level = "debug", skip_all)]
    pub(crate) async fn upgrade(
        migrator: &mut PersistHandle<StateUpdateKindBinary>,
        mut version: u64,
    ) -> Result<(), DurableCatalogError> {
        let (is_initialized, mut upper) = migrator.is_initialized_inner().await;
        soft_assert!(is_initialized, "cannot upgrade uninitialized catalog");

        // Run migrations until we're up-to-date.
        while version < CATALOG_VERSION {
            let (new_version, new_upper) = run_upgrade(migrator, upper).await?;
            version = new_version;
            upper = new_upper;
        }

        async fn run_upgrade(
            migrator: &mut PersistHandle<StateUpdateKindBinary>,
            upper: Timestamp,
        ) -> Result<(u64, Timestamp), DurableCatalogError> {
            let as_of = migrator.as_of(upper);
            let version = migrator
                .get_user_version(as_of)
                .await
                .expect("initialized catalog must have a version");

            let incompatible = DurableCatalogError::IncompatibleVersion {
                found_version: version,
                min_catalog_version: MIN_CATALOG_VERSION,
                catalog_version: CATALOG_VERSION,
            };

            match version {
                ..=TOO_OLD_VERSION => return Err(incompatible),

                // TODO(jkosh44) Implement migrations.
                39 => panic!("upgrades not implemented"),
                40 => panic!("upgrades not implemented"),
                41 => panic!("upgrades not implemented"),
                42 => panic!("upgrades not implemented"),
                43 => panic!("upgrades not implemented"),

                // Up-to-date, no migration needed!
                CATALOG_VERSION => return Ok((CATALOG_VERSION, upper)),
                FUTURE_VERSION.. => return Err(incompatible),
            };
        }

        Ok(())
    }
}
