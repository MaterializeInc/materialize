// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! This module contains all of the helpers and code paths for upgrading/migrating the [`Stash`].
//!
//! We facilitate migrations by keeping snapshots of the objects we previously stored, and relying
//! entirely on these snapshots. These exist in the form of `catalog/protos/objects_vXX.proto`. By
//! maintaining and relying on snapshots we don't have to worry about changes elsewhere in the
//! codebase effecting our migrations because our application and serialization logic is decoupled,
//! and the objects of the Stash for a given version are "frozen in time".
//!
//! When you want to make a change to the `Stash` you need to follow these steps:
//!
//! 1. Check the current [`STASH_VERSION`], make sure an `objects_v<STASH_VERSION>.proto` file
//!    exists. If one doesn't, copy and paste the current `objects.proto` file, renaming it to
//!    `objects_v<STASH_VERSION>.proto`.
//! 2. Bump [`STASH_VERSION`] by one.
//! 3. Make your changes to `objects.proto`.
//! 4. Copy and paste `objects.proto`, naming the copy `objects_v<STASH_VERSION>.proto`.
//! 5. We should now have a copy of the protobuf objects as they currently exist, and a copy of
//!    how we want them to exist. For example, if the version of the Stash before we made our
//!    changes was 15, we should now have `objects_v15.proto` and `objects_v16.proto`.
//! 6. Add `v<STASH_VERSION>` to the call to the `objects!` macro in this file.
//! 7. Add a new file to `catalog/src/durable/upgrade`, which is where we'll put the new migration
//!    path.
//! 8. Write an upgrade function using the the two versions of the protos we now have, e.g.
//!    `objects_v15.proto` and `objects_v16.proto`. In this migration code you __should not__
//!    import any defaults or constants from elsewhere in the codebase, because then a future
//!    change could then impact a previous migration.
//! 9. Call your upgrade function in [`crate::durable::upgrade::upgrade`].
//!
//! When in doubt, reach out to the Surfaces team, and we'll be more than happy to help :)
//!
//! [`Stash`]: mz_stash::Stash

//! TODO(jkosh44) Make this generic over the catalog instead of specific to the stash
//! implementation.

use futures::FutureExt;
use mz_stash::Stash;
use mz_stash_types::{InternalStashError, StashError};
use paste::paste;

use crate::durable::initialize::USER_VERSION_KEY;
use crate::durable::objects::serialization::proto;
use crate::durable::{upgrade, CONFIG_COLLECTION};

mod v35_to_v36;
mod v36_to_v37;
mod v37_to_v38;
mod v38_to_v39;
mod v39_to_v40;
mod v40_to_v41;
mod v41_to_v42;
mod v42_to_v43;

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

objects!(v35, v36, v37, v38, v39, v40, v41, v42, v43);

/// The current version of the `Stash`.
///
/// We will initialize new `Stash`es with this version, and migrate existing `Stash`es to this
/// version. Whenever the `Stash` changes, e.g. the protobufs we serialize in the `Stash`
/// change, we need to bump this version.
pub const STASH_VERSION: u64 = 43;

/// The minimum `Stash` version number that we support migrating from.
///
/// After bumping this we can delete the old migrations.
pub const MIN_STASH_VERSION: u64 = 35;

#[tracing::instrument(name = "stash::upgrade", level = "debug", skip_all)]
pub async fn upgrade(stash: &mut Stash) -> Result<(), StashError> {
    // Run migrations until we're up-to-date.
    while run_upgrade(stash).await? < STASH_VERSION {}

    async fn run_upgrade(stash: &mut Stash) -> Result<u64, StashError> {
        stash
            .with_transaction(move |tx| {
                async move {
                    let version = version(&tx).await?;

                    // Note(parkmycar): Ideally we wouldn't have to define these extra constants,
                    // but const expressions aren't yet supported in match statements.
                    const TOO_OLD_VERSION: u64 = MIN_STASH_VERSION - 1;
                    const FUTURE_VERSION: u64 = STASH_VERSION + 1;
                    let incompatible = StashError {
                        inner: InternalStashError::IncompatibleVersion {
                            found_version: version,
                            min_stash_version: MIN_STASH_VERSION,
                            stash_version: STASH_VERSION,
                        },
                    };

                    match version {
                        ..=TOO_OLD_VERSION => return Err(incompatible),

                        35 => upgrade::v35_to_v36::upgrade(&tx).await?,
                        36 => upgrade::v36_to_v37::upgrade(),
                        37 => upgrade::v37_to_v38::upgrade(&tx).await?,
                        38 => upgrade::v38_to_v39::upgrade(&tx).await?,
                        39 => upgrade::v39_to_v40::upgrade(&tx).await?,
                        40 => upgrade::v40_to_v41::upgrade(),
                        41 => upgrade::v41_to_v42::upgrade(),
                        42 => upgrade::v42_to_v43::upgrade(),

                        // Up-to-date, no migration needed!
                        STASH_VERSION => return Ok(STASH_VERSION),
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
