// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Contains the logic for upgrading the catalog from one version to another.
//! TODO(jkosh44) Make this generic over the catalog instead of specific to the stash
//! implementation.

use crate::durable::initialize::USER_VERSION_KEY;
use crate::durable::CONFIG_COLLECTION;
use futures::FutureExt;
use mz_stash::Stash;
use mz_stash_types::objects::proto;
use mz_stash_types::{InternalStashError, StashError, MIN_STASH_VERSION, STASH_VERSION};

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
                        inner: InternalStashError::IncompatibleVersion(version),
                    };

                    match version {
                        ..=TOO_OLD_VERSION => return Err(incompatible),

                        35 => mz_stash::upgrade::v35_to_v36::upgrade(&tx).await?,
                        36 => mz_stash::upgrade::v36_to_v37::upgrade(),
                        37 => mz_stash::upgrade::v37_to_v38::upgrade(&tx).await?,
                        38 => mz_stash::upgrade::v38_to_v39::upgrade(&tx).await?,
                        39 => mz_stash::upgrade::v39_to_v40::upgrade(&tx).await?,
                        40 => mz_stash::upgrade::v40_to_v41::upgrade(),
                        41 => mz_stash::upgrade::v41_to_v42::upgrade(),
                        42 => mz_stash::upgrade::v42_to_v43::upgrade(),

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
