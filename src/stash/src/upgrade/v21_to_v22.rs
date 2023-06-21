// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::upgrade::MigrationAction;
use crate::{StashError, Transaction, TypedCollection};

pub mod objects_v21 {
    include!(concat!(env!("OUT_DIR"), "/objects_v21.rs"));
}
pub mod objects_v22 {
    include!(concat!(env!("OUT_DIR"), "/objects_v22.rs"));
}

/// Project away remap_shard from DurableCollectionMetadata.
pub async fn upgrade(tx: &'_ mut Transaction<'_>) -> Result<(), StashError> {
    const METADATA_COLLECTION: TypedCollection<
        objects_v21::GlobalId,
        objects_v21::DurableCollectionMetadata,
    > = TypedCollection::new("storage-collection-metadata");

    METADATA_COLLECTION
        .migrate_to(tx, |entries| {
            let mut updates = vec![];

            for (global_id, durable_collection_metadata) in entries {
                let new_key = objects_v22::GlobalId::from(global_id.clone());
                let new_value = objects_v22::DurableCollectionMetadata {
                    data_shard: durable_collection_metadata.data_shard.clone(),
                };
                updates.push(MigrationAction::<
                    objects_v21::GlobalId,
                    objects_v22::GlobalId,
                    objects_v22::DurableCollectionMetadata,
                >::Update(
                    global_id.clone(), (new_key, new_value)
                ));
            }
            updates
        })
        .await?;

    Ok(())
}

impl From<objects_v21::GlobalId> for objects_v22::GlobalId {
    fn from(id: objects_v21::GlobalId) -> Self {
        let value = match id.value {
            None => None,
            Some(objects_v21::global_id::Value::User(id)) => {
                Some(objects_v22::global_id::Value::User(id))
            }
            Some(objects_v21::global_id::Value::System(id)) => {
                Some(objects_v22::global_id::Value::System(id))
            }
            Some(objects_v21::global_id::Value::Transient(id)) => {
                Some(objects_v22::global_id::Value::Transient(id))
            }
            Some(objects_v21::global_id::Value::Explain(_)) => {
                Some(objects_v22::global_id::Value::Explain(Default::default()))
            }
        };
        objects_v22::GlobalId { value }
    }
}

#[cfg(test)]
mod tests {
    use super::objects_v21::{
        self, DurableCollectionMetadata as DurableCollectionMetadataV20, GlobalId as GlobalIdV20,
    };
    use super::objects_v22::{
        self, DurableCollectionMetadata as DurableCollectionMetadataV21, GlobalId as GlobalIdV21,
    };
    use super::upgrade;

    use crate::{DebugStashFactory, TypedCollection};

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
    async fn smoke_test() {
        // Connect to the Stash.
        let factory = DebugStashFactory::new().await;
        let mut stash = factory.open_debug().await;

        // Insert some items.
        let metadata_collection_v21: TypedCollection<GlobalIdV20, DurableCollectionMetadataV20> =
            TypedCollection::new("storage-collection-metadata");
        metadata_collection_v21
            .insert_without_overwrite(
                &mut stash,
                vec![(
                    GlobalIdV20 {
                        value: Some(objects_v21::global_id::Value::User(8)),
                    },
                    DurableCollectionMetadataV20 {
                        remap_shard: None,
                        data_shard: "lidsacae".to_string(),
                    },
                )],
            )
            .await
            .unwrap();

        // Run our migration.
        stash
            .with_transaction(|mut tx| {
                Box::pin(async move {
                    upgrade(&mut tx).await?;
                    Ok(())
                })
            })
            .await
            .expect("migration failed");

        // Read back the values.
        let metadata_collection_v22: TypedCollection<GlobalIdV21, DurableCollectionMetadataV21> =
            TypedCollection::new("storage-collection-metadata");
        let metadata = metadata_collection_v22.iter(&mut stash).await.unwrap();

        // Project away the time and diff.
        let mut ids: Vec<_> = metadata
            .into_iter()
            .map(|((key, value), _, _)| (key, value))
            .collect();
        ids.sort();

        assert_eq!(
            ids,
            vec![(
                GlobalIdV21 {
                    value: Some(objects_v22::global_id::Value::User(8)),
                },
                DurableCollectionMetadataV21 {
                    data_shard: "lidsacae".to_string(),
                }
            )]
        );
    }
}
