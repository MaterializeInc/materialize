// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use tracing::warn;

use crate::durable::upgrade::objects_v56::PersistTxnShardValue;
use crate::durable::upgrade::MigrationAction;
use crate::durable::upgrade::{objects_v56 as v56, objects_v57 as v57};

/// In v57 we changed the name of PersistTxnShard to TxnWalShard.
pub fn upgrade(
    snapshot: Vec<v56::StateUpdateKind>,
) -> Vec<MigrationAction<v56::StateUpdateKind, v57::StateUpdateKind>> {
    let txn_shard: Vec<_> = snapshot
        .into_iter()
        .filter_map(|update| {
            let v56::state_update_kind::Kind::PersistTxnShard(
                v56::state_update_kind::PersistTxnShard { value },
            ) = update.kind.expect("missing field")
            else {
                return None;
            };
            Some(value)
        })
        .collect();
    // Even 0 is highly suspect, but maybe the catalog isn't initialized yet.
    assert!(
        txn_shard.len() <= 1,
        "There should never be more than one txn shard, but we found {txn_shard:?}"
    );
    match txn_shard.into_iter().next() {
        Some(value) => {
            vec![
                MigrationAction::Delete(v56::StateUpdateKind {
                    kind: Some(v56::state_update_kind::Kind::PersistTxnShard(
                        v56::state_update_kind::PersistTxnShard {
                            value: value.clone(),
                        },
                    )),
                }),
                MigrationAction::Insert(v57::StateUpdateKind {
                    kind: Some(v57::state_update_kind::Kind::TxnWalShard(
                        v57::state_update_kind::TxnWalShard {
                            value: value.map(Into::into),
                        },
                    )),
                }),
            ]
        }
        None => {
            warn!("no txn shard found");
            Vec::new()
        }
    }
}

impl From<v56::PersistTxnShardValue> for v57::TxnWalShardValue {
    fn from(value: PersistTxnShardValue) -> Self {
        Self { shard: value.shard }
    }
}
