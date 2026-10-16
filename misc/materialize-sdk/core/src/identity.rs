// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file at the
// root of this repository, or online at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Telling a replaced object from a different object behind the same name.

use serde::{Deserialize, Serialize};

use crate::decode::Envelope;
use crate::error::UsageError;
use crate::statement::ObjectRef;

/// One output column of an object, as `mz_columns` describes it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Column {
    /// The column name.
    pub name: String,
    /// The type's OID.
    pub type_oid: u32,
    /// The type modifier, such as a `numeric` precision, or `-1` for none.
    pub type_mod: i32,
    /// Whether the column can be `NULL`.
    pub nullable: bool,
}

/// What a checkpoint records about the object it was taken from, a table,
/// source, or materialized view, all of which have a storage shard.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Fingerprint {
    /// The object, with the catalog id its name resolved to.
    pub object: ObjectRef,
    /// The object's storage shard, from `mz_internal.mz_storage_shards`.
    ///
    /// Names and catalog ids alone cannot tell a replacement from a different
    /// object: `ALTER SCHEMA ... SWAP` moves names and keeps ids, while a
    /// replacement keeps the shard and changes the id
    /// (`src/adapter/src/catalog/transact.rs`).
    pub shard: String,
    /// The subscription's output envelope.
    pub envelope: Envelope,
    /// The object's output columns, in order.
    pub columns: Vec<Column>,
}

/// A subscription's columns in the order a change's cells arrive.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PayloadColumns {
    /// The key columns in `KEY` order, the cells of a key. Empty in the diff
    /// envelope.
    pub key: Vec<Column>,
    /// The other columns in relation order, the cells of an upsert's value.
    /// In the diff envelope, every column, the cells of a row.
    pub value: Vec<Column>,
}

impl Fingerprint {
    /// The columns of the cells the subscription produces (see
    /// [`crate::decode`] for the layout). Fails if a key column is missing or
    /// repeated.
    pub fn payload_columns(&self) -> Result<PayloadColumns, UsageError> {
        let Envelope::Upsert { key } = &self.envelope else {
            return Ok(PayloadColumns {
                key: Vec::new(),
                value: self.columns.clone(),
            });
        };
        let mut value = self.columns.clone();
        let mut key_columns = Vec::with_capacity(key.len());
        for name in key {
            let position = value
                .iter()
                .position(|column| &column.name == name)
                .ok_or_else(|| {
                    UsageError::new(format!(
                        "the key column {name:?} is not a column of {:?}, or is repeated",
                        self.object.name
                    ))
                })?;
            key_columns.push(value.remove(position));
        }
        Ok(PayloadColumns {
            key: key_columns,
            value,
        })
    }
}

/// How the object a name resolves to now differs from a stored fingerprint.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum IdentityChange {
    /// The same object.
    Unchanged,
    /// The same storage and columns under a new catalog id, as after
    /// `ALTER MATERIALIZED VIEW ... APPLY REPLACEMENT`. History is continuous,
    /// but retention comes from the replacement, so check the retention margin
    /// again before resuming, and store the new fingerprint after.
    Replaced,
    /// The same columns over different storage, as after
    /// `ALTER SCHEMA ... SWAP`. The target holds the old object's state, which
    /// only a new snapshot can reconcile.
    Swapped,
    /// The output columns or the envelope differ, so the target's rows no
    /// longer have the shape the subscription produces.
    SchemaChanged,
    /// The name resolves to nothing.
    Dropped,
}

/// Compares the stored fingerprint with the one the stored name resolves to
/// now, or `None` if it resolves to nothing.
///
/// Changed columns win over every other difference, because resuming would mix
/// rows of two shapes in one target.
pub fn compare(stored: &Fingerprint, current: Option<&Fingerprint>) -> IdentityChange {
    let Some(current) = current else {
        return IdentityChange::Dropped;
    };
    if stored.columns != current.columns || stored.envelope != current.envelope {
        IdentityChange::SchemaChanged
    } else if stored.shard != current.shard {
        IdentityChange::Swapped
    } else if stored.object.id != current.object.id {
        IdentityChange::Replaced
    } else {
        IdentityChange::Unchanged
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fingerprint() -> Fingerprint {
        Fingerprint {
            object: ObjectRef {
                id: "u7".into(),
                database: Some("db".into()),
                schema: "public".into(),
                name: "orders".into(),
            },
            shard: "s1".into(),
            envelope: Envelope::Upsert {
                key: vec!["id".into()],
            },
            columns: vec![
                Column {
                    name: "id".into(),
                    type_oid: 20,
                    type_mod: -1,
                    nullable: false,
                },
                Column {
                    name: "total".into(),
                    type_oid: 1700,
                    type_mod: 655366,
                    nullable: true,
                },
            ],
        }
    }

    #[test]
    fn classifies_each_kind_of_change() {
        let stored = fingerprint();
        assert_eq!(compare(&stored, None), IdentityChange::Dropped);
        assert_eq!(compare(&stored, Some(&stored)), IdentityChange::Unchanged);

        let mut replaced = stored.clone();
        replaced.object.id = "u8".into();
        assert_eq!(compare(&stored, Some(&replaced)), IdentityChange::Replaced);

        let mut swapped = stored.clone();
        swapped.shard = "s2".into();
        assert_eq!(compare(&stored, Some(&swapped)), IdentityChange::Swapped);
        swapped.object.id = "u9".into();
        assert_eq!(compare(&stored, Some(&swapped)), IdentityChange::Swapped);
    }

    #[test]
    fn payload_columns_put_the_key_first_in_key_order() {
        let mut stored = fingerprint();
        stored.columns.insert(
            1,
            Column {
                name: "region".into(),
                type_oid: 25,
                type_mod: -1,
                nullable: false,
            },
        );
        let names = |columns: &[Column]| columns.iter().map(|c| c.name.clone()).collect::<Vec<_>>();

        stored.envelope = Envelope::Upsert {
            key: vec!["total".into(), "id".into()],
        };
        let payload = stored.payload_columns().unwrap();
        assert_eq!(names(&payload.key), ["total", "id"]);
        assert_eq!(names(&payload.value), ["region"]);

        stored.envelope = Envelope::Diff;
        let payload = stored.payload_columns().unwrap();
        assert!(payload.key.is_empty());
        assert_eq!(names(&payload.value), ["id", "region", "total"]);

        for key in [vec!["missing".to_string()], vec!["id".into(), "id".into()]] {
            stored.envelope = Envelope::Upsert { key };
            assert!(stored.payload_columns().is_err(), "{:?}", stored.envelope);
        }
    }

    #[test]
    fn any_column_difference_is_a_schema_change() {
        let stored = fingerprint();
        let changes: [fn(&mut Fingerprint); 5] = [
            |f| f.columns[1].name = "amount".into(),
            |f| f.columns[1].type_mod = 655368,
            |f| f.columns[0].nullable = true,
            |f| {
                f.columns.pop();
            },
            |f| f.envelope = Envelope::Diff,
        ];
        for change in changes {
            let mut current = stored.clone();
            change(&mut current);
            current.shard = "s2".into();
            assert_eq!(
                compare(&stored, Some(&current)),
                IdentityChange::SchemaChanged,
                "{current:?}"
            );
        }
    }
}
