// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! This module contains all of the helpers for upgrading/migrating the [`crate::Stash`].

use std::collections::BTreeMap;

use bytes::Bytes;
use mz_stash_types::{InternalStashError, StashError};

use crate::{AppendBatch, Data, Transaction, TypedCollection};

pub enum MigrationAction<K1, K2, V2> {
    /// Deletes the provided key.
    #[allow(unused)]
    Delete(K1),
    /// Inserts the provided key-value pair. The key must not currently exist!
    Insert(K2, V2),
    /// Update the key-value pair for the provided key.
    Update(K1, (K2, V2)),
}

impl<K, V> TypedCollection<K, V>
where
    K: Data,
    V: Data,
{
    /// Provided a closure, will migrate a [`TypedCollection`] of types `K` and `V` to types `K2`
    /// and `V2`.
    pub async fn migrate_to<K2, V2>(
        &self,
        tx: &Transaction<'_>,
        f: impl for<'a> FnOnce(&'a BTreeMap<K, V>) -> Vec<MigrationAction<K, K2, V2>>,
    ) -> Result<(), StashError>
    where
        K2: Data,
        V2: Data,
    {
        // Create a batch that we'll write to.
        let collection = tx.collection::<K, V>(self.name).await?;
        let lower = tx.upper(collection.id).await?;
        let mut batch = collection.make_batch_lower(lower)?;
        let current = match tx.peek_one(collection).await {
            Ok(set) => set,
            Err(err) => match err.inner {
                InternalStashError::PeekSinceUpper(_) => {
                    // If the upper isn't > since, bump the upper and try again to find a sealed
                    // entry. Do this by appending the empty batch which will advance the upper.
                    drop(tx.append(vec![batch]).await?);
                    let lower = tx.upper(collection.id).await?;
                    batch = collection.make_batch_lower(lower)?;
                    tx.peek_one(collection).await?
                }
                _ => return Err(err),
            },
        };

        // Note: this method exists, instead of using `StashCollection::append_to_batch` so we can
        // append types other than K or V.
        fn append_to_batch<A, B>(batch: &mut AppendBatch, k: &A, v: &B, diff: i64)
        where
            A: ::prost::Message,
            B: ::prost::Message,
        {
            let key = k.encode_to_vec();
            let val = v.encode_to_vec();

            batch.entries.push(((key, val), batch.timestamp, diff));
        }

        // Call the provided closure, generating a list of update actions.
        for op in f(&current) {
            match op {
                MigrationAction::Delete(old_key) => {
                    let old_value = current.get(&old_key).expect("key to exist");
                    append_to_batch(&mut batch, &old_key, old_value, -1);
                }
                MigrationAction::Insert(key, value) => {
                    append_to_batch(&mut batch, &key, &value, 1);
                }
                MigrationAction::Update(old_key, (new_key, new_value)) => {
                    let old_value = current.get(&old_key).expect("key to exist");
                    append_to_batch(&mut batch, &old_key, old_value, -1);
                    append_to_batch(&mut batch, &new_key, &new_value, 1);
                }
            }
        }

        drop(tx.append(vec![batch]).await?);

        Ok(())
    }

    /// Provided a closure, will migrate a [`TypedCollection`] of types `K` and `V` to
    /// [`WireCompatible`] types `K2` and `V2`.
    #[allow(unused)]
    pub(crate) async fn migrate_compat<K2, V2>(
        &self,
        tx: &Transaction<'_>,
        f: impl for<'a> FnOnce(&'a BTreeMap<K2, V2>) -> Vec<MigrationAction<K2, K2, V2>>,
    ) -> Result<(), StashError>
    where
        K2: Data + WireCompatible<K>,
        V2: Data + WireCompatible<V>,
    {
        // Create a batch that we'll write to.
        //
        // Note: this opens the collection with the NEW types that we're migrating to. This is okay
        // though because the new types are defined as being wire compatible with the old types.
        let collection = tx.collection::<K2, V2>(self.name).await?;
        let lower = tx.upper(collection.id).await?;
        let mut batch = collection.make_batch_lower(lower)?;
        let current = match tx.peek_one(collection).await {
            Ok(set) => set,
            Err(err) => match err.inner {
                InternalStashError::PeekSinceUpper(_) => {
                    // If the upper isn't > since, bump the upper and try again to find a sealed
                    // entry. Do this by appending the empty batch which will advance the upper.
                    tx.append(vec![batch]).await?;
                    let lower = tx.upper(collection.id).await?;
                    batch = collection.make_batch_lower(lower)?;
                    tx.peek_one(collection).await?
                }
                _ => return Err(err),
            },
        };

        // Call the provided closure, generating a list of update actions.
        for op in f(&current) {
            match op {
                MigrationAction::Delete(old_key) => {
                    let old_value = current.get(&old_key).expect("key to exist");
                    collection.append_to_batch(&mut batch, &old_key, old_value, -1);
                }
                MigrationAction::Insert(key, value) => {
                    collection.append_to_batch(&mut batch, &key, &value, 1);
                }
                MigrationAction::Update(old_key, (new_key, new_value)) => {
                    let old_value = current.get(&old_key).expect("key to exist");
                    collection.append_to_batch(&mut batch, &old_key, old_value, -1);
                    collection.append_to_batch(&mut batch, &new_key, &new_value, 1);
                }
            }
        }

        tx.append(vec![batch]).await?;

        Ok(())
    }

    /// Initializes a [`TypedCollection`] with the values provided in `values`.
    ///
    /// # Panics
    /// * If the [`TypedCollection`] is not empty.
    pub async fn initialize(
        &self,
        tx: &Transaction<'_>,
        values: impl IntoIterator<Item = (K, V)>,
    ) -> Result<(), StashError> {
        self.migrate_to(tx, |entries| {
            assert!(entries.is_empty());

            values
                .into_iter()
                .map(|(key, val)| MigrationAction::Insert(key, val))
                .collect()
        })
        .await?;

        Ok(())
    }
}

/// Denotes that `Self` is wire compatible with type `T`.
///
/// You should not implement this yourself, instead use the `wire_compatible!` macro.
pub unsafe trait WireCompatible<T: prost::Message>: prost::Message + Default {
    /// Converts the type `T` into `Self` by serializing `T` and deserializing as `Self`.
    fn convert(old: &T) -> Self {
        let bytes = old.encode_to_vec();
        // Note: use Bytes to enable possible re-use of the underlying buffer.
        let bytes = Bytes::from(bytes);
        Self::decode(bytes).expect("wire compatible")
    }
}

// SAFETY: A message type is trivially wire compatible with itself.
unsafe impl<T: prost::Message + Default + Clone> WireCompatible<T> for T {
    fn convert(old: &Self) -> Self {
        old.clone()
    }
}

/// Defines one protobuf type as wire compatible with another.
///
/// ```text
/// wire_compatible!(objects_v28::DatabaseKey with objects_v27::DatabaseKey);
/// ```
///
/// Internally this will implement the `WireCompatible<B> for <A>`, e.g.
/// `WireCompatible<objects_v27::DatabaseKey> for objects_v28::DatabaseKey` and generate `proptest`
/// cases that will create arbitrary objects of type `B` and assert they can be deserialized with
/// type `A`, and vice versa.
#[macro_export]
macro_rules! wire_compatible {
    ($a:ident $(:: $a_sub:ident)* with $b:ident $(:: $b_sub:ident)*) => {
        ::static_assertions::assert_impl_all!(
            $a $(::$a_sub)* : ::proptest::arbitrary::Arbitrary, ::prost::Message, Default,
        );
        ::static_assertions::assert_impl_all!(
            $b $(::$b_sub)*  : ::proptest::arbitrary::Arbitrary, ::prost::Message, Default,
        );

        // SAFETY: Below we assert that these types are wire compatible by generating arbitrary
        // structs, encoding in one, and then decoding in the other.
        unsafe impl $crate::upgrade::WireCompatible< $b $(::$b_sub)* > for $a $(::$a_sub)* {}
        unsafe impl $crate::upgrade::WireCompatible< $a $(::$a_sub)* > for $b $(::$b_sub)* {}

        ::paste::paste! {
            ::proptest::proptest! {
                #[mz_ore::test]
                #[cfg_attr(miri, ignore)] // slow
                fn [<proptest_wire_compat_ $a:snake $(_$a_sub:snake)* _to_ $b:snake $(_$b_sub:snake)* >](a: $a $(::$a_sub)* ) {
                    use ::prost::Message;
                    let a_bytes = a.encode_to_vec();
                    let b_decoded = $b $(::$b_sub)*::decode(&a_bytes[..]);
                    ::proptest::prelude::prop_assert!(b_decoded.is_ok());

                    // Maybe superfluous, but this is a method called in production.
                    let b_decoded = b_decoded.expect("asserted Ok");
                    let b_converted: $b $(::$b_sub)* = $crate::upgrade::WireCompatible::convert(&a);
                    assert_eq!(b_decoded, b_converted);

                    let b_bytes = b_decoded.encode_to_vec();
                    ::proptest::prelude::prop_assert_eq!(a_bytes, b_bytes, "a and b serialize differently");
                }

                #[mz_ore::test]
                #[cfg_attr(miri, ignore)] // slow
                fn [<proptest_wire_compat_ $b:snake $(_$b_sub:snake)* _to_ $a:snake $(_$a_sub:snake)* >](b: $b $(::$b_sub)* ) {
                    use ::prost::Message;
                    let b_bytes = b.encode_to_vec();
                    let a_decoded = $a $(::$a_sub)*::decode(&b_bytes[..]);
                    ::proptest::prelude::prop_assert!(a_decoded.is_ok());

                    // Maybe superfluous, but this is a method called in production.
                    let a_decoded = a_decoded.expect("asserted Ok");
                    let a_converted: $a $(::$a_sub)* = $crate::upgrade::WireCompatible::convert(&b);
                    assert_eq!(a_decoded, a_converted);

                    let a_bytes = a_decoded.encode_to_vec();
                    ::proptest::prelude::prop_assert_eq!(a_bytes, b_bytes, "a and b serialize differently");
                }
            }
        }
    };
}
pub use wire_compatible;
