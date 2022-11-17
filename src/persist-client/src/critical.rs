// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Since capabilities and handles

use std::fmt::Debug;
use std::time::Duration;

use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use mz_ore::now::EpochMillis;
use mz_persist_types::{Codec, Codec64, Opaque};
use serde::{Deserialize, Serialize};
use timely::progress::{Antichain, Timestamp};
use tracing::instrument;
use uuid::Uuid;

use crate::internal::machine::Machine;
use crate::{parse_id, GarbageCollector, Since};

/// An opaque identifier for a reader of a persist durable TVC (aka shard).
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct CriticalReaderId(pub(crate) [u8; 16]);

impl std::fmt::Display for CriticalReaderId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "c{}", Uuid::from_bytes(self.0))
    }
}

impl std::fmt::Debug for CriticalReaderId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CriticalReaderId({})", Uuid::from_bytes(self.0))
    }
}

impl std::str::FromStr for CriticalReaderId {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        parse_id('c', "CriticalReaderId", s).map(CriticalReaderId)
    }
}

impl From<CriticalReaderId> for String {
    fn from(reader_id: CriticalReaderId) -> Self {
        reader_id.to_string()
    }
}

impl TryFrom<String> for CriticalReaderId {
    type Error = String;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        s.parse()
    }
}

impl CriticalReaderId {
    /// Returns a random [CriticalReaderId] that is reasonably likely to have
    /// never been generated before.
    ///
    /// This is intentionally public, unlike [crate::read::LeasedReaderId] and
    /// [crate::write::WriterId], because [SinceHandle]s are expected to live
    /// beyond process lifetimes.
    pub fn new() -> Self {
        CriticalReaderId(*Uuid::new_v4().as_bytes())
    }
}

/// A "capability" granting the ability to hold back the `since` frontier of a
/// shard.
///
/// In contrast to [crate::read::ReadHandle], which is time-leased, this handle
/// and its associated capability are not leased. A SinceHandle only releases
/// its since capability when [Self::expire] is called. Also unlike
/// `ReadHandle`, expire is not called on drop. This is less ergonomic, but
/// useful for "critical" since holds which must survive even lease timeouts.
///
/// **IMPORTANT**: The above means that if a SinceHandle is registered and then
/// lost, the shard's since will be permanently "stuck", forever preventing
/// logical compaction. Users are advised to durably record (preferably in code)
/// the intended [CriticalReaderId] _before_ registering a SinceHandle (in case
/// the process crashes at the wrong time).
///
/// All async methods on SinceHandle retry for as long as they are able, but the
/// returned [std::future::Future]s implement "cancel on drop" semantics. This
/// means that callers can add a timeout using [tokio::time::timeout] or
/// [tokio::time::timeout_at].
#[derive(Debug)]
pub struct SinceHandle<K, V, T, D, O>
where
    K: Debug + Codec,
    V: Debug + Codec,
    T: Timestamp + Lattice + Codec64,
    D: Semigroup + Codec64 + Send + Sync,
    O: Opaque + Codec64,
{
    pub(crate) machine: Machine<K, V, T, D>,
    pub(crate) gc: GarbageCollector<K, V, T, D>,
    pub(crate) reader_id: CriticalReaderId,

    since: Antichain<T>,
    opaque: O,
    last_downgrade_since: EpochMillis,
}

impl<K, V, T, D, O> SinceHandle<K, V, T, D, O>
where
    K: Debug + Codec,
    V: Debug + Codec,
    T: Timestamp + Lattice + Codec64,
    D: Semigroup + Codec64 + Send + Sync,
    O: Opaque + Codec64,
{
    pub(crate) fn new(
        machine: Machine<K, V, T, D>,
        gc: GarbageCollector<K, V, T, D>,
        reader_id: CriticalReaderId,
        since: Antichain<T>,
        opaque: O,
    ) -> Self {
        SinceHandle {
            machine,
            gc,
            reader_id,
            since,
            opaque,
            last_downgrade_since: EpochMillis::default(),
        }
    }

    /// This handle's `since` capability.
    ///
    /// This will always be greater or equal to the shard-global `since`.
    pub fn since(&self) -> &Antichain<T> {
        &self.since
    }

    /// This handle's `opaque`.
    pub fn opaque(&self) -> &O {
        &self.opaque
    }

    /// Attempts to forward the since capability of this handle to `new_since` iff
    /// the opaque value of this handle's [CriticalReaderId] is `expected`, and
    /// [Self::maybe_compare_and_downgrade_since] chooses to perform the downgrade.
    ///
    /// Users are expected to call this function frequently, but should not expect
    /// `since` to be downgraded with each call -- this function is free to no-op
    /// requests to perform rate-limiting for downstream services. A `None` is returned
    /// for no-op requests, and `Some` is returned when downgrading since.
    ///
    /// When returning `Some(since)`, `since` will be set to the most recent value
    /// known for this critical reader ID, and is guaranteed to be `!less_than(new_since)`.
    ///
    /// Because SinceHandles are expected to live beyond process lifetimes, it's
    /// possible for the same [CriticalReaderId] to be used concurrently from
    /// multiple processes (either intentionally or something like a zombie
    /// process). To discover this, [Self::maybe_compare_and_downgrade_since] has
    /// "compare and set" semantics over an opaque value. If the `expected` opaque
    /// value does not match state, an `Err` is returned and the caller must decide
    /// how to handle it (likely a retry or a `halt!`).
    ///
    /// If desired, users may use the opaque value to fence out concurrent access
    /// of other [SinceHandle]s for a given [CriticalReaderId]. e.g.:
    ///
    /// ```rust,no_run
    /// use timely::progress::Antichain;
    /// use mz_persist_client::critical::SinceHandle;
    /// use mz_persist_types::Codec64;
    ///
    /// # async fn example() {
    /// let fencing_token: u64 = unimplemented!();
    /// let mut since: SinceHandle<String, String, u64, i64, u64> = unimplemented!();
    ///
    /// let new_since: Antichain<u64> = unimplemented!();
    /// let res = since
    ///     .maybe_compare_and_downgrade_since(
    ///         &since.opaque().clone(),
    ///         (&fencing_token, &new_since),
    ///     )
    ///     .await;
    ///
    /// match res {
    ///     Some(Ok(_)) => {
    ///         // we downgraded since!
    ///     }
    ///     Some(Err(actual_fencing_token)) => {
    ///         // compare `fencing_token` and `actual_fencing_token`, etc
    ///     }
    ///     None => {
    ///         // no problem, we'll try again later
    ///     }
    /// }
    /// # }
    /// ```
    ///
    /// If fencing is not required and it's acceptable to have concurrent [SinceHandle] for
    /// a given [CriticalReaderId], the opaque value can be given a default value and ignored:
    ///
    /// ```rust,no_run
    /// use timely::progress::Antichain;
    /// use mz_persist_client::critical::SinceHandle;
    /// use mz_persist_types::Codec64;
    ///
    /// # async fn example() {
    /// let mut since: SinceHandle<String, String, u64, i64, u64> = unimplemented!();
    /// let new_since: Antichain<u64> = unimplemented!();
    /// let res = since
    ///     .maybe_compare_and_downgrade_since(
    ///         &since.opaque().clone(),
    ///         (&since.opaque().clone(), &new_since),
    ///     )
    ///     .await;
    ///
    /// match res {
    ///     Some(Ok(_)) => {
    ///         // woohoo!
    ///     }
    ///     Some(Err(_actual_opaque)) => {
    ///         panic!("the opaque value should never change from the default");
    ///     }
    ///     None => {
    ///         // no problem, we'll try again later
    ///     }
    /// };
    /// # }
    /// ```
    #[instrument(level = "debug", skip_all, fields(shard = %self.machine.shard_id()))]
    pub async fn maybe_compare_and_downgrade_since(
        &mut self,
        expected: &O,
        new: (&O, &Antichain<T>),
    ) -> Option<Result<Since<T>, O>> {
        let elapsed_since_last_downgrade = Duration::from_millis(
            (self.machine.cfg.now)().saturating_sub(self.last_downgrade_since),
        );
        if elapsed_since_last_downgrade >= self.machine.cfg.critical_downgrade_interval {
            Some(self.compare_and_downgrade_since(expected, new).await)
        } else {
            None
        }
    }

    /// Forwards the since capability of this handle to `new_since` iff the opaque value of this
    /// handle's [CriticalReaderId] is `expected`, and `new_since` is beyond the
    /// current `since`.
    ///
    /// Users are expected to call this function only when a guaranteed downgrade is necessary. All
    /// other downgrades should preferably go through [Self::maybe_compare_and_downgrade_since]
    /// which will automatically rate limit the operations.
    ///
    /// When returning `Ok(since)`, `since` will be set to the most recent value known for this
    /// critical reader ID, and is guaranteed to be `!less_than(new_since)`.
    ///
    /// Because SinceHandles are expected to live beyond process lifetimes, it's possible for the
    /// same [CriticalReaderId] to be used concurrently from multiple processes (either
    /// intentionally or something like a zombie process). To discover this,
    /// [Self::compare_and_downgrade_since] has "compare and set" semantics over an opaque value.
    /// If the `expected` opaque value does not match state, an `Err` is returned and the caller
    /// must decide how to handle it (likely a retry or a `halt!`).
    #[instrument(level = "debug", skip_all, fields(shard = %self.machine.shard_id()))]
    pub async fn compare_and_downgrade_since(
        &mut self,
        expected: &O,
        new: (&O, &Antichain<T>),
    ) -> Result<Since<T>, O> {
        let (res, maintenance) = self
            .machine
            .compare_and_downgrade_since(&self.reader_id, expected, new)
            .await;
        self.last_downgrade_since = (self.machine.cfg.now)();
        maintenance.start_performing(&self.machine, &self.gc);
        match res {
            Ok(since) => {
                self.since = since.0.clone();
                self.opaque = new.0.clone();
                Ok(since)
            }
            Err((actual_opaque, since)) => {
                self.since = since.0;
                self.opaque = actual_opaque.clone();
                Err(actual_opaque)
            }
        }
    }

    /// Politely expires this reader, releasing its since capability.
    #[instrument(level = "debug", skip_all, fields(shard = %self.machine.shard_id()))]
    pub async fn expire(mut self) {
        self.machine.expire_critical_reader(&self.reader_id).await;
    }
}

#[cfg(test)]
mod tests {
    use serde::{Deserialize, Serialize};
    use serde_json::json;
    use std::str::FromStr;

    use super::*;
    use crate::tests::new_test_client;
    use crate::{PersistClient, ShardId};

    #[test]
    fn reader_id_human_readable_serde() {
        #[derive(Debug, Serialize, Deserialize)]
        struct Container {
            reader_id: CriticalReaderId,
        }

        // roundtrip through json
        let id =
            CriticalReaderId::from_str("c00000000-1234-5678-0000-000000000000").expect("valid id");
        assert_eq!(
            id,
            serde_json::from_value(serde_json::to_value(id.clone()).expect("serializable"))
                .expect("deserializable")
        );

        // deserialize a serialized string directly
        assert_eq!(
            id,
            serde_json::from_str("\"c00000000-1234-5678-0000-000000000000\"")
                .expect("deserializable")
        );

        // roundtrip id through a container type
        let json = json!({ "reader_id": id });
        assert_eq!(
            "{\"reader_id\":\"c00000000-1234-5678-0000-000000000000\"}",
            &json.to_string()
        );
        let container: Container = serde_json::from_value(json).expect("deserializable");
        assert_eq!(container.reader_id, id);
    }

    #[tokio::test]
    async fn rate_limit() {
        let client = crate::tests::new_test_client().await;

        let shard_id = crate::ShardId::new();

        let mut since = client
            .open_critical_since::<(), (), u64, i64, i64>(shard_id, CriticalReaderId::new())
            .await
            .expect("codec mismatch");

        assert_eq!(since.opaque(), &i64::initial());

        since
            .compare_and_downgrade_since(&i64::initial(), (&5, &Antichain::from_elem(0)))
            .await
            .unwrap();

        // should not fire, since we just had a successful `compare_and_downgrade_since` call
        let noop = since
            .maybe_compare_and_downgrade_since(&5, (&5, &Antichain::from_elem(0)))
            .await;

        assert_eq!(noop, None);
    }

    // Verifies that the handle updates its view of the opaque token correctly
    #[tokio::test]
    async fn handle_opaque_token() {
        let client = new_test_client().await;
        let shard_id = ShardId::new();

        let mut since = client
            .open_critical_since::<(), (), u64, i64, i64>(
                shard_id,
                PersistClient::CONTROLLER_CRITICAL_SINCE,
            )
            .await
            .expect("codec mismatch");

        // The token must be initialized to the default value
        assert_eq!(since.opaque(), &i64::MIN);

        since
            .compare_and_downgrade_since(&i64::MIN, (&5, &Antichain::from_elem(0)))
            .await
            .unwrap();

        // Our view of the token must be updated now
        assert_eq!(since.opaque(), &5);

        let since2 = client
            .open_critical_since::<(), (), u64, i64, i64>(
                shard_id,
                PersistClient::CONTROLLER_CRITICAL_SINCE,
            )
            .await
            .expect("codec mismatch");

        // The token should still be 5
        assert_eq!(since2.opaque(), &5);
    }
}
