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

use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use mz_persist::location::Indeterminate;
use mz_persist_types::{Codec, Codec64};
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
/// logical compaction. Users are advised to durably record the intended
/// [CriticalReaderId] _before_ registering a SinceHandle (in case the process
/// crashes at the wrong time).
///
/// All async methods on ReadHandle retry for as long as they are able, but the
/// returned [std::future::Future]s implement "cancel on drop" semantics. This
/// means that callers can add a timeout using [tokio::time::timeout] or
/// [tokio::time::timeout_at].
#[derive(Debug)]
pub struct SinceHandle<K, V, T, D>
where
    K: Debug + Codec,
    V: Debug + Codec,
    T: Timestamp + Lattice + Codec64,
    D: Semigroup + Codec64 + Send + Sync,
{
    pub(crate) machine: Machine<K, V, T, D>,
    pub(crate) gc: GarbageCollector<K, V, T, D>,
    pub(crate) reader_id: CriticalReaderId,
    pub(crate) token: [u8; 8],

    since: Antichain<T>,
}

impl<K, V, T, D> SinceHandle<K, V, T, D>
where
    K: Debug + Codec,
    V: Debug + Codec,
    T: Timestamp + Lattice + Codec64,
    D: Semigroup + Codec64 + Send + Sync,
{
    pub(crate) fn new(
        machine: Machine<K, V, T, D>,
        gc: GarbageCollector<K, V, T, D>,
        reader_id: CriticalReaderId,
        since: Antichain<T>,
        token: [u8; 8],
    ) -> Self {
        SinceHandle {
            machine,
            gc,
            reader_id,
            since,
            token,
        }
    }

    /// This handle's `since` capability.
    ///
    /// This will always be greater or equal to the shard-global `since`.
    pub fn since(&self) -> &Antichain<T> {
        &self.since
    }

    /// Forwards the since capability of this handle to `new_since` iff the
    /// current capability of this handle's [CriticalReaderId] is
    /// `expected_since`.
    ///
    /// This may trigger (asynchronous) compaction and consolidation in the
    /// system. A `new_since` of the empty antichain "finishes" this shard,
    /// promising that no more data will ever be read by this handle.
    ///
    /// Because SinceHandles are expected to live beyond process lifetimes, it's
    /// possible for the same [CriticalReaderId] to be used concurrently from
    /// multiple processes (either intentionally or something like a zombie
    /// process). To discover this, [Self::compare_and_downgrade_since] has
    /// "compare and set" semantics. If `expected_since` doesn't match, the
    /// actual current value is returned and the caller must decide how to
    /// handle it (likely a retry or a `halt!`).
    ///
    /// WIP: Figure out how to get rid of the Indeterminate. It's subtle but
    /// doable: an easier version of an analogous issue with compare_and_append
    /// #12797.
    #[instrument(level = "debug", skip_all, fields(shard = %self.machine.shard_id()))]
    pub async fn compare_and_downgrade_since(
        &mut self,
        expected_since: &Antichain<T>,
        new_since: &Antichain<T>,
    ) -> Result<Result<(), Since<T>>, Indeterminate> {
        let (res, maintenance) = self
            .machine
            .compare_and_downgrade_since(&self.reader_id, expected_since, new_since)
            .await?;
        maintenance.start_performing(&self.machine, &self.gc);
        match res {
            Ok(x) => {
                self.since = x.0;
                Ok(Ok(()))
            }
            Err(x) => {
                self.since = x.0.clone();
                Ok(Err(x))
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
}
