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

//! The sink.

use std::collections::BTreeSet;
use std::time::Duration;

use materialize_sdk::core::Checkpoint;
use materialize_sdk::core::checkpoint::MAX_BYTES;
use materialize_sdk::{
    CheckpointWrite, Sink, SinkBatch, SinkError, SinkType, WriteKind, WriteResult,
};
use serde_json::json;
use sha2::{Digest, Sha256};

use crate::checkpoint::Checkpoints;
use crate::client::{Client, Connection};
use crate::documents::{self, Indexing, Layout};

/// The most a request to a data namespace holds. turbopuffer accepts 512 MB,
/// but a smaller request costs less to send again after a lost response.
const MAX_REQUEST_BYTES: usize = 16 << 20;
const MAX_REQUEST_ROWS: usize = 10_000;

/// Room for the document's fields to grow in one write, on top of their
/// current size: one more retired incarnation and a longer revision.
const FIELD_GROWTH_BYTES: usize = 64;

/// How long to wait before checking again whether a snapshot is indexed.
const INDEX_POLL: Duration = Duration::from_secs(5);

/// A sink that keeps turbopuffer namespaces equal to a view. See the crate
/// documentation.
#[derive(Debug)]
pub struct TurbopufferSink {
    pub(crate) client: Client,
    name: String,
    pub(crate) checkpoints: Checkpoints,
    indexing: Indexing,
    max_unindexed_bytes: u64,
    layout: Option<Layout>,
    /// The most rows in one request, [`MAX_REQUEST_ROWS`] outside of tests.
    pub(crate) request_rows: usize,
    /// Digests of the namespace and body of each request turbopuffer
    /// acknowledged since the last write that applied in full. Sending an
    /// acknowledged request to the same namespace again changes nothing, so
    /// the resend of a batch after a timeout or throttling skips them.
    acknowledged: BTreeSet<[u8; 32]>,
}

impl TurbopufferSink {
    /// A sink that keeps the checkpoint of `sink_id` and writes the namespaces
    /// named after `name`, which holds letters, digits, `-`, `_`, and `.`
    /// only, without `__`.
    pub fn new(
        connection: &Connection,
        sink_id: impl Into<String>,
        name: impl Into<String>,
    ) -> Result<TurbopufferSink, SinkError> {
        let name = name.into();
        let valid = |c: char| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.');
        if name.is_empty() || name.len() > 100 || name.contains("__") || !name.chars().all(valid) {
            return Err(SinkError::Target {
                message: format!(
                    "{name:?} cannot name turbopuffer namespaces: use up to 100 letters, digits, \
                     `-`, `_`, and `.`, without `__`"
                ),
                permanent: true,
            });
        }
        let client = Client::new(connection)?;
        let checkpoints =
            Checkpoints::new(client.clone(), checkpoint_namespace(&name), sink_id.into());
        Ok(TurbopufferSink {
            client,
            name,
            checkpoints,
            indexing: Indexing::default(),
            max_unindexed_bytes: 64 << 20,
            layout: None,
            request_rows: MAX_REQUEST_ROWS,
            acknowledged: BTreeSet::new(),
        })
    }

    /// Makes the text `columns` filterable. Other columns are filterable
    /// unless they are text. A filterable value holds at most 4 KiB, and a
    /// longer one is rejected.
    pub fn filterable<S: Into<String>>(mut self, columns: impl IntoIterator<Item = S>) -> Self {
        self.indexing.filterable = columns.into_iter().map(Into::into).collect();
        self
    }

    /// Makes the text `columns` searchable by full text.
    pub fn full_text_search<S: Into<String>>(
        mut self,
        columns: impl IntoIterator<Item = S>,
    ) -> Self {
        self.indexing.full_text_search = columns.into_iter().map(Into::into).collect();
        self
    }

    /// The most unindexed bytes a new incarnation may hold when readers
    /// switch to it, as turbopuffer's namespace metadata reports them. 64 MiB
    /// by default. Once a namespace holds more than 128 MiB of unindexed
    /// writes, eventually consistent queries can miss later writes for up to
    /// about an hour (turbopuffer's query page). Strong queries, the default,
    /// search every unindexed write.
    ///
    /// NOTE: The metadata reports a backlog late, and not at all for a small
    /// one, so the wait is best effort.
    pub fn max_unindexed_bytes(mut self, bytes: u64) -> Self {
        self.max_unindexed_bytes = bytes;
        self
    }
}

impl TurbopufferSink {
    /// Refuses a subscription whose columns cannot become documents, so that
    /// setup fails instead of the first write.
    fn check_layout(&self, checkpoint: &str) -> Result<(), SinkError> {
        let refuse = |message: String| SinkError::Target {
            message,
            permanent: true,
        };
        let checkpoint = Checkpoint::decode(checkpoint).map_err(|e| refuse(e.to_string()))?;
        for fingerprint in checkpoint.subscriptions.values() {
            let columns = fingerprint
                .payload_columns()
                .map_err(|e| refuse(e.to_string()))?;
            Layout::new(&columns, &self.indexing)?;
        }
        Ok(())
    }
}

/// The namespace that holds the checkpoint of the sink named `name`.
pub(crate) fn checkpoint_namespace(name: &str) -> String {
    format!("{name}__checkpoint")
}

/// The namespace of the sink named `name` for `incarnation`, the generation
/// of the snapshot that writes it.
pub fn data_namespace(name: &str, incarnation: u64) -> String {
    format!("{name}__i{incarnation}")
}

impl Sink for TurbopufferSink {
    fn sink_type(&self) -> SinkType {
        SinkType::NonTransactional
    }

    async fn create(&mut self, checkpoint: &str) -> WriteResult {
        if let Err(error) = self.check_layout(checkpoint) {
            return WriteResult::Failed(error);
        }
        match self.checkpoints.load().await {
            Ok(Some(_)) => return WriteResult::NotApplied,
            Ok(None) => {}
            Err(error) => return WriteResult::Failed(error),
        }
        // A worker of an earlier sink under this name could still write to
        // them, so the new sink's snapshot would not start from empty
        // namespaces.
        let prefix = format!("{}__i", self.name);
        let left = match self.client.namespaces(&prefix).await {
            Ok(names) => names,
            Err(failure) => return WriteResult::Failed(failure.into_error()),
        };
        let left: Vec<_> = left
            .into_iter()
            .filter(|name| {
                name.strip_prefix(&prefix)
                    .is_some_and(|rest| rest.bytes().all(|b| b.is_ascii_digit()))
            })
            .collect();
        if !left.is_empty() {
            return WriteResult::Failed(SinkError::Target {
                message: format!(
                    "the namespaces {left:?} are left from an earlier sink: stop its workers, \
                     wait until no delayed write of theirs can land, and delete them"
                ),
                permanent: true,
            });
        }
        self.checkpoints.create(checkpoint).await
    }

    async fn load(&mut self) -> Result<Option<String>, SinkError> {
        Ok(self
            .checkpoints
            .load()
            .await?
            .map(|document| document.checkpoint))
    }

    async fn write(&mut self, batch: &SinkBatch, _: Option<&CheckpointWrite>) -> WriteResult {
        let layout = match &mut self.layout {
            Some(layout) if layout.fits(batch.columns()) => layout,
            slot => match Layout::new(batch.columns(), &self.indexing) {
                Ok(layout) => slot.insert(layout),
                Err(error) => return WriteResult::Failed(error),
            },
        };
        let (documents, rejected) = documents::documents(batch, layout);
        let namespace = data_namespace(&self.name, batch.generation());
        for rows in documents::requests(documents, MAX_REQUEST_BYTES, self.request_rows) {
            let body = json!({
                "upsert_rows": rows,
                "upsert_condition": documents::newer_condition(),
                "schema": layout.schema(),
            })
            .to_string();
            let digest: [u8; 32] = Sha256::new()
                .chain_update(namespace.as_bytes())
                .chain_update([0])
                .chain_update(body.as_bytes())
                .finalize()
                .into();
            if self.acknowledged.contains(&digest) {
                continue;
            }
            if let Err(failure) = self.client.write(&namespace, body).await {
                return WriteResult::Failed(failure.into_error());
            }
            self.acknowledged.insert(digest);
        }
        self.acknowledged.clear();
        WriteResult::Applied { rejected }
    }

    async fn commit(&mut self, checkpoint: &CheckpointWrite) -> WriteResult {
        if let WriteKind::CompleteSnapshot { generation, .. } = checkpoint.kind {
            // Readers switch to the new incarnation with this write.
            let namespace = data_namespace(&self.name, generation);
            match self.client.unindexed_bytes(&namespace).await {
                Ok(Some(bytes)) if bytes > self.max_unindexed_bytes => {
                    return WriteResult::Failed(SinkError::Throttled {
                        retry_after: Some(INDEX_POLL),
                    });
                }
                Ok(_) => {}
                Err(failure) => return WriteResult::Failed(failure.into_error()),
            }
        }
        self.checkpoints.commit(checkpoint).await
    }

    fn max_checkpoint_bytes(&self) -> usize {
        let fields = self.checkpoints.field_bytes();
        MAX_BYTES.saturating_sub(fields.saturating_add(FIELD_GROWTH_BYTES))
    }

    // TODO: Delete retired incarnations once twice the grace period has
    // passed, as the design's appendix describes.
}

#[cfg(test)]
pub(crate) mod tests {
    use std::collections::BTreeMap;

    use materialize_sdk::core::Fingerprint;
    use materialize_sdk::{Column, Envelope, ObjectRef};

    use super::*;

    /// The checkpoint text of a new sink on a view `(id int, title text)`.
    pub(crate) fn checkpoint(envelope: Envelope) -> String {
        let column = |name: &str, type_oid| Column {
            name: name.into(),
            type_oid,
            type_mod: -1,
            nullable: true,
        };
        let fingerprint = Fingerprint {
            object: ObjectRef {
                id: "u1".into(),
                database: Some("materialize".into()),
                schema: "public".into(),
                name: "articles".into(),
            },
            shard: "s1".into(),
            envelope,
            columns: vec![column("id", 23), column("title", 25)],
        };
        Checkpoint::new(BTreeMap::from([("articles".into(), fingerprint)]))
            .encode()
            .unwrap()
    }

    pub(crate) fn keyed() -> Envelope {
        Envelope::Upsert {
            key: vec!["id".into()],
        }
    }

    #[test]
    fn setup_refuses_a_subscription_that_cannot_become_documents() {
        let connection = Connection::new("test", "key");
        let sink = TurbopufferSink::new(&connection, "s", "articles").unwrap();
        assert!(sink.check_layout(&checkpoint(keyed())).is_ok());
        assert!(sink.check_layout(&checkpoint(Envelope::Diff)).is_err());
        let typo = TurbopufferSink::new(&connection, "s", "articles")
            .unwrap()
            .filterable(["titel"]);
        assert!(typo.check_layout(&checkpoint(keyed())).is_err());
    }

    #[test]
    fn names_that_cannot_name_namespaces_are_refused() {
        let connection = Connection::new("test", "key");
        for name in ["", "a__b", "a/b", "a b", &"a".repeat(101)] {
            assert!(
                TurbopufferSink::new(&connection, "s", name).is_err(),
                "{name:?}"
            );
        }
        assert!(TurbopufferSink::new(&connection, "s", "a-b_c.d").is_ok());
    }

    #[test]
    fn debug_output_leaves_the_key_out() {
        let connection = Connection::new("test", "secret-key");
        let sink = TurbopufferSink::new(&connection, "s", "articles").unwrap();
        let printed = format!("{connection:?} {sink:?}");
        assert!(!printed.contains("secret-key"), "{printed}");
    }
}
