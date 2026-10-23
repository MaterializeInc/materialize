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

use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

use materialize_sdk::core::Checkpoint;
use materialize_sdk::core::checkpoint::MAX_BYTES;
use materialize_sdk::{
    CheckpointWrite, Rejected, Sink, SinkBatch, SinkError, SinkType, WriteKind, WriteRequest,
    WriteResult,
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use crate::checkpoint::Checkpoints;
use crate::client::{Client, Connection, Failure};
use crate::documents::{self, Document, Embedding, Indexing, Layout, Limits};
use crate::pace::{MAX_TOKENS_PER_TEXT, Pacer};

/// The most a request to a data namespace holds. turbopuffer accepts 512 MB,
/// but a smaller request costs less to send again after a lost response.
const MAX_REQUEST_BYTES: usize = 16 << 20;
const MAX_REQUEST_ROWS: usize = 10_000;
/// The most rows with text to embed that turbopuffer takes in one request.
/// Rows that carry a vector do not count: a request with 300 placeholder
/// rows and one text applied in tests.
const MAX_EMBEDDED_ROWS: usize = 256;

/// Room for the document's fields to grow in one write, on top of their
/// current size: one more retired incarnation and a longer revision.
const FIELD_GROWTH_BYTES: usize = 64;

/// How long one write sends requests before it yields, well below the SDK's
/// default write timeout of a minute.
const WRITE_SLICE: Duration = Duration::from_secs(20);

/// How often a write checks that its incarnation is not retired.
const FENCE_CHECK: Duration = Duration::from_secs(20);

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
    /// How long one write sends requests before it yields, [`WRITE_SLICE`]
    /// outside of tests.
    pub(crate) write_slice: Duration,
    /// When the sink last checked that its incarnation is not retired.
    fence_checked: Option<Instant>,
    /// How often it checks, [`FENCE_CHECK`] outside of tests.
    pub(crate) fence_interval: Duration,
    /// Digests of each document turbopuffer acknowledged since the last write
    /// that applied in full, with its namespace and kind, and for a patch
    /// whether it applied. Writing an acknowledged document to the same
    /// namespace again changes nothing, so the resend of a batch after a
    /// timeout or throttling skips it, however its requests split.
    acknowledged: BTreeMap<[u8; 32], bool>,
    /// Digests of each upsert turbopuffer refused on its own since the last
    /// write that applied in full, with its message, so that the resend of a
    /// batch does not repeat the split that found it.
    refused: BTreeMap<[u8; 32], String>,
    /// Whether turbopuffer accepted an upsert request since the last write
    /// that applied in full.
    upserted: bool,
    /// Digests of each document in a request turbopuffer refused whole, since
    /// the last write that applied in full. The resend of a batch sends them
    /// one a request, so a write that yields before it finds the refused row
    /// still makes progress.
    suspect: BTreeSet<[u8; 32]>,
    pacer: Pacer,
    /// The namespaces whose embedding settings match the sink's. turbopuffer
    /// refuses a write that adds an embedded attribute to an existing
    /// namespace or changes its dimensions, so a mismatch would otherwise
    /// stop the sink at some later write.
    matching: BTreeSet<String>,
    embedded_tokens: u64,
}

impl TurbopufferSink {
    /// A sink that keeps the checkpoint of `sink_id`, the same id as its
    /// [`materialize_sdk::DurableSink`]'s, and writes the namespaces named
    /// after `name`, which holds letters, digits, `-`, `_`, and `.` only,
    /// without `__`.
    pub fn new(
        connection: &Connection,
        sink_id: impl Into<String>,
        name: impl Into<String>,
    ) -> Result<TurbopufferSink, SinkError> {
        let name = name.into();
        check_name(&name).map_err(|message| SinkError::Target {
            message,
            permanent: true,
        })?;
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
            write_slice: WRITE_SLICE,
            fence_checked: None,
            fence_interval: FENCE_CHECK,
            acknowledged: BTreeMap::new(),
            refused: BTreeMap::new(),
            upserted: false,
            suspect: BTreeSet::new(),
            pacer: Pacer::new(2_000_000, 1024),
            matching: BTreeSet::new(),
            embedded_tokens: 0,
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

    /// Has turbopuffer embed the text `column` with `model`, one of the models
    /// it hosts, into vectors of `dims` dimensions, searched by cosine
    /// distance in the attribute `embed_<column>`. A namespace embeds at
    /// most four columns. The sink refuses a namespace that embeds
    /// differently, so a change here needs a new sink.
    ///
    /// turbopuffer refuses an empty text to embed, so a row whose text is
    /// empty or `NULL` leaves the attribute out and gets a zero vector, as
    /// does every tombstone. A zero vector is at cosine distance 1 from every
    /// query, so it ranks below every text the query is similar to.
    pub fn embed(mut self, column: impl Into<String>, model: impl Into<String>, dims: u32) -> Self {
        self.indexing.embeddings.push(Embedding {
            column: column.into(),
            model: model.into(),
            dims,
        });
        self
    }

    /// The embedding tokens and requests the sink paces itself to per minute.
    /// It must allow one document's texts, 32,768 tokens for each embedded
    /// column. By default 2 million tokens and 1,024 requests, the limits turbopuffer
    /// gives a new organization for each model. The organization's sinks
    /// share its limits, so divide them between sinks that embed together.
    pub fn embedding_rate(mut self, tokens_per_minute: u64, requests_per_minute: usize) -> Self {
        self.pacer = Pacer::new(tokens_per_minute, requests_per_minute);
        self
    }

    /// The embedding tokens turbopuffer reported billing for this sink's
    /// writes since it was built, including writes whose condition skipped
    /// them.
    pub fn embedding_tokens(&self) -> u64 {
        self.embedded_tokens
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
            check_rate(&self.pacer, &Layout::new(&columns, &self.indexing)?)?;
        }
        Ok(())
    }

    /// Creates the first checkpoint, `checkpoint`, and the checkpoint document.
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

    async fn write_batch(&mut self, batch: &SinkBatch) -> WriteResult {
        let started = Instant::now();
        // A worker learns that it was fenced only at its next commit, which
        // a snapshot reaches after hours of embedding. Its incarnation being
        // retired tells it sooner, since its owner never writes one again.
        if self
            .fence_checked
            .is_none_or(|at| at.elapsed() >= self.fence_interval)
        {
            match self.checkpoints.load().await {
                Ok(Some(document)) if document.retired.contains(&batch.generation()) => {
                    return WriteResult::Failed(SinkError::Target {
                        message: format!(
                            "incarnation {} is retired, so a newer worker took over",
                            batch.generation()
                        ),
                        permanent: true,
                    });
                }
                Ok(_) => self.fence_checked = Some(Instant::now()),
                Err(error) => return WriteResult::Failed(error),
            }
        }
        let layout = match &mut self.layout {
            Some(layout) if layout.fits(batch.columns()) => layout,
            slot => match Layout::new(batch.columns(), &self.indexing).and_then(|layout| {
                check_rate(&self.pacer, &layout)?;
                Ok(layout)
            }) {
                Ok(layout) => slot.insert(layout),
                Err(error) => return WriteResult::Failed(error),
            },
        };
        let namespace = data_namespace(&self.name, batch.generation());
        // A namespace that does not exist yet holds nothing a patch could match.
        let mut exists = true;
        if !self.matching.contains(&namespace) {
            match self.client.embeddings(&namespace).await {
                Ok(None) => exists = false,
                Ok(Some(stored)) if stored == layout.embedded() => {
                    self.matching.insert(namespace.clone());
                }
                Ok(Some(stored)) => {
                    return WriteResult::Failed(SinkError::Target {
                        message: format!(
                            "the namespace {namespace} embeds {stored:?}, but the sink embeds {:?}",
                            layout.embedded()
                        ),
                        permanent: true,
                    });
                }
                Err(failure) => return WriteResult::Failed(failure.into_error()),
            }
        }
        let (mut documents, mut rejected) = documents::documents(batch, layout);
        let limits = Limits {
            bytes: MAX_REQUEST_BYTES,
            rows: self.request_rows,
            embedded_rows: MAX_EMBEDDED_ROWS.min(self.request_rows),
            tokens: self.pacer.request_tokens(),
        };
        let mut sent = Sent {
            client: &self.client,
            namespace: &namespace,
            layout,
            acknowledged: &mut self.acknowledged,
            refused: &mut self.refused,
            upserted: &mut self.upserted,
            suspect: &mut self.suspect,
            pacer: &mut self.pacer,
            started,
            slice: self.write_slice,
            sent_any: false,
            tokens: 0,
        };

        // A patch costs a query per request, and most snapshot documents are
        // new to their namespace, so snapshot writes skip it. A document
        // without text to embed costs nothing to upsert, so it skips it too.
        let mut failed = None;
        if exists && !layout.embeddings().is_empty() && !batch.is_snapshot() {
            let (live, rest): (Vec<_>, Vec<_>) = documents
                .into_iter()
                .partition(|d| !d.is_tombstone() && d.tokens > 0);
            let mut patched = BTreeSet::new();
            let mut patches = Vec::new();
            for document in &live {
                let row = document.patch(layout);
                let patch = Document {
                    index: document.index,
                    bytes: row.to_string().len(),
                    row,
                    tokens: 0,
                };
                match sent.acknowledged(Kind::Patch, &patch) {
                    Some(true) => patched.extend(document.id().map(str::to_string)),
                    Some(false) => {}
                    None => patches.push(patch),
                }
            }
            match sent.send(Kind::Patch, patches, limits).await {
                Ok(applied) => patched.extend(applied),
                Err(failure) => failed = Some(failure),
            }
            documents = live
                .into_iter()
                .filter(|document| document.id().is_none_or(|id| !patched.contains(id)))
                .chain(rest)
                .collect();
        }
        let candidates: Vec<(usize, [u8; 32])> = documents
            .iter()
            .map(|document| (document.index, sent.key(Kind::Upsert, document)))
            .collect();
        if failed.is_none() {
            let unsent = documents
                .into_iter()
                .filter(|document| {
                    sent.acknowledged(Kind::Upsert, document).is_none()
                        && !sent.refused.contains_key(&sent.key(Kind::Upsert, document))
                })
                .collect();
            if let Err(failure) = sent.send(Kind::Upsert, unsent, limits).await {
                failed = Some(failure);
            }
        }
        let tokens = sent.tokens;
        self.embedded_tokens = self.embedded_tokens.saturating_add(tokens);
        if let Some(failure) = failed {
            if let Failure::Throttled {
                retry_after: Some(Duration::ZERO),
            } = failure
            {
                tracing::debug!(sink = %self.name, "a long write yields");
            }
            return WriteResult::Failed(failure.into_error());
        }
        let refusals: Vec<Rejected> = candidates
            .iter()
            .filter_map(|(index, key)| {
                self.refused.get(key).map(|message| Rejected {
                    index: *index,
                    category: "refused".into(),
                    message: message.clone(),
                })
            })
            .collect();
        // Every upsert refused, and none accepted, points at the request
        // rather than the rows, such as an embedding model turbopuffer does
        // not have. Rejecting them all would commit past every row.
        if let Some(first) = refusals.first().filter(|_| !self.upserted) {
            return WriteResult::Failed(SinkError::Target {
                message: format!(
                    "turbopuffer refused every row of the batch, so the sink stops instead of \
                     skipping them: {}",
                    first.message
                ),
                permanent: true,
            });
        }
        rejected.extend(refusals);
        self.acknowledged.clear();
        self.refused.clear();
        self.upserted = false;
        self.suspect.clear();
        rejected.sort_by_key(|rejected| rejected.index);
        WriteResult::Applied { rejected }
    }

    async fn commit(&mut self, checkpoint: &CheckpointWrite) -> WriteResult {
        if let WriteKind::CompleteSnapshot { generation, .. } = checkpoint.kind {
            // Readers switch to the new incarnation with this write.
            let namespace = data_namespace(&self.name, generation);
            match self.client.unindexed_bytes(&namespace).await {
                Ok(Some(bytes)) if bytes > self.max_unindexed_bytes => {
                    tracing::info!(
                        sink = %self.name,
                        unindexed_bytes = bytes,
                        "waiting for turbopuffer to index the snapshot before readers switch"
                    );
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
}

/// Refuses a name that cannot prefix the sink's namespaces.
pub(crate) fn check_name(name: &str) -> Result<(), String> {
    let valid = |c: char| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.');
    if name.is_empty() || name.len() > 100 || name.contains("__") || !name.chars().all(valid) {
        return Err(format!(
            "{name:?} cannot name turbopuffer namespaces: use up to 100 letters, digits, `-`, \
             `_`, and `.`, without `__`"
        ));
    }
    Ok(())
}

/// Refuses an embedding rate too low for one document, which turbopuffer
/// would refuse every time.
fn check_rate(pacer: &Pacer, layout: &Layout) -> Result<(), SinkError> {
    let columns = u64::try_from(layout.embeddings().len()).unwrap_or(u64::MAX);
    let needed = columns.saturating_mul(MAX_TOKENS_PER_TEXT);
    if pacer.tokens_per_minute() < needed {
        return Err(SinkError::Target {
            message: format!(
                "an embedding rate of {} tokens a minute is too low for one document, which \
                 needs up to {needed}",
                pacer.tokens_per_minute()
            ),
            permanent: true,
        });
    }
    Ok(())
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

    async fn load(&mut self) -> Result<Option<String>, SinkError> {
        Ok(self
            .checkpoints
            .load()
            .await?
            .map(|document| document.checkpoint))
    }

    async fn write(&mut self, request: WriteRequest<'_>) -> WriteResult {
        match request {
            WriteRequest::Batch(batch) => self.write_batch(batch).await,
            WriteRequest::Checkpoint(write) if write.expected.is_none() => {
                self.create(&write.new).await
            }
            WriteRequest::Checkpoint(write) => self.commit(write).await,
            WriteRequest::BatchAndCheckpoint(..) => WriteResult::Failed(SinkError::Target {
                message: "turbopuffer cannot write a batch and its checkpoint together".into(),
                permanent: true,
            }),
        }
    }

    fn max_checkpoint_bytes(&self) -> usize {
        let fields = self.checkpoints.field_bytes();
        // The text is compact JSON, without raw control characters, stored
        // inside the document's JSON, where escaping takes at most two bytes
        // for each of its bytes. So within this budget the SDK refuses an
        // oversized checkpoint with its own error.
        MAX_BYTES.saturating_sub(fields.saturating_add(FIELD_GROWTH_BYTES)) / 2
    }

    // TODO: Delete retired incarnations once twice the grace period has
    // passed, as the design's appendix describes.
}

/// Which kind of write a request makes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Kind {
    /// Patches that keep the stored vectors, see
    /// [`Layout::unchanged_text_condition`].
    Patch,
    /// Upserts conditional on a newer `mz_timestamp`.
    Upsert,
}

/// Sends the documents of one write to one namespace.
struct Sent<'a> {
    client: &'a Client,
    namespace: &'a str,
    layout: &'a Layout,
    acknowledged: &'a mut BTreeMap<[u8; 32], bool>,
    refused: &'a mut BTreeMap<[u8; 32], String>,
    upserted: &'a mut bool,
    suspect: &'a mut BTreeSet<[u8; 32]>,
    pacer: &'a mut Pacer,
    started: Instant,
    slice: Duration,
    sent_any: bool,
    /// The embedding tokens turbopuffer reported for this call.
    tokens: u64,
}

impl Sent<'_> {
    fn key(&self, kind: Kind, document: &Document) -> [u8; 32] {
        Sha256::new()
            .chain_update(self.namespace.as_bytes())
            .chain_update([0, u8::from(kind == Kind::Patch)])
            .chain_update(document.row.to_string().as_bytes())
            .finalize()
            .into()
    }

    /// Whether turbopuffer acknowledged `document` in an earlier call for
    /// this batch, and for a patch whether it applied.
    fn acknowledged(&self, kind: Kind, document: &Document) -> Option<bool> {
        self.acknowledged.get(&self.key(kind, document)).copied()
    }

    /// Sends `documents` in requests within `limits`, and returns the ids a
    /// patch applied to. A request turbopuffer refuses is split until the
    /// refused rows are found: a refused upsert becomes rejected, and a
    /// refused patch is left to its upsert.
    async fn send(
        &mut self,
        kind: Kind,
        documents: Vec<Document>,
        limits: Limits,
    ) -> Result<BTreeSet<String>, Failure> {
        let mut applied = BTreeSet::new();
        let (suspects, documents): (Vec<_>, Vec<_>) = documents
            .into_iter()
            .partition(|document| self.suspect.contains(&self.key(kind, document)));
        let mut requests = documents::requests(documents, limits);
        requests.extend(suspects.into_iter().map(|document| vec![document]));
        requests.reverse();
        while let Some(mut request) = requests.pop() {
            // A long write yields as throttling, which the SDK sends again
            // after a second without using up an attempt, so a batch of any
            // size makes progress within the SDK's write timeout.
            if self.sent_any && self.started.elapsed() >= self.slice {
                return Err(Failure::Throttled {
                    retry_after: Some(Duration::ZERO),
                });
            }
            let estimate: u64 = request.iter().map(|document| document.tokens).sum();
            let reservation = if estimate > 0 {
                match self.pacer.reserve(Instant::now(), estimate) {
                    Ok(reservation) => Some(reservation),
                    Err(wait) => {
                        return Err(Failure::Throttled {
                            retry_after: Some(wait),
                        });
                    }
                }
            } else {
                None
            };
            self.sent_any = true;
            let written = self
                .client
                .write(self.namespace, self.body(kind, &request))
                .await;
            if let Some(reservation) = reservation {
                match &written {
                    Ok(response) => {
                        let tokens = response.performance.embedding_tokens;
                        self.pacer.applied(reservation, tokens.unwrap_or(estimate));
                    }
                    Err(Failure::Throttled { .. }) => self.pacer.refused(reservation),
                    Err(Failure::Refused {
                        status: 400 | 422, ..
                    }) => self.pacer.returned(reservation),
                    Err(_) => {}
                }
            }
            match written {
                Ok(response) => {
                    let tokens = response.performance.embedding_tokens.unwrap_or(0);
                    self.tokens = self.tokens.saturating_add(tokens);
                    if kind == Kind::Upsert {
                        *self.upserted = true;
                    }
                    let patched: BTreeSet<&str> = response
                        .patched_ids
                        .iter()
                        .filter_map(Value::as_str)
                        .collect();
                    for document in &request {
                        let applied_here = match kind {
                            Kind::Patch => document.id().is_some_and(|id| patched.contains(id)),
                            Kind::Upsert => true,
                        };
                        if kind == Kind::Patch && applied_here {
                            applied.extend(document.id().map(str::to_string));
                        }
                        let key = self.key(kind, document);
                        self.acknowledged.insert(key, applied_here);
                    }
                }
                Err(Failure::Refused {
                    status: 400 | 422,
                    message,
                }) => {
                    if request.len() > 1 {
                        for document in &request {
                            let key = self.key(kind, document);
                            self.suspect.insert(key);
                        }
                        let second = request.split_off(request.len() / 2);
                        requests.push(second);
                        requests.push(request);
                    } else if kind == Kind::Upsert {
                        for document in &request {
                            let key = self.key(kind, document);
                            self.refused.insert(key, message.clone());
                        }
                    }
                }
                Err(failure) => return Err(failure),
            }
        }
        Ok(applied)
    }

    fn body(&self, kind: Kind, request: &[Document]) -> String {
        let rows: Vec<&Value> = request.iter().map(|document| &document.row).collect();
        let body = match kind {
            Kind::Patch => json!({
                "patch_rows": rows,
                "patch_condition": self.layout.unchanged_text_condition(),
                "return_affected_ids": true,
            }),
            Kind::Upsert if self.layout.embeddings().is_empty() => json!({
                "upsert_rows": rows,
                "upsert_condition": documents::newer_condition(),
                "schema": self.layout.schema(),
            }),
            Kind::Upsert => json!({
                "upsert_rows": rows,
                "upsert_condition": documents::newer_condition(),
                "schema": self.layout.schema(),
                "distance_metric": "cosine_distance",
            }),
        };
        body.to_string()
    }
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
