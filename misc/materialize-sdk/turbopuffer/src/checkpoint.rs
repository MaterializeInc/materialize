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

//! The checkpoint document: the one document in a sink's checkpoint namespace.
//! It holds the SDK's checkpoint text and the incarnations readers use.
//!
//! Workers change it only with patches conditional on its `revision`, so each
//! patch is a compare-and-set on the whole document, and a patch never
//! creates a deleted document.

use materialize_sdk::core::checkpoint::MAX_BYTES;
use materialize_sdk::{CheckpointWrite, SinkError, WriteKind, WriteResult};
use serde_json::{Map, Value, json};

use crate::client::{Client, Failure};

/// The id of the checkpoint document.
pub(crate) const DOCUMENT_ID: &str = "checkpoint";

/// The layout of the document's attributes. A reader refuses any other.
pub(crate) const LAYOUT: u64 = 1;

/// The checkpoint document.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Document {
    pub(crate) revision: u64,
    pub(crate) sink_id: String,
    /// The SDK's checkpoint text.
    pub(crate) checkpoint: String,
    /// The incarnation readers use, set by the write that completes a
    /// snapshot.
    pub(crate) active: Option<u64>,
    /// The incarnation a snapshot in progress writes.
    pub(crate) snapshot: Option<u64>,
    /// Incarnations readers no longer use and the active worker no longer
    /// writes, in the order they retired. Fenced workers and delayed writes
    /// can still reach them.
    pub(crate) retired: Vec<u64>,
}

impl Document {
    fn new(sink_id: &str, checkpoint: &str) -> Document {
        Document {
            revision: 0,
            sink_id: sink_id.to_string(),
            checkpoint: checkpoint.to_string(),
            active: None,
            snapshot: None,
            retired: Vec::new(),
        }
    }

    /// The document after `write`.
    pub(crate) fn after(&self, write: &CheckpointWrite) -> Document {
        let mut next = Document {
            revision: self.revision.saturating_add(1),
            checkpoint: write.new.clone(),
            ..self.clone()
        };
        match write.kind {
            // A snapshot that never completed leaves its incarnation behind.
            WriteKind::StartSnapshot { generation, .. } => {
                if let Some(abandoned) = next.snapshot.replace(generation) {
                    next.retire(abandoned);
                }
            }
            WriteKind::CompleteSnapshot { generation, .. } => {
                next.snapshot = None;
                if let Some(old) = next.active.replace(generation) {
                    next.retire(old);
                }
            }
            _ => {}
        }
        next
    }

    fn retire(&mut self, incarnation: u64) {
        if Some(incarnation) != self.active
            && Some(incarnation) != self.snapshot
            && !self.retired.contains(&incarnation)
        {
            self.retired.push(incarnation);
        }
    }

    /// The attributes a patch replaces.
    fn changes(&self) -> Map<String, Value> {
        let mut changes = Map::new();
        changes.insert("id".into(), json!(DOCUMENT_ID));
        changes.insert("revision".into(), json!(self.revision));
        changes.insert("checkpoint".into(), json!(self.checkpoint));
        changes.insert("active".into(), json!(self.active));
        changes.insert("snapshot".into(), json!(self.snapshot));
        changes.insert("retired".into(), json!(self.retired));
        changes
    }

    fn row(&self) -> Map<String, Value> {
        let mut row = self.changes();
        row.insert("sink_id".into(), json!(self.sink_id));
        row.insert("layout".into(), json!(LAYOUT));
        row
    }

    /// The bytes of the document's JSON with an empty checkpoint text. A text
    /// adds at least its own length.
    fn field_bytes(&self) -> usize {
        let empty = Document {
            revision: self.revision,
            sink_id: self.sink_id.clone(),
            checkpoint: String::new(),
            active: self.active,
            snapshot: self.snapshot,
            retired: self.retired.clone(),
        };
        empty.encoded_bytes()
    }

    fn encoded_bytes(&self) -> usize {
        Value::Object(self.row()).to_string().len()
    }

    /// Refuses a document whose JSON is over the checkpoint limit. The SDK
    /// checks the text alone, and escaping it in the document can add bytes.
    fn check_size(&self) -> Result<(), SinkError> {
        let size = self.encoded_bytes();
        if size > MAX_BYTES {
            return Err(SinkError::Target {
                message: format!(
                    "the checkpoint document would hold {size} bytes, over the limit of {MAX_BYTES}"
                ),
                permanent: true,
            });
        }
        Ok(())
    }

    pub(crate) fn from_row(row: &Value) -> Result<Document, SinkError> {
        let invalid = |what: &str| SinkError::Target {
            message: format!("the checkpoint document is invalid: {what}"),
            permanent: true,
        };
        let number = |name: &str| row.get(name).and_then(Value::as_u64);
        let text = |name: &str| row.get(name).and_then(Value::as_str).map(str::to_string);
        // turbopuffer leaves out an attribute that is null.
        let optional = |name: &str| match row.get(name) {
            None | Some(Value::Null) => Ok(None),
            Some(value) => value.as_u64().map(Some).ok_or_else(|| invalid(name)),
        };
        match number("layout") {
            Some(LAYOUT) => {}
            other => return Err(invalid(&format!("layout {other:?}, expected {LAYOUT}"))),
        }
        let retired = match row.get("retired") {
            None | Some(Value::Null) => Vec::new(),
            Some(Value::Array(values)) => values
                .iter()
                .map(|value| value.as_u64().ok_or_else(|| invalid("retired")))
                .collect::<Result<_, _>>()?,
            Some(_) => return Err(invalid("retired")),
        };
        Ok(Document {
            revision: number("revision").ok_or_else(|| invalid("revision"))?,
            sink_id: text("sink_id").ok_or_else(|| invalid("sink_id"))?,
            checkpoint: text("checkpoint").ok_or_else(|| invalid("checkpoint"))?,
            active: optional("active")?,
            snapshot: optional("snapshot")?,
            retired,
        })
    }
}

/// The checkpoint document of one sink.
#[derive(Debug)]
pub(crate) struct Checkpoints {
    client: Client,
    namespace: String,
    sink_id: String,
    /// The document as last read or written. `None` while a patch is in
    /// flight, and after one that did not apply or whose result is unknown,
    /// so that the next patch reads the document first. A patch stopped
    /// before it could apply, by the size check or by throttling, leaves it
    /// in place.
    known: Option<Document>,
}

impl Checkpoints {
    pub(crate) fn new(client: Client, namespace: String, sink_id: String) -> Checkpoints {
        Checkpoints {
            client,
            namespace,
            sink_id,
            known: None,
        }
    }

    /// [`Document::field_bytes`] of the document as last read or written, or
    /// of a new one.
    pub(crate) fn field_bytes(&self) -> usize {
        match &self.known {
            Some(document) => document.field_bytes(),
            None => Document::new(&self.sink_id, "").field_bytes(),
        }
    }

    /// Creates the document with `checkpoint`, only if there is none.
    pub(crate) async fn create(&mut self, checkpoint: &str) -> WriteResult {
        let document = Document::new(&self.sink_id, checkpoint);
        if let Err(error) = document.check_size() {
            return WriteResult::Failed(error);
        }
        // turbopuffer applies an upsert to a missing document without its
        // condition and skips it when the document exists, and every
        // document has an id. So the upsert only creates, and a delayed copy
        // of it cannot overwrite a live checkpoint.
        let body = json!({
            "upsert_rows": [document.row()],
            "upsert_condition": ["id", "Eq", null],
            "schema": schema(),
        });
        match self.client.write(&self.namespace, body.to_string()).await {
            Ok(response) if response.rows_upserted == 1 => {
                self.known = Some(document);
                WriteResult::applied()
            }
            Ok(_) => WriteResult::NotApplied,
            Err(failure) => failure.into_write_result(),
        }
    }

    /// Reads the document, or `None` if there is none.
    pub(crate) async fn load(&mut self) -> Result<Option<Document>, SinkError> {
        self.known = None;
        let body = json!({
            "filters": ["id", "Eq", DOCUMENT_ID],
            "rank_by": ["id", "asc"],
            "limit": 1,
            "include_attributes": true,
        });
        let rows = self
            .client
            .query(&self.namespace, &body)
            .await
            .map_err(Failure::into_error)?;
        let Some(row) = rows.as_deref().and_then(<[Value]>::first) else {
            return Ok(None);
        };
        let document = Document::from_row(row)?;
        if document.sink_id != self.sink_id {
            return Err(SinkError::Target {
                message: format!(
                    "the namespace {} keeps the checkpoint of sink {:?}, not {:?}",
                    self.namespace, document.sink_id, self.sink_id
                ),
                permanent: true,
            });
        }
        self.known = Some(document.clone());
        Ok(Some(document))
    }

    /// Applies `write` with a patch conditional on the revision last read or
    /// written, only if the document still holds the expected text.
    pub(crate) async fn commit(&mut self, write: &CheckpointWrite) -> WriteResult {
        // The SDK settles a write that did not apply or whose result is
        // unknown by the checkpoint text alone, and that settles the other
        // fields too: every write changes the text, and the fields after a
        // write follow from the document that held its expected text.
        let current = match self.known.take() {
            Some(document) => document,
            None => match self.load().await {
                Ok(Some(document)) => document,
                Ok(None) => {
                    return WriteResult::Failed(SinkError::Target {
                        message: format!("the namespace {} has no checkpoint", self.namespace),
                        permanent: true,
                    });
                }
                Err(error) => return WriteResult::Failed(error),
            },
        };
        // `load` cached the document, and a patch dropped in flight must
        // leave nothing cached.
        self.known = None;
        if write.expected.as_deref() != Some(current.checkpoint.as_str()) {
            return WriteResult::NotApplied;
        }
        let next = current.after(write);
        if let Err(error) = next.check_size() {
            self.known = Some(current);
            return WriteResult::Failed(error);
        }
        let body = json!({
            "patch_rows": [next.changes()],
            "patch_condition": ["revision", "Eq", current.revision],
        });
        match self.client.write(&self.namespace, body.to_string()).await {
            Ok(response) if response.rows_patched == 1 => {
                self.known = Some(next);
                WriteResult::applied()
            }
            Ok(_) => WriteResult::NotApplied,
            Err(Failure::Throttled { retry_after }) => {
                // Throttled before it applied, so the document is unchanged.
                self.known = Some(current);
                WriteResult::Failed(SinkError::Throttled { retry_after })
            }
            Err(failure) => failure.into_write_result(),
        }
    }
}

/// Only `revision` is filterable, because turbopuffer allows 8 MiB in one
/// attribute but only 4 KiB in a value it filters on.
fn schema() -> Value {
    let stored = |kind: &str| json!({"type": kind, "filterable": false});
    json!({
        "revision": {"type": "uint", "filterable": true},
        "layout": stored("uint"),
        "sink_id": stored("string"),
        "checkpoint": stored("string"),
        "active": stored("uint"),
        "snapshot": stored("uint"),
        "retired": stored("[]uint"),
    })
}

#[cfg(test)]
mod tests {
    use materialize_sdk::core::Checkpoint;
    use materialize_sdk::{Frontier, WriteKind};

    use super::*;

    fn write(kind: WriteKind) -> CheckpointWrite {
        CheckpointWrite {
            expected: Some("a".into()),
            new: "b".into(),
            kind,
        }
    }

    fn start(old_generation: u64, generation: u64) -> CheckpointWrite {
        write(WriteKind::StartSnapshot {
            as_of: 5,
            old_generation,
            generation,
        })
    }

    fn complete(generation: u64) -> CheckpointWrite {
        write(WriteKind::CompleteSnapshot {
            frontier: Frontier::new(9).unwrap(),
            generation,
        })
    }

    #[test]
    fn incarnations_retire_once_no_worker_writes_them() {
        let first = Document::new("s", "a");
        // The first snapshot leaves nothing behind: generation 1 never wrote.
        let started = first.after(&start(1, 2));
        assert_eq!(
            (started.snapshot, started.retired.as_slice()),
            (Some(2), &[][..])
        );
        assert_eq!((started.revision, started.checkpoint.as_str()), (1, "b"));

        // A restarted snapshot retires the one that never completed.
        let restarted = started.after(&start(2, 3));
        assert_eq!(
            (restarted.snapshot, restarted.retired.as_slice()),
            (Some(3), &[2][..])
        );

        let completed = restarted.after(&complete(3));
        assert_eq!(
            (
                completed.active,
                completed.snapshot,
                completed.retired.as_slice()
            ),
            (Some(3), None, &[2][..])
        );

        // A re-snapshot retires the active incarnation once it completes.
        let replaced = completed.after(&start(3, 4)).after(&complete(4));
        assert_eq!(
            (replaced.active, replaced.retired.as_slice()),
            (Some(4), &[2, 3][..])
        );

        let committed = replaced.after(&write(WriteKind::Commit {
            frontier: Frontier::new(10).unwrap(),
        }));
        assert_eq!(
            (
                committed.active,
                committed.retired.as_slice(),
                committed.revision
            ),
            (Some(4), &[2, 3][..], replaced.revision + 1)
        );
    }

    #[test]
    fn the_whole_document_counts_against_the_checkpoint_limit() {
        let empty = Document::new("s", "").field_bytes();
        let fits = Document::new("s", &"x".repeat(MAX_BYTES - empty));
        assert!(fits.check_size().is_ok());
        let over = Document::new("s", &"x".repeat(MAX_BYTES - empty + 1));
        assert!(over.check_size().is_err());
        // A quote in the text takes two bytes once escaped.
        let quoted = Document::new("s", &format!("\"{}", "x".repeat(MAX_BYTES - empty - 1)));
        assert!(quoted.check_size().is_err());
    }

    #[test]
    fn a_document_reads_back_as_it_was_written() {
        let document = Document::new("s", &Checkpoint::new(Default::default()).encode().unwrap())
            .after(&start(1, 2))
            .after(&complete(2))
            .after(&start(2, 3));
        let mut row = Value::Object(document.row());
        assert_eq!(Document::from_row(&row).unwrap(), document);

        // turbopuffer leaves out null attributes.
        row.as_object_mut().unwrap().remove("snapshot");
        let read = Document::from_row(&row).unwrap();
        assert_eq!(read.snapshot, None);

        row["layout"] = json!(2);
        assert!(Document::from_row(&row).is_err());
    }
}
