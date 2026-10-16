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

//! The data documents: one per key of the upsert envelope, holding every
//! column, its `mz_timestamp`, and whether the key was deleted.
//!
//! A delete writes a tombstone in place of deleting the document, and every
//! upsert is conditional on the stored `mz_timestamp` being older. A replayed
//! or stale write of an older change then meets a newer document or
//! tombstone and is skipped, so every write is safe to repeat and to apply
//! late.

use std::collections::{BTreeMap, BTreeSet};

use materialize_sdk::{Change, Column, PayloadColumns, Rejected, SinkBatch, SinkError, Update};
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};

use crate::pace::estimate_tokens;

/// The largest id turbopuffer accepts, in bytes.
const MAX_ID_BYTES: usize = 64;
/// The largest value turbopuffer filters on, in bytes.
const MAX_FILTERABLE_BYTES: usize = 4096;
/// The largest attribute value turbopuffer accepts, in bytes.
const MAX_VALUE_BYTES: usize = 8 << 20;
/// The largest document turbopuffer accepts, in bytes.
const MAX_DOCUMENT_BYTES: usize = 64 << 20;
/// The most attribute names turbopuffer allows in a namespace.
const MAX_ATTRIBUTES: usize = 1024;
/// The longest attribute name turbopuffer allows, in bytes.
const MAX_NAME_BYTES: usize = 128;

/// The attributes the sink writes on every document. A column with one of
/// these names, or one turbopuffer reserves, cannot be an attribute.
const RESERVED: [&str; 4] = ["id", "mz_timestamp", "deleted", "vector"];

/// The condition on every data upsert: only a newer change replaces the
/// stored document. A missing document is created without the condition.
pub(crate) fn newer_condition() -> Value {
    json!(["mz_timestamp", "Lt", {"$ref_new": "mz_timestamp"}])
}

/// The most embedded attributes turbopuffer allows in a namespace.
const MAX_EMBEDDINGS: usize = 4;
/// The most dimensions turbopuffer allows in a dense vector.
const MAX_DIMS: u32 = 10_752;

/// Which columns readers filter, search, or search by meaning on.
#[derive(Clone, Debug, Default)]
pub(crate) struct Indexing {
    pub(crate) filterable: BTreeSet<String>,
    pub(crate) full_text_search: BTreeSet<String>,
    pub(crate) embeddings: Vec<Embedding>,
}

/// A text column that turbopuffer embeds with one of the models it hosts.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Embedding {
    pub(crate) column: String,
    pub(crate) model: String,
    pub(crate) dims: u32,
}

impl Embedding {
    /// The attribute turbopuffer stores the vector in.
    pub(crate) fn vector(&self) -> String {
        format!("embed_{}", self.column)
    }

    /// The attribute that holds a hash of the embedded text, so that a write
    /// can keep a stored vector whose text did not change.
    pub(crate) fn hash(&self) -> String {
        format!("mz_hash_{}", self.column)
    }

    /// The vector of a document without text.
    fn placeholder(&self) -> Value {
        Value::from(vec![0.0f64; usize::try_from(self.dims).unwrap_or(0)])
    }
}

/// How a column's text becomes an attribute value.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Kind {
    Bool,
    Int,
    Uint,
    Float,
    String,
}

impl Kind {
    /// The kind of a column with the type `type_oid`. Every type without a
    /// matching turbopuffer type keeps its text.
    fn of(type_oid: u32) -> Kind {
        match type_oid {
            16 => Kind::Bool,
            20 | 21 | 23 => Kind::Int,
            16_460 | 16_462 | 16_464 => Kind::Uint,
            700 | 701 => Kind::Float,
            _ => Kind::String,
        }
    }

    fn name(self) -> &'static str {
        match self {
            Kind::Bool => "bool",
            Kind::Int => "int",
            Kind::Uint => "uint",
            Kind::Float => "float",
            Kind::String => "string",
        }
    }

    fn value(self, text: &str) -> Option<Value> {
        match self {
            Kind::Bool => match text {
                "t" => Some(Value::Bool(true)),
                "f" => Some(Value::Bool(false)),
                _ => None,
            },
            Kind::Int => text.parse::<i64>().ok().map(Value::from),
            Kind::Uint => text.parse::<u64>().ok().map(Value::from),
            // JSON has no NaN or infinity.
            Kind::Float => text
                .parse::<f64>()
                .ok()
                .filter(|value| value.is_finite())
                .map(Value::from),
            Kind::String => Some(Value::String(text.to_string())),
        }
    }
}

#[derive(Clone, Debug)]
struct Attribute {
    name: String,
    kind: Kind,
    filterable: bool,
    /// Whether the cell is in the key, and its position there or in the
    /// value.
    in_key: bool,
    position: usize,
}

/// How a subscription's cells become documents.
#[derive(Clone, Debug)]
pub(crate) struct Layout {
    columns: PayloadColumns,
    attributes: Vec<Attribute>,
    embeddings: Vec<Embedding>,
    schema: Value,
}

impl Layout {
    /// The layout for `columns`. Fails if a column cannot be an attribute, or
    /// if `indexing` names a column that is not one.
    pub(crate) fn new(columns: &PayloadColumns, indexing: &Indexing) -> Result<Layout, SinkError> {
        let refuse = |message: String| SinkError::Target {
            message,
            permanent: true,
        };
        if columns.key.is_empty() {
            return Err(refuse(
                "the turbopuffer sink reads the upsert envelope, keyed by the document id".into(),
            ));
        }
        // A single key column named `id` is the document id itself.
        let id_column = match columns.key.as_slice() {
            [only] if only.name == "id" => true,
            _ => false,
        };
        let cells = (columns.key.iter().enumerate().map(|(i, c)| (true, i, c)))
            .chain(columns.value.iter().enumerate().map(|(i, c)| (false, i, c)));
        let mut attributes = Vec::new();
        let mut schema = Map::new();
        schema.insert("mz_timestamp".into(), json!({"type": "uint"}));
        schema.insert("deleted".into(), json!({"type": "bool"}));
        for (in_key, position, column) in cells {
            if id_column && in_key {
                continue;
            }
            check_name(column).map_err(refuse)?;
            let kind = Kind::of(column.type_oid);
            let full_text = indexing.full_text_search.contains(&column.name);
            if full_text && kind != Kind::String {
                return Err(refuse(format!(
                    "the column {:?} is not text, so it cannot be searched",
                    column.name
                )));
            }
            // A long text in a filterable attribute fails the whole request,
            // so text is filterable only where the sink was told.
            let filterable = kind != Kind::String || indexing.filterable.contains(&column.name);
            let mut entry = Map::new();
            entry.insert("type".into(), json!(kind.name()));
            entry.insert("filterable".into(), json!(filterable));
            if full_text {
                entry.insert("full_text_search".into(), json!(true));
            }
            let embedding = indexing.embeddings.iter().find(|e| e.column == column.name);
            if let Some(embedding) = embedding {
                if !(1..=MAX_DIMS).contains(&embedding.dims) {
                    return Err(refuse(format!(
                        "an embedding has between 1 and {MAX_DIMS} dimensions, not {}",
                        embedding.dims
                    )));
                }
                if kind != Kind::String {
                    return Err(refuse(format!(
                        "the column {:?} is not text, so it cannot be embedded",
                        column.name
                    )));
                }
                entry.insert(
                    "embed".into(),
                    json!({"model": embedding.model, "dims": embedding.dims}),
                );
                // A condition can compare only filterable attributes.
                schema.insert(
                    embedding.hash(),
                    json!({"type": "string", "filterable": true}),
                );
            }
            schema.insert(column.name.clone(), Value::Object(entry));
            attributes.push(Attribute {
                name: column.name.clone(),
                kind,
                filterable,
                in_key,
                position,
            });
        }
        // The schema leaves out `id` and the vectors.
        if schema.len().saturating_add(indexing.embeddings.len()) >= MAX_ATTRIBUTES {
            return Err(refuse(format!(
                "the view has {} columns, more than a turbopuffer namespace holds",
                attributes.len()
            )));
        }
        let embedded: Vec<&String> = indexing.embeddings.iter().map(|e| &e.column).collect();
        if embedded.len() > MAX_EMBEDDINGS {
            return Err(refuse(format!(
                "a turbopuffer namespace embeds at most {MAX_EMBEDDINGS} columns"
            )));
        }
        if embedded.iter().collect::<BTreeSet<_>>().len() != embedded.len() {
            return Err(refuse("a column is embedded twice".into()));
        }
        let derived = indexing
            .embeddings
            .iter()
            .flat_map(|e| [e.vector(), e.hash()]);
        let all_names: BTreeSet<&String> = attributes.iter().map(|a| &a.name).collect();
        if let Some(column) = embedded.iter().find(|column| !all_names.contains(*column)) {
            return Err(refuse(format!(
                "the view has no column {column:?} to embed, other than a key named id"
            )));
        }
        let derived: Vec<String> = derived.collect();
        if let Some(long) = derived.iter().find(|name| name.len() > MAX_NAME_BYTES) {
            return Err(refuse(format!(
                "the attribute {long:?} the sink adds for embedding is over {MAX_NAME_BYTES} \
                 bytes, so shorten the column's name in the view"
            )));
        }
        if let Some(clash) = derived.iter().find(|name| all_names.contains(name)) {
            return Err(refuse(format!(
                "the column {clash:?} clashes with an attribute the sink adds for embedding, \
                 so rename it in the view"
            )));
        }
        let named = indexing.filterable.iter().chain(&indexing.full_text_search);
        let all: BTreeSet<&String> = columns
            .key
            .iter()
            .chain(&columns.value)
            .map(|c| &c.name)
            .collect();
        if let Some(missing) = named.into_iter().find(|name| !all.contains(name)) {
            return Err(refuse(format!("the view has no column {missing:?}")));
        }
        Ok(Layout {
            columns: columns.clone(),
            attributes,
            embeddings: indexing.embeddings.clone(),
            schema: Value::Object(schema),
        })
    }

    /// Whether the layout was made for `columns`.
    pub(crate) fn fits(&self, columns: &PayloadColumns) -> bool {
        &self.columns == columns
    }

    pub(crate) fn schema(&self) -> &Value {
        &self.schema
    }

    pub(crate) fn embeddings(&self) -> &[Embedding] {
        &self.embeddings
    }

    /// The model and dimensions of each embedded column, as
    /// [`crate::client::Client::embeddings`] reads them back.
    pub(crate) fn embedded(&self) -> BTreeMap<String, (String, u32)> {
        self.embeddings
            .iter()
            .map(|e| (e.column.clone(), (e.model.clone(), e.dims)))
            .collect()
    }

    /// The condition on a patch that keeps the stored vectors: the stored
    /// document is not newer, is not a tombstone, and has the same hash of
    /// every embedded text, so its vectors match the new texts.
    pub(crate) fn unchanged_text_condition(&self) -> Value {
        // An equal timestamp is the same change, so a patch sent again after
        // a lost response applies the same values again instead of falling
        // through to an upsert that bills embedding.
        let mut conditions = vec![
            json!(["mz_timestamp", "Lte", {"$ref_new": "mz_timestamp"}]),
            json!(["deleted", "Eq", false]),
        ];
        for embedding in &self.embeddings {
            let hash = embedding.hash();
            conditions.push(json!([hash, "Eq", {"$ref_new": hash}]));
        }
        json!(["And", conditions])
    }
}

fn check_name(column: &Column) -> Result<(), String> {
    let name = &column.name;
    if RESERVED.contains(&name.as_str()) || name.starts_with('$') || name.len() > MAX_NAME_BYTES {
        return Err(format!(
            "the column {name:?} cannot be a turbopuffer attribute, so rename it in the view"
        ));
    }
    Ok(())
}

/// A document to write.
#[derive(Clone, Debug)]
pub(crate) struct Document {
    pub(crate) row: Value,
    pub(crate) bytes: usize,
    /// An estimate of the tokens turbopuffer bills to embed its texts.
    pub(crate) tokens: u64,
}

impl Document {
    pub(crate) fn id(&self) -> Option<&str> {
        self.row.get("id").and_then(Value::as_str)
    }

    pub(crate) fn is_tombstone(&self) -> bool {
        self.row.get("deleted") == Some(&Value::Bool(true))
    }

    /// The patch that changes every attribute but the embedded texts and
    /// their vectors, which [`Layout::unchanged_text_condition`] keeps. A
    /// patch leaves an attribute it omits as it was, so a `NULL` cell is an
    /// explicit null.
    pub(crate) fn patch(&self, layout: &Layout) -> Value {
        let mut row = self.row.clone();
        if let Some(row) = row.as_object_mut() {
            for attribute in &layout.attributes {
                row.entry(attribute.name.clone()).or_insert(Value::Null);
            }
            for embedding in layout.embeddings() {
                row.remove(&embedding.column);
                row.remove(&embedding.vector());
            }
        }
        row
    }
}

/// The documents of `batch`: the latest change of each key that can become a
/// document or a tombstone, and the updates that cannot.
///
/// Netting is safe because the conditions only let a newer timestamp win, and
/// necessary because turbopuffer rejects a request that names an id twice. A
/// rejected change counts as never made, so the key's previous change in the
/// batch takes its place, and the result does not depend on where a delivery
/// splits the changes into batches.
pub(crate) fn documents(batch: &SinkBatch, layout: &Layout) -> (Vec<Document>, Vec<Rejected>) {
    let mut by_id: BTreeMap<String, Vec<usize>> = BTreeMap::new();
    let mut rejected = Vec::new();
    for (index, update) in batch.updates().iter().enumerate() {
        let key = match &update.change {
            Change::Upsert { key, .. } | Change::Delete { key } => key,
            Change::Diff { .. } | Change::KeyViolation { .. } => {
                rejected.push(reject(index, "not_an_upsert", "the change has no key"));
                continue;
            }
        };
        match document_id(key) {
            Ok(id) => by_id.entry(id).or_default().push(index),
            Err(category) => rejected.push(reject(index, category, "the key cannot be an id")),
        }
    }
    let mut documents = Vec::with_capacity(by_id.len());
    for (id, indexes) in by_id {
        for &index in indexes.iter().rev() {
            let Some(update) = batch.updates().get(index) else {
                continue;
            };
            match document(&id, update, layout) {
                Ok(document) => {
                    documents.push(document);
                    break;
                }
                Err((category, message)) => rejected.push(reject(index, category, &message)),
            }
        }
    }
    rejected.sort_by_key(|rejected| rejected.index);
    (documents, rejected)
}

fn document(
    id: &str,
    update: &Update,
    layout: &Layout,
) -> Result<Document, (&'static str, String)> {
    let mut row = Map::new();
    row.insert("id".into(), Value::String(id.to_string()));
    row.insert("mz_timestamp".into(), Value::from(update.timestamp));
    let mut tokens = 0u64;
    match &update.change {
        Change::Upsert { key, value } => {
            row.insert("deleted".into(), Value::Bool(false));
            attributes(&mut row, layout, key, value)?;
            for embedding in layout.embeddings() {
                let text = row
                    .get(&embedding.column)
                    .and_then(Value::as_str)
                    .unwrap_or("");
                let hash = text_hash(text);
                if text.is_empty() {
                    row.remove(&embedding.column);
                    row.insert(embedding.vector(), embedding.placeholder());
                } else {
                    tokens = tokens.saturating_add(estimate_tokens(text.len()));
                }
                row.insert(embedding.hash(), Value::String(hash));
            }
        }
        _ => {
            row.insert("deleted".into(), Value::Bool(true));
            for embedding in layout.embeddings() {
                row.insert(embedding.vector(), embedding.placeholder());
            }
        }
    }
    let row = Value::Object(row);
    let bytes = row.to_string().len();
    if bytes > MAX_DOCUMENT_BYTES {
        return Err(("too_large", "the document is over 64 MiB".into()));
    }
    Ok(Document { row, bytes, tokens })
}

/// The hash of a text to embed, empty for no text, so that two documents
/// without text keep the same placeholder.
fn text_hash(text: &str) -> String {
    if text.is_empty() {
        return String::new();
    }
    Sha256::digest(text.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// The id of the document for `key`: the text of a single key column, or the
/// cells of several as a JSON array.
fn document_id(key: &[Option<String>]) -> Result<String, &'static str> {
    let id = match key {
        [Some(only)] => only.clone(),
        [None] => return Err("null_key"),
        cells => Value::from(cells.to_vec()).to_string(),
    };
    if id.len() > MAX_ID_BYTES {
        return Err("key_too_large");
    }
    Ok(id)
}

fn attributes(
    row: &mut Map<String, Value>,
    layout: &Layout,
    key: &[Option<String>],
    value: &[Option<String>],
) -> Result<(), (&'static str, String)> {
    for attribute in &layout.attributes {
        let cells = if attribute.in_key { key } else { value };
        let Some(Some(text)) = cells.get(attribute.position) else {
            continue;
        };
        let limit = if attribute.filterable {
            MAX_FILTERABLE_BYTES
        } else {
            MAX_VALUE_BYTES
        };
        if text.len() > limit {
            return Err((
                "value_too_large",
                format!("{} is over {limit} bytes", attribute.name),
            ));
        }
        let converted = attribute.kind.value(text).ok_or_else(|| {
            (
                "invalid_value",
                format!(
                    "{} is not a valid {}",
                    attribute.name,
                    attribute.kind.name()
                ),
            )
        })?;
        row.insert(attribute.name.clone(), converted);
    }
    Ok(())
}

fn reject(index: usize, category: &str, message: &str) -> Rejected {
    Rejected {
        index,
        category: category.into(),
        message: message.into(),
    }
}

/// The most one request holds.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Limits {
    pub(crate) bytes: usize,
    pub(crate) rows: usize,
    /// The most rows with text for turbopuffer to embed.
    pub(crate) embedded_rows: usize,
    /// The most tokens to embed, so that a request fits in a minute's rate
    /// limit, which turbopuffer would otherwise refuse every time.
    pub(crate) tokens: u64,
}

/// Splits `documents` into requests within `limits`, in order. A document
/// over a limit goes in a request of its own.
pub(crate) fn requests(documents: Vec<Document>, limits: Limits) -> Vec<Vec<Document>> {
    let mut requests = Vec::new();
    let mut current: Vec<Document> = Vec::new();
    let (mut bytes, mut embedded, mut tokens) = (0usize, 0usize, 0u64);
    for document in documents {
        let embeds = usize::from(document.tokens > 0);
        if !current.is_empty()
            && (bytes.saturating_add(document.bytes) > limits.bytes
                || current.len() >= limits.rows
                || embedded.saturating_add(embeds) > limits.embedded_rows
                || tokens.saturating_add(document.tokens) > limits.tokens)
        {
            requests.push(std::mem::take(&mut current));
            (bytes, embedded, tokens) = (0, 0, 0);
        }
        bytes = bytes.saturating_add(document.bytes);
        embedded = embedded.saturating_add(embeds);
        tokens = tokens.saturating_add(document.tokens);
        current.push(document);
    }
    if !current.is_empty() {
        requests.push(current);
    }
    requests
}

#[cfg(test)]
mod tests {
    use super::*;

    fn column(name: &str, type_oid: u32) -> Column {
        Column {
            name: name.into(),
            type_oid,
            type_mod: -1,
            nullable: true,
        }
    }

    fn columns() -> PayloadColumns {
        PayloadColumns {
            key: vec![column("id", 23)],
            value: vec![
                column("title", 25),
                column("views", 20),
                column("score", 701),
                column("public", 16),
            ],
        }
    }

    fn cells(values: &[Option<&str>]) -> Vec<Option<String>> {
        values.iter().map(|v| v.map(str::to_string)).collect()
    }

    fn upsert(timestamp: u64, id: &str, value: &[Option<&str>]) -> Update {
        Update {
            timestamp,
            change: Change::Upsert {
                key: cells(&[Some(id)]),
                value: cells(value),
            },
        }
    }

    fn delete(timestamp: u64, id: &str) -> Update {
        Update {
            timestamp,
            change: Change::Delete {
                key: cells(&[Some(id)]),
            },
        }
    }

    fn batch(updates: Vec<Update>) -> SinkBatch {
        SinkBatch::new(updates, columns(), 2, false)
    }

    #[test]
    fn each_key_keeps_its_latest_change_as_a_typed_document_or_a_tombstone() {
        let layout = Layout::new(&columns(), &Indexing::default()).unwrap();
        let batch = batch(vec![
            upsert(1, "1", &[Some("a"), Some("10"), Some("0.5"), Some("t")]),
            upsert(1, "2", &[Some("b"), None, None, Some("f")]),
            upsert(2, "1", &[Some("A"), Some("11"), Some("1e3"), Some("f")]),
            delete(3, "2"),
        ]);
        let (documents, rejected) = documents(&batch, &layout);
        assert!(rejected.is_empty(), "{rejected:?}");
        let rows: Vec<_> = documents.iter().map(|d| d.row.clone()).collect();
        assert_eq!(
            rows,
            [
                json!({"id": "1", "mz_timestamp": 2, "deleted": false, "title": "A",
                       "views": 11, "score": 1000.0, "public": false}),
                json!({"id": "2", "mz_timestamp": 3, "deleted": true}),
            ]
        );
        assert_eq!(
            layout.schema()["title"],
            json!({"type": "string", "filterable": false})
        );
        assert_eq!(
            layout.schema()["views"],
            json!({"type": "int", "filterable": true})
        );
        assert!(layout.schema().get("id").is_none());
    }

    #[test]
    fn updates_that_cannot_be_documents_are_rejected_alone() {
        let layout = Layout::new(&columns(), &Indexing::default()).unwrap();
        let long_key = "k".repeat(65);
        let batch = batch(vec![
            upsert(1, &long_key, &[None, None, None, None]),
            upsert(1, "nan", &[None, None, Some("NaN"), None]),
            Update {
                timestamp: 1,
                change: Change::Upsert {
                    key: vec![None],
                    value: cells(&[None, None, None, None]),
                },
            },
            upsert(1, "ok", &[Some("fine"), None, None, None]),
        ]);
        let (documents, rejected) = documents(&batch, &layout);
        let categories: Vec<_> = rejected
            .iter()
            .map(|r| (r.index, r.category.as_str()))
            .collect();
        assert_eq!(
            categories,
            [(0, "key_too_large"), (1, "invalid_value"), (2, "null_key")]
        );
        assert_eq!(documents.len(), 1);
        assert_eq!(documents[0].row["id"], "ok");
    }

    #[test]
    fn a_rejected_change_gives_way_to_the_previous_change_of_its_key() {
        let layout = Layout::new(&columns(), &Indexing::default()).unwrap();
        let invalid = upsert(6, "1", &[None, Some("not a number"), None, None]);
        let combined = batch(vec![delete(5, "1"), invalid.clone()]);
        let (written, rejected) = documents(&combined, &layout);
        let rows: Vec<_> = written.iter().map(|d| d.row.clone()).collect();
        assert_eq!(
            rows,
            [json!({"id": "1", "mz_timestamp": 5, "deleted": true})]
        );
        assert_eq!(rejected.iter().map(|r| r.index).collect::<Vec<_>>(), [1]);

        // Both changes rejected leaves nothing to write.
        let (written, rejected) = documents(&batch(vec![invalid.clone(), invalid]), &layout);
        assert!(written.is_empty());
        assert_eq!(rejected.iter().map(|r| r.index).collect::<Vec<_>>(), [0, 1]);
    }

    #[test]
    fn filterable_text_over_4_kib_is_rejected() {
        let indexing = Indexing {
            filterable: ["title".to_string()].into(),
            ..Indexing::default()
        };
        let layout = Layout::new(&columns(), &indexing).unwrap();
        let long = "x".repeat(MAX_FILTERABLE_BYTES + 1);
        let (written, rejected) = documents(
            &batch(vec![upsert(1, "1", &[Some(&long), None, None, None])]),
            &layout,
        );
        assert!(written.is_empty());
        assert_eq!(rejected[0].category, "value_too_large");

        let layout = Layout::new(&columns(), &Indexing::default()).unwrap();
        let (written, _) = documents(
            &batch(vec![upsert(1, "1", &[Some(&long), None, None, None])]),
            &layout,
        );
        assert_eq!(written.len(), 1);
    }

    #[test]
    fn composite_keys_become_json_ids_and_stay_attributes() {
        let columns = PayloadColumns {
            key: vec![column("region", 25), column("id", 23)],
            value: vec![column("title", 25)],
        };
        let refused = Layout::new(&columns, &Indexing::default());
        assert!(
            refused.is_err(),
            "a key column named id would shadow the id"
        );

        let columns = PayloadColumns {
            key: vec![column("region", 25), column("number", 23)],
            value: vec![column("title", 25)],
        };
        let layout = Layout::new(&columns, &Indexing::default()).unwrap();
        let update = Update {
            timestamp: 4,
            change: Change::Upsert {
                key: cells(&[Some("eu"), Some("7")]),
                value: cells(&[Some("t")]),
            },
        };
        let batch = SinkBatch::new(vec![update], columns, 1, false);
        let (documents, _) = documents(&batch, &layout);
        assert_eq!(
            documents[0].row,
            json!({"id": "[\"eu\",\"7\"]", "mz_timestamp": 4, "deleted": false,
                   "region": "eu", "number": 7, "title": "t"})
        );
    }

    #[test]
    fn layouts_refuse_what_turbopuffer_would() {
        let with_value = |name: &str| PayloadColumns {
            key: vec![column("id", 23)],
            value: vec![column(name, 25)],
        };
        for name in ["mz_timestamp", "deleted", "vector", "$score", "id"] {
            assert!(
                Layout::new(&with_value(name), &Indexing::default()).is_err(),
                "{name}"
            );
        }
        let diff = PayloadColumns {
            key: Vec::new(),
            value: vec![column("id", 23)],
        };
        assert!(Layout::new(&diff, &Indexing::default()).is_err());
        let searching_a_number = Indexing {
            full_text_search: ["views".to_string()].into(),
            ..Indexing::default()
        };
        assert!(Layout::new(&columns(), &searching_a_number).is_err());
        let a_typo = Indexing {
            filterable: ["titel".to_string()].into(),
            ..Indexing::default()
        };
        assert!(Layout::new(&columns(), &a_typo).is_err());
    }

    #[test]
    fn requests_split_by_bytes_rows_and_embedded_rows() {
        let document = |bytes, tokens| Document {
            row: json!(bytes),
            bytes,
            tokens,
        };
        let sizes = |requests: Vec<Vec<Document>>| -> Vec<Vec<usize>> {
            requests
                .into_iter()
                .map(|r| r.iter().map(|d| d.bytes).collect())
                .collect()
        };
        let limits = |bytes, rows, embedded_rows| Limits {
            bytes,
            rows,
            embedded_rows,
            tokens: 100,
        };
        let split = requests(
            vec![
                document(4, 0),
                document(4, 0),
                document(30, 0),
                document(1, 0),
            ],
            limits(10, 100, 100),
        );
        assert_eq!(sizes(split), [vec![4, 4], vec![30], vec![1]]);
        let split = requests(
            (0..5).map(|_| document(1, 0)).collect(),
            limits(100, 2, 100),
        );
        assert_eq!(sizes(split), [vec![1, 1], vec![1, 1], vec![1]]);
        // Only rows with text to embed count against the embedding limit.
        let split = requests(
            vec![
                document(1, 1),
                document(2, 0),
                document(3, 1),
                document(4, 1),
            ],
            limits(100, 100, 2),
        );
        assert_eq!(sizes(split), [vec![1, 2, 3], vec![4]]);
        let split = requests(
            vec![
                document(1, 60),
                document(2, 40),
                document(3, 1),
                document(4, 500),
            ],
            limits(100, 100, 100),
        );
        assert_eq!(sizes(split), [vec![1, 2], vec![3], vec![4]]);
        assert!(requests(Vec::new(), limits(10, 10, 10)).is_empty());
    }

    fn embedding(column: &str) -> Embedding {
        Embedding {
            column: column.into(),
            model: "m".into(),
            dims: 3,
        }
    }

    fn embedding_layout() -> Layout {
        let indexing = Indexing {
            embeddings: vec![embedding("title")],
            ..Indexing::default()
        };
        Layout::new(&columns(), &indexing).unwrap()
    }

    #[test]
    fn embedded_text_carries_its_hash_and_missing_text_a_placeholder() {
        let layout = embedding_layout();
        assert_eq!(
            layout.schema()["title"],
            json!({"type": "string", "filterable": false, "embed": {"model": "m", "dims": 3}})
        );
        assert_eq!(
            layout.schema()["mz_hash_title"],
            json!({"type": "string", "filterable": true})
        );
        let batch = batch(vec![
            upsert(1, "1", &[Some("a cat"), None, None, None]),
            upsert(1, "2", &[None, None, None, None]),
            upsert(1, "3", &[Some(""), None, None, None]),
            delete(1, "4"),
        ]);
        let (written, rejected) = documents(&batch, &layout);
        assert!(rejected.is_empty(), "{rejected:?}");
        let rows: Vec<_> = written.iter().map(|d| d.row.clone()).collect();
        let hash = text_hash("a cat");
        assert_eq!(hash.len(), 64);
        assert_eq!(
            rows,
            [
                json!({"id": "1", "mz_timestamp": 1, "deleted": false, "title": "a cat",
                       "mz_hash_title": hash}),
                json!({"id": "2", "mz_timestamp": 1, "deleted": false,
                       "embed_title": [0.0, 0.0, 0.0], "mz_hash_title": ""}),
                json!({"id": "3", "mz_timestamp": 1, "deleted": false,
                       "embed_title": [0.0, 0.0, 0.0], "mz_hash_title": ""}),
                json!({"id": "4", "mz_timestamp": 1, "deleted": true,
                       "embed_title": [0.0, 0.0, 0.0]}),
            ]
        );
        let tokens: Vec<_> = written.iter().map(|d| d.tokens).collect();
        assert_eq!(tokens, [estimate_tokens(5), 0, 0, 0]);
        // A patch keeps the stored text and vector, so it leaves both out,
        // and clears what is NULL now.
        assert_eq!(
            written[0].patch(&layout),
            json!({"id": "1", "mz_timestamp": 1, "deleted": false, "mz_hash_title": hash,
                   "views": null, "score": null, "public": null})
        );
        assert_eq!(
            layout.unchanged_text_condition(),
            json!(["And", [
                ["mz_timestamp", "Lte", {"$ref_new": "mz_timestamp"}],
                ["deleted", "Eq", false],
                ["mz_hash_title", "Eq", {"$ref_new": "mz_hash_title"}],
            ]])
        );
    }

    #[test]
    fn each_embedded_text_is_estimated_on_its_own() {
        let columns = PayloadColumns {
            key: vec![column("id", 23)],
            value: vec![column("title", 25), column("body", 25)],
        };
        let indexing = Indexing {
            embeddings: vec![embedding("title"), embedding("body")],
            ..Indexing::default()
        };
        let layout = Layout::new(&columns, &indexing).unwrap();
        let long = "x".repeat(200_000);
        let update = Update {
            timestamp: 1,
            change: Change::Upsert {
                key: cells(&[Some("1")]),
                value: cells(&[Some(&long), Some(&long)]),
            },
        };
        let batch = SinkBatch::new(vec![update], columns, 1, false);
        let (written, _) = documents(&batch, &layout);
        assert_eq!(written[0].tokens, 2 * estimate_tokens(long.len()));
        assert_eq!(
            estimate_tokens(long.len()),
            crate::pace::MAX_TOKENS_PER_TEXT
        );
    }

    #[test]
    fn layouts_refuse_embeddings_turbopuffer_would() {
        let with = |embeddings: Vec<Embedding>| {
            let indexing = Indexing {
                embeddings,
                ..Indexing::default()
            };
            Layout::new(&columns(), &indexing)
        };
        assert!(with(vec![embedding("views")]).is_err(), "not text");
        assert!(with(vec![embedding("missing")]).is_err());
        assert!(
            with(vec![embedding("id")]).is_err(),
            "the id is no attribute"
        );
        assert!(with(vec![embedding("title"), embedding("title")]).is_err());
        for dims in [0, MAX_DIMS + 1] {
            let wide = Embedding {
                dims,
                ..embedding("title")
            };
            assert!(with(vec![wide]).is_err(), "{dims}");
        }
        let clash = PayloadColumns {
            key: vec![column("id", 23)],
            value: vec![column("title", 25), column("embed_title", 25)],
        };
        let indexing = Indexing {
            embeddings: vec![embedding("title")],
            ..Indexing::default()
        };
        assert!(Layout::new(&clash, &indexing).is_err());
        let many = PayloadColumns {
            key: vec![column("id", 23)],
            value: (0..5).map(|i| column(&format!("t{i}"), 25)).collect(),
        };
        let five = Indexing {
            embeddings: (0..5).map(|i| embedding(&format!("t{i}"))).collect(),
            ..Indexing::default()
        };
        assert!(Layout::new(&many, &five).is_err());
    }
}
