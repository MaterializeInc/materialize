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

/// The attributes the sink writes on every document. A column with one of
/// these names, or one turbopuffer reserves, cannot be an attribute.
const RESERVED: [&str; 4] = ["id", "mz_timestamp", "deleted", "vector"];

/// The condition on every data upsert: only a newer change replaces the
/// stored document. A missing document is created without the condition.
pub(crate) fn newer_condition() -> Value {
    json!(["mz_timestamp", "Lt", {"$ref_new": "mz_timestamp"}])
}

/// Which columns readers filter or search on.
#[derive(Clone, Debug, Default)]
pub(crate) struct Indexing {
    pub(crate) filterable: BTreeSet<String>,
    pub(crate) full_text_search: BTreeSet<String>,
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
            schema.insert(column.name.clone(), Value::Object(entry));
            attributes.push(Attribute {
                name: column.name.clone(),
                kind,
                filterable,
                in_key,
                position,
            });
        }
        if schema.len() >= MAX_ATTRIBUTES {
            return Err(refuse(format!(
                "the view has {} columns, more than a turbopuffer namespace holds",
                attributes.len()
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
}

fn check_name(column: &Column) -> Result<(), String> {
    let name = &column.name;
    if RESERVED.contains(&name.as_str()) || name.starts_with('$') || name.len() > 128 {
        return Err(format!(
            "the column {name:?} cannot be a turbopuffer attribute, so rename it in the view"
        ));
    }
    Ok(())
}

/// A document to upsert.
#[derive(Clone, Debug)]
pub(crate) struct Document {
    pub(crate) row: Value,
    pub(crate) bytes: usize,
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
    match &update.change {
        Change::Upsert { key, value } => {
            row.insert("deleted".into(), Value::Bool(false));
            attributes(&mut row, layout, key, value)?;
        }
        _ => {
            row.insert("deleted".into(), Value::Bool(true));
        }
    }
    let row = Value::Object(row);
    let bytes = row.to_string().len();
    if bytes > MAX_DOCUMENT_BYTES {
        return Err(("too_large", "the document is over 64 MiB".into()));
    }
    Ok(Document { row, bytes })
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

/// Splits `documents` into requests of at most `max_bytes` and `max_rows`
/// each. A document larger than `max_bytes` goes in a request of its own.
pub(crate) fn requests(
    documents: Vec<Document>,
    max_bytes: usize,
    max_rows: usize,
) -> Vec<Vec<Value>> {
    let mut requests = Vec::new();
    let mut current = Vec::new();
    let mut bytes = 0usize;
    for document in documents {
        if !current.is_empty()
            && (bytes.saturating_add(document.bytes) > max_bytes || current.len() >= max_rows)
        {
            requests.push(std::mem::take(&mut current));
            bytes = 0;
        }
        bytes = bytes.saturating_add(document.bytes);
        current.push(document.row);
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
    fn requests_split_by_bytes_and_rows() {
        let document = |bytes| Document {
            row: json!(bytes),
            bytes,
        };
        let sizes = |requests: Vec<Vec<Value>>| -> Vec<Vec<u64>> {
            requests
                .into_iter()
                .map(|r| r.iter().map(|v| v.as_u64().unwrap()).collect())
                .collect()
        };
        let split = requests(
            vec![document(4), document(4), document(30), document(1)],
            10,
            100,
        );
        assert_eq!(sizes(split), [vec![4, 4], vec![30], vec![1]]);
        let split = requests((0..5).map(|_| document(1)).collect(), 100, 2);
        assert_eq!(sizes(split), [vec![1, 1], vec![1, 1], vec![1]]);
        assert!(requests(Vec::new(), 10, 10).is_empty());
    }
}
