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

//! Decoding `SUBSCRIBE ... WITH (PROGRESS)` rows into updates and progress.
//!
//! Each cell is the column's text format exactly as the server sent it, with
//! SQL `NULL` as `None`. The simple query protocol delivers that. A driver that
//! parses values into native types and prints them again does not: it can turn
//! a boolean `t` into `True`.
//!
//! The output layout is fixed by `describe_subscribe` in
//! `src/sql/src/plan/statement/dml.rs`: `mz_timestamp`, `mz_progressed`, then
//! `mz_diff` (diff envelope) or `mz_state` (upsert envelope), then the payload.
//! In the upsert envelope the payload is the key columns in `KEY` order
//! followed by the remaining columns in relation order. The decoder reads by
//! position and checks the column names once, so a user column named like a
//! metadata column cannot be mistaken for one.

use std::collections::BTreeSet;

use serde::{Deserialize, Deserializer, Serialize};

use crate::error::ProtocolError;
use crate::wire;

/// One cell in the text format, or `None` for SQL `NULL`.
pub type Datum = Option<String>;

/// The cells of one row, in column order.
pub type Row = Vec<Datum>;

/// The output envelope of a subscription.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Envelope {
    /// Each update is a row and a signed multiplicity.
    Diff,
    /// `ENVELOPE UPSERT (KEY (...))`: each update is an upsert, a delete, or a
    /// key violation for one key at one timestamp.
    Upsert {
        /// The key column names. The decoder refuses an empty key or a
        /// repeated column, as the server does.
        key: Vec<String>,
    },
}

/// Refuses unknown fields and a `key` on the diff envelope, which derived
/// deserialization accepts and drops for a unit variant.
impl<'de> Deserialize<'de> for Envelope {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(rename_all = "snake_case")]
        enum Kind {
            Diff,
            Upsert,
        }

        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            kind: Kind,
            key: Option<Vec<String>>,
        }

        let Wire { kind, key } = Wire::deserialize(deserializer)?;
        match (kind, key) {
            (Kind::Diff, None) => Ok(Envelope::Diff),
            (Kind::Upsert, Some(key)) => Ok(Envelope::Upsert { key }),
            (Kind::Diff, Some(_)) => Err(serde::de::Error::custom("the diff envelope has no key")),
            (Kind::Upsert, None) => Err(serde::de::Error::missing_field("key")),
        }
    }
}

/// A change to the subscribed relation at one timestamp.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum Change {
    /// `diff` copies of `row` were added, or removed if `diff` is negative.
    /// The decoder never produces a zero `diff`.
    Diff {
        /// The row.
        row: Row,
        /// The signed multiplicity.
        #[serde(with = "wire::i64_string")]
        diff: i64,
    },
    /// `key` now has the value `value`.
    Upsert {
        /// The key columns.
        key: Row,
        /// The non-key columns, in relation order.
        value: Row,
    },
    /// `key` no longer has a value.
    Delete {
        /// The key columns.
        key: Row,
    },
    /// The changes to `key` at this timestamp were not an insert, update, or
    /// delete, so `key` is not unique in the subscribed relation.
    KeyViolation {
        /// The key columns.
        key: Row,
    },
}

/// A change and the `mz_timestamp` it happened at.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Update {
    /// The `mz_timestamp` of the change.
    #[serde(with = "wire::u64_string")]
    pub timestamp: u64,
    /// The change.
    #[serde(flatten)]
    pub change: Change,
}

/// A decoded row: an update, or a progress row.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "message", rename_all = "snake_case")]
pub enum Message {
    /// An update.
    Update(Update),
    /// No update at a time below `frontier` will follow.
    Progress {
        /// The new frontier.
        #[serde(with = "wire::u64_string")]
        frontier: u64,
    },
}

const MZ_TIMESTAMP: &str = "mz_timestamp";
const MZ_PROGRESSED: &str = "mz_progressed";
const MZ_DIFF: &str = "mz_diff";
const MZ_STATE: &str = "mz_state";

/// The number of metadata columns before the payload.
const META_COLUMNS: usize = 3;

/// Decodes the rows of one `SUBSCRIBE ... WITH (PROGRESS)` cursor.
#[derive(Clone, Debug)]
pub struct Decoder {
    /// The payload column names, in output order.
    columns: Vec<String>,
    /// `None` for the diff envelope, else the number of key columns.
    key_arity: Option<usize>,
}

impl Decoder {
    /// Returns a decoder for a cursor with the given output `columns`.
    ///
    /// Fails if the columns are not the layout of `SUBSCRIBE ... WITH
    /// (PROGRESS)` in `envelope`.
    pub fn new<S: AsRef<str>>(
        envelope: &Envelope,
        columns: &[S],
    ) -> Result<Decoder, ProtocolError> {
        let names: Vec<&str> = columns.iter().map(AsRef::as_ref).collect();
        let (meta_column, key_arity) = match envelope {
            Envelope::Diff => (MZ_DIFF, None),
            Envelope::Upsert { key } => (MZ_STATE, Some(key.len())),
        };
        let expected_meta = [MZ_TIMESTAMP, MZ_PROGRESSED, meta_column];
        if names.get(..META_COLUMNS) != Some(&expected_meta[..]) {
            return Err(ProtocolError::new(format!(
                "expected the columns to start with {expected_meta:?}, got {names:?}"
            )));
        }
        let payload = names.get(META_COLUMNS..).unwrap_or_default();
        if let Envelope::Upsert { key } = envelope {
            let key_columns = payload.get(..key.len());
            let distinct: BTreeSet<&String> = key.iter().collect();
            if key.is_empty()
                || distinct.len() != key.len()
                || key_columns.is_none_or(|columns| {
                    !columns.iter().copied().eq(key.iter().map(String::as_str))
                })
            {
                return Err(ProtocolError::new(format!(
                    "expected the distinct key columns {key:?} after {expected_meta:?}, got \
                     {names:?}"
                )));
            }
        }
        Ok(Decoder {
            columns: payload.iter().map(|name| name.to_string()).collect(),
            key_arity,
        })
    }

    /// The payload column names, in output order. In the upsert envelope the
    /// key columns come first, so they name the cells of [`Change::Upsert`]'s
    /// `key` followed by its `value`.
    pub fn columns(&self) -> &[String] {
        &self.columns
    }

    /// Decodes one row.
    pub fn decode(&self, row: Row) -> Result<Message, ProtocolError> {
        let width = META_COLUMNS.saturating_add(self.columns.len());
        if row.len() != width {
            return Err(ProtocolError::new(format!(
                "expected a row of {width} columns, got {}",
                row.len()
            )));
        }
        let mut cells = row.into_iter();

        let timestamp = cells.next().flatten().ok_or_else(|| null(MZ_TIMESTAMP))?;
        let timestamp =
            wire::parse_u64(&timestamp).ok_or_else(|| invalid(MZ_TIMESTAMP, &timestamp))?;
        let progressed = cells.next().flatten().ok_or_else(|| null(MZ_PROGRESSED))?;
        match progressed.as_str() {
            // The server fills every other cell of a progress row with NULL
            // (`ActiveSubscribe::send_progress_message`).
            "t" => {
                if cells.any(|cell| cell.is_some()) {
                    return Err(ProtocolError::new("a progress row has a non-NULL cell"));
                }
                return Ok(Message::Progress {
                    frontier: timestamp,
                });
            }
            "f" => {}
            _ => return Err(invalid(MZ_PROGRESSED, &progressed)),
        }

        let meta = cells.next().flatten();
        let mut payload = cells;
        let change = match self.key_arity {
            None => {
                let diff = meta.ok_or_else(|| null(MZ_DIFF))?;
                let diff = wire::parse_i64(&diff)
                    .filter(|d| *d != 0)
                    .ok_or_else(|| invalid(MZ_DIFF, &diff))?;
                Change::Diff {
                    row: payload.collect(),
                    diff,
                }
            }
            Some(key_arity) => {
                let state = meta.ok_or_else(|| null(MZ_STATE))?;
                let key: Row = payload.by_ref().take(key_arity).collect();
                match state.as_str() {
                    "upsert" => Change::Upsert {
                        key,
                        value: payload.collect(),
                    },
                    // The server sends a delete and a key violation with NULL
                    // values (`ActiveSubscribe::process_response`).
                    "delete" | "key_violation" if payload.any(|cell| cell.is_some()) => {
                        return Err(ProtocolError::new(format!("a {state} row has a value")));
                    }
                    "delete" => Change::Delete { key },
                    "key_violation" => Change::KeyViolation { key },
                    _ => return Err(invalid(MZ_STATE, &state)),
                }
            }
        };
        Ok(Message::Update(Update { timestamp, change }))
    }
}

fn null(column: &str) -> ProtocolError {
    ProtocolError::new(format!("{column} is NULL"))
}

fn invalid(column: &str, value: &str) -> ProtocolError {
    ProtocolError::new(format!("invalid {column} value {value:?}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn upsert_envelope(key: &[&str]) -> Envelope {
        Envelope::Upsert {
            key: key.iter().map(|k| k.to_string()).collect(),
        }
    }

    fn row(cells: &[Option<&str>]) -> Row {
        cells.iter().map(|c| c.map(str::to_string)).collect()
    }

    #[test]
    fn decodes_diff_updates_and_progress() {
        let decoder = Decoder::new(
            &Envelope::Diff,
            &["mz_timestamp", "mz_progressed", "mz_diff", "id", "name"],
        )
        .unwrap();
        assert_eq!(decoder.columns(), ["id", "name"]);
        assert_eq!(
            decoder
                .decode(row(&[Some("7"), Some("f"), Some("-2"), Some("1"), None]))
                .unwrap(),
            Message::Update(Update {
                timestamp: 7,
                change: Change::Diff {
                    row: row(&[Some("1"), None]),
                    diff: -2,
                },
            })
        );
        assert_eq!(
            decoder
                .decode(row(&[Some("8"), Some("t"), None, None, None]))
                .unwrap(),
            Message::Progress { frontier: 8 }
        );
    }

    #[test]
    fn decodes_every_upsert_state() {
        let decoder = Decoder::new(
            &upsert_envelope(&["region", "id"]),
            &[
                "mz_timestamp",
                "mz_progressed",
                "mz_state",
                "region",
                "id",
                "total",
            ],
        )
        .unwrap();
        let decode = |cells: &[Option<&str>]| match decoder.decode(row(cells)).unwrap() {
            Message::Update(update) => update.change,
            other @ Message::Progress { .. } => panic!("expected an update, got {other:?}"),
        };
        assert_eq!(
            decode(&[
                Some("5"),
                Some("f"),
                Some("upsert"),
                Some("eu"),
                Some("9"),
                Some("10")
            ]),
            Change::Upsert {
                key: row(&[Some("eu"), Some("9")]),
                value: row(&[Some("10")]),
            }
        );
        assert_eq!(
            decode(&[
                Some("5"),
                Some("f"),
                Some("delete"),
                Some("eu"),
                Some("9"),
                None
            ]),
            Change::Delete {
                key: row(&[Some("eu"), Some("9")]),
            }
        );
        assert_eq!(
            decode(&[
                Some("5"),
                Some("f"),
                Some("key_violation"),
                Some("eu"),
                Some("9"),
                None
            ]),
            Change::KeyViolation {
                key: row(&[Some("eu"), Some("9")]),
            }
        );
    }

    #[test]
    fn a_user_column_named_like_metadata_is_payload() {
        let decoder = Decoder::new(
            &Envelope::Diff,
            &["mz_timestamp", "mz_progressed", "mz_diff", "mz_timestamp"],
        )
        .unwrap();
        assert_eq!(
            decoder
                .decode(row(&[Some("3"), Some("f"), Some("1"), Some("x")]))
                .unwrap(),
            Message::Update(Update {
                timestamp: 3,
                change: Change::Diff {
                    row: row(&[Some("x")]),
                    diff: 1,
                },
            })
        );
    }

    #[test]
    fn rejects_columns_that_are_not_the_subscribe_layout() {
        let diff = &Envelope::Diff;
        assert!(Decoder::new(diff, &["mz_timestamp", "mz_diff", "id"]).is_err());
        assert!(Decoder::new(diff, &["mz_timestamp", "mz_progressed", "mz_state"]).is_err());
        let upsert = &upsert_envelope(&["id"]);
        assert!(Decoder::new(upsert, &["mz_timestamp", "mz_progressed", "mz_diff", "id"]).is_err());
        assert!(
            Decoder::new(
                upsert,
                &["mz_timestamp", "mz_progressed", "mz_state", "other"]
            )
            .is_err()
        );
        assert!(Decoder::new(upsert, &["mz_timestamp", "mz_progressed", "mz_state"]).is_err());
        let no_key = &upsert_envelope(&[]);
        assert!(
            Decoder::new(no_key, &["mz_timestamp", "mz_progressed", "mz_state", "id"]).is_err()
        );
    }

    #[test]
    fn rejects_malformed_rows() {
        let decoder = Decoder::new(
            &Envelope::Diff,
            &["mz_timestamp", "mz_progressed", "mz_diff", "id"],
        )
        .unwrap();
        for cells in [
            vec![Some("1"), Some("f"), Some("1")],
            vec![None, Some("f"), Some("1"), Some("a")],
            vec![Some("-1"), Some("f"), Some("1"), Some("a")],
            vec![Some("1"), None, Some("1"), Some("a")],
            vec![Some("1"), Some("true"), None, None],
            vec![Some("1"), Some("f"), None, Some("a")],
            vec![Some("1"), Some("f"), Some("0"), Some("a")],
            vec![Some("1"), Some("f"), Some("1.5"), Some("a")],
        ] {
            assert!(decoder.decode(row(&cells)).is_err(), "{cells:?}");
        }
    }

    #[test]
    fn rejects_rows_the_server_cannot_send() {
        let diff = Decoder::new(
            &Envelope::Diff,
            &["mz_timestamp", "mz_progressed", "mz_diff", "id"],
        )
        .unwrap();
        assert!(
            diff.decode(row(&[Some("1"), Some("t"), Some("1"), None]))
                .is_err(),
            "a progress row with a diff"
        );
        assert!(
            diff.decode(row(&[Some("1"), Some("t"), None, Some("a")]))
                .is_err(),
            "a progress row with a payload"
        );

        let upsert = Decoder::new(
            &upsert_envelope(&["id"]),
            &["mz_timestamp", "mz_progressed", "mz_state", "id", "v"],
        )
        .unwrap();
        for cells in [
            [Some("1"), Some("f"), Some("delete"), Some("k"), Some("v")],
            [
                Some("1"),
                Some("f"),
                Some("key_violation"),
                Some("k"),
                Some("v"),
            ],
            [Some("1"), Some("f"), None, Some("k"), Some("v")],
            [Some("1"), Some("f"), Some("insert"), Some("k"), Some("v")],
            [Some("1"), Some("t"), None, Some("k"), None],
        ] {
            assert!(upsert.decode(row(&cells)).is_err(), "{cells:?}");
        }
        assert!(
            upsert
                .decode(row(&[Some("1"), Some("t"), None, None]))
                .is_err()
        );
        assert_eq!(
            upsert
                .decode(row(&[Some("1"), Some("t"), None, None, None]))
                .unwrap(),
            Message::Progress { frontier: 1 }
        );
    }

    #[test]
    fn rejects_a_repeated_key_column() {
        let columns = ["mz_timestamp", "mz_progressed", "mz_state", "id", "id"];
        assert!(Decoder::new(&upsert_envelope(&["id", "id"]), &columns).is_err());
    }

    #[test]
    fn envelope_json_is_strict() {
        let parse = serde_json::from_str::<Envelope>;
        assert_eq!(parse(r#"{"kind":"diff"}"#).unwrap(), Envelope::Diff);
        assert_eq!(
            parse(r#"{"kind":"upsert","key":["id"]}"#).unwrap(),
            upsert_envelope(&["id"])
        );
        assert!(parse(r#"{"kind":"diff","key":["id"]}"#).is_err());
        assert!(parse(r#"{"kind":"upsert"}"#).is_err());
        assert!(parse(r#"{"kind":"diff","other":1}"#).is_err());
    }

    #[test]
    fn message_json_form() {
        let progress = Message::Progress { frontier: 9 };
        assert_eq!(
            serde_json::to_value(&progress).unwrap(),
            serde_json::json!({"message": "progress", "frontier": "9"})
        );
        let update = Message::Update(Update {
            timestamp: 5,
            change: Change::Delete {
                key: row(&[Some("k")]),
            },
        });
        let json = serde_json::to_value(&update).unwrap();
        assert_eq!(
            json,
            serde_json::json!({"message": "update", "timestamp": "5", "op": "delete", "key": ["k"]})
        );
        assert_eq!(serde_json::from_value::<Message>(json).unwrap(), update);
    }

    #[test]
    fn update_json_is_flat_with_string_integers() {
        let update = Update {
            timestamp: 5,
            change: Change::Diff {
                row: row(&[Some("a"), None]),
                diff: -1,
            },
        };
        let json = serde_json::to_value(&update).unwrap();
        assert_eq!(
            json,
            serde_json::json!({"timestamp": "5", "op": "diff", "row": ["a", null], "diff": "-1"})
        );
        assert_eq!(serde_json::from_value::<Update>(json).unwrap(), update);
    }
}
