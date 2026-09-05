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

//! Turning raw `SUBSCRIBE` rows into typed changes and progress markers.
//!
//! Decoding is by column *name*, not position, because the column layout
//! differs between envelopes (the upsert envelope interleaves key, `mz_state`,
//! and value columns) and because it makes the decoder robust to the exact
//! ordering the server chooses.
//!
//! In this initial version a value is its text encoding, with SQL `NULL`
//! represented as `None`. Typed decoding (numbers, timestamps, arrays) is
//! future work; the text form is unambiguous and matches what the pgwire
//! simple-query protocol delivers.

use crate::error::SubscribeError;

/// A single column value: its text encoding, or `None` for SQL `NULL`.
pub type Datum = Option<String>;

/// A row of [`Datum`]s.
pub type Row = Vec<Datum>;

/// How the server shapes the update stream. Chosen when the subscription is
/// created and fixed for its lifetime.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Envelope {
    /// Raw differential updates: each change carries a signed `mz_diff`
    /// multiplicity.
    Diff,
    /// Server-side upsert compaction keyed by the given columns: each change is
    /// an insert/update, a delete, or a key violation.
    Upsert {
        /// The names of the key columns.
        key: Vec<String>,
    },
}

/// A decoded change to the subscribed relation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Change {
    /// A differential update. `diff` is the signed multiplicity: positive means
    /// `diff` copies were inserted, negative means `|diff|` were retracted. The
    /// magnitude is preserved rather than collapsed to +/-1.
    Diff {
        /// The affected row.
        row: Row,
        /// The signed multiplicity (never zero).
        diff: i64,
    },
    /// An upsert-envelope insert or update. `value` is the new full row for
    /// `key`.
    Upsert {
        /// The key columns.
        key: Row,
        /// The value columns.
        value: Row,
    },
    /// An upsert-envelope delete: `key` no longer has a value.
    Delete {
        /// The key columns.
        key: Row,
    },
    /// An upsert-envelope key violation: more than one live value was observed
    /// for `key`. Best-effort per the server, so treat it as a signal, not a
    /// guarantee.
    KeyViolation {
        /// The offending key columns.
        key: Row,
    },
}

/// A decoded message from the stream: either a data change at a timestamp, or a
/// progress marker advancing the frontier.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StreamMessage {
    /// A change occurring at `timestamp`.
    Data {
        /// The `mz_timestamp` of the change.
        timestamp: u64,
        /// The change itself.
        change: Change,
    },
    /// A progress marker: no more changes will arrive with a timestamp strictly
    /// less than `frontier`.
    Progress {
        /// The advanced frontier.
        frontier: u64,
    },
}

const COL_TIMESTAMP: &str = "mz_timestamp";
const COL_PROGRESSED: &str = "mz_progressed";
const COL_DIFF: &str = "mz_diff";
const COL_STATE: &str = "mz_state";

const STATE_UPSERT: &str = "upsert";
const STATE_DELETE: &str = "delete";
const STATE_KEY_VIOLATION: &str = "key_violation";

/// Decodes raw `SUBSCRIBE` rows into [`StreamMessage`]s.
///
/// Built once from the result's column names (which the SDK always requests
/// `WITH (PROGRESS)`), then reused for every row. The layout of the metadata
/// columns is resolved up front so per-row decoding is a handful of indexed
/// reads.
#[derive(Debug)]
pub struct Decoder {
    timestamp_idx: usize,
    progressed_idx: usize,
    kind: DecoderKind,
}

#[derive(Debug)]
enum DecoderKind {
    Diff {
        diff_idx: usize,
        /// Payload column indices, in relation order.
        payload_idxs: Vec<usize>,
    },
    Upsert {
        state_idx: usize,
        key_idxs: Vec<usize>,
        value_idxs: Vec<usize>,
    },
}

impl Decoder {
    /// Builds a decoder from the result `columns` and the subscription
    /// `envelope`. Fails if a required metadata column is missing, which would
    /// indicate the subscription was not created by this SDK.
    pub fn new(columns: &[String], envelope: &Envelope) -> Result<Self, SubscribeError> {
        let find = |name: &str| columns.iter().position(|c| c == name);
        let require = |name: &str| {
            find(name).ok_or_else(|| {
                SubscribeError::Protocol(format!("result is missing the {name} column"))
            })
        };

        let timestamp_idx = require(COL_TIMESTAMP)?;
        let progressed_idx = require(COL_PROGRESSED)?;

        let kind = match envelope {
            Envelope::Diff => {
                let diff_idx = require(COL_DIFF)?;
                let meta = [timestamp_idx, progressed_idx, diff_idx];
                let payload_idxs = (0..columns.len()).filter(|i| !meta.contains(i)).collect();
                DecoderKind::Diff {
                    diff_idx,
                    payload_idxs,
                }
            }
            Envelope::Upsert { key } => {
                let state_idx = require(COL_STATE)?;
                let mut key_idxs = Vec::with_capacity(key.len());
                for key_col in key {
                    key_idxs.push(find(key_col).ok_or_else(|| {
                        SubscribeError::Protocol(format!(
                            "upsert key column {key_col} is not in the result"
                        ))
                    })?);
                }
                let meta: Vec<usize> = [timestamp_idx, progressed_idx, state_idx]
                    .into_iter()
                    .chain(key_idxs.iter().copied())
                    .collect();
                let value_idxs = (0..columns.len()).filter(|i| !meta.contains(i)).collect();
                DecoderKind::Upsert {
                    state_idx,
                    key_idxs,
                    value_idxs,
                }
            }
        };

        Ok(Decoder {
            timestamp_idx,
            progressed_idx,
            kind,
        })
    }

    /// Decodes a single raw row.
    pub fn decode(&self, row: &[Datum]) -> Result<StreamMessage, SubscribeError> {
        let timestamp = self.parse_timestamp(row)?;

        if self.is_progress(row)? {
            return Ok(StreamMessage::Progress {
                frontier: timestamp,
            });
        }

        let change = match &self.kind {
            DecoderKind::Diff {
                diff_idx,
                payload_idxs,
            } => {
                let diff = self.parse_diff(row, *diff_idx)?;
                Change::Diff {
                    row: project(row, payload_idxs),
                    diff,
                }
            }
            DecoderKind::Upsert {
                state_idx,
                key_idxs,
                value_idxs,
            } => {
                let key = project(row, key_idxs);
                let state = cell(row, *state_idx)?;
                match state.as_deref() {
                    Some(STATE_UPSERT) => Change::Upsert {
                        key,
                        value: project(row, value_idxs),
                    },
                    Some(STATE_DELETE) => Change::Delete { key },
                    Some(STATE_KEY_VIOLATION) => Change::KeyViolation { key },
                    other => {
                        return Err(SubscribeError::Protocol(format!(
                            "unexpected mz_state value: {other:?}"
                        )));
                    }
                }
            }
        };

        Ok(StreamMessage::Data { timestamp, change })
    }

    fn is_progress(&self, row: &[Datum]) -> Result<bool, SubscribeError> {
        // The pgwire text encoding of a boolean is "t" / "f".
        match cell(row, self.progressed_idx)?.as_deref() {
            Some("t") => Ok(true),
            Some("f") | None => Ok(false),
            Some(other) => Err(SubscribeError::Protocol(format!(
                "unexpected mz_progressed value: {other:?}"
            ))),
        }
    }

    fn parse_timestamp(&self, row: &[Datum]) -> Result<u64, SubscribeError> {
        let raw = cell(row, self.timestamp_idx)?
            .as_deref()
            .ok_or_else(|| SubscribeError::Protocol("mz_timestamp was NULL".to_string()))?;
        raw.parse::<u64>().map_err(|e| {
            SubscribeError::Protocol(format!("mz_timestamp {raw:?} is not a u64: {e}"))
        })
    }

    fn parse_diff(&self, row: &[Datum], diff_idx: usize) -> Result<i64, SubscribeError> {
        let raw = cell(row, diff_idx)?
            .as_deref()
            .ok_or_else(|| SubscribeError::Protocol("mz_diff was NULL".to_string()))?;
        raw.parse::<i64>()
            .map_err(|e| SubscribeError::Protocol(format!("mz_diff {raw:?} is not an i64: {e}")))
    }
}

/// Reads a cell, erroring if the row is shorter than the layout expects.
fn cell(row: &[Datum], idx: usize) -> Result<&Datum, SubscribeError> {
    row.get(idx).ok_or_else(|| {
        SubscribeError::Protocol(format!(
            "row has {} columns but column {idx} was expected",
            row.len()
        ))
    })
}

/// Projects the given column indices out of a row, in index order.
fn project(row: &[Datum], idxs: &[usize]) -> Row {
    idxs.iter()
        .map(|&i| row.get(i).cloned().flatten())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cols(names: &[&str]) -> Vec<String> {
        names.iter().map(|s| s.to_string()).collect()
    }

    fn datum(s: &str) -> Datum {
        Some(s.to_string())
    }

    #[test]
    fn diff_decoder_reads_progress() {
        let columns = cols(&["mz_timestamp", "mz_progressed", "mz_diff", "id", "name"]);
        let decoder = Decoder::new(&columns, &Envelope::Diff).unwrap();
        // A progress row: mz_progressed = t, payload columns NULL.
        let row = vec![datum("100"), datum("t"), None, None, None];
        assert_eq!(
            decoder.decode(&row).unwrap(),
            StreamMessage::Progress { frontier: 100 }
        );
    }

    #[test]
    fn diff_decoder_preserves_multiplicity_and_sign() {
        let columns = cols(&["mz_timestamp", "mz_progressed", "mz_diff", "id"]);
        let decoder = Decoder::new(&columns, &Envelope::Diff).unwrap();

        let insert = vec![datum("7"), datum("f"), datum("3"), datum("42")];
        assert_eq!(
            decoder.decode(&insert).unwrap(),
            StreamMessage::Data {
                timestamp: 7,
                change: Change::Diff {
                    row: vec![datum("42")],
                    diff: 3,
                },
            }
        );

        let retract = vec![datum("7"), datum("f"), datum("-2"), datum("42")];
        assert_eq!(
            decoder.decode(&retract).unwrap(),
            StreamMessage::Data {
                timestamp: 7,
                change: Change::Diff {
                    row: vec![datum("42")],
                    diff: -2,
                },
            }
        );
    }

    #[test]
    fn upsert_decoder_handles_all_states() {
        // Layout mirrors the server's upsert envelope: key first, then
        // mz_state, mz_timestamp, value columns, mz_progressed.
        let columns = cols(&[
            "id",
            "mz_state",
            "mz_timestamp",
            "name",
            "email",
            "mz_progressed",
        ]);
        let envelope = Envelope::Upsert {
            key: vec!["id".to_string()],
        };
        let decoder = Decoder::new(&columns, &envelope).unwrap();

        let upsert = vec![
            datum("1"),
            datum("upsert"),
            datum("50"),
            datum("alice"),
            datum("a@x"),
            datum("f"),
        ];
        assert_eq!(
            decoder.decode(&upsert).unwrap(),
            StreamMessage::Data {
                timestamp: 50,
                change: Change::Upsert {
                    key: vec![datum("1")],
                    value: vec![datum("alice"), datum("a@x")],
                },
            }
        );

        let delete = vec![
            datum("1"),
            datum("delete"),
            datum("51"),
            None,
            None,
            datum("f"),
        ];
        assert_eq!(
            decoder.decode(&delete).unwrap(),
            StreamMessage::Data {
                timestamp: 51,
                change: Change::Delete {
                    key: vec![datum("1")],
                },
            }
        );

        let violation = vec![
            datum("1"),
            datum("key_violation"),
            datum("52"),
            None,
            None,
            datum("f"),
        ];
        assert_eq!(
            decoder.decode(&violation).unwrap(),
            StreamMessage::Data {
                timestamp: 52,
                change: Change::KeyViolation {
                    key: vec![datum("1")],
                },
            }
        );
    }

    #[test]
    fn upsert_decoder_reads_progress() {
        let columns = cols(&["id", "mz_state", "mz_timestamp", "name", "mz_progressed"]);
        let envelope = Envelope::Upsert {
            key: vec!["id".to_string()],
        };
        let decoder = Decoder::new(&columns, &envelope).unwrap();
        let row = vec![None, None, datum("200"), None, datum("t")];
        assert_eq!(
            decoder.decode(&row).unwrap(),
            StreamMessage::Progress { frontier: 200 }
        );
    }

    #[test]
    fn multi_column_key_is_projected_in_order() {
        let columns = cols(&[
            "region",
            "id",
            "mz_state",
            "mz_timestamp",
            "total",
            "mz_progressed",
        ]);
        let envelope = Envelope::Upsert {
            key: vec!["region".to_string(), "id".to_string()],
        };
        let decoder = Decoder::new(&columns, &envelope).unwrap();
        let row = vec![
            datum("us"),
            datum("9"),
            datum("upsert"),
            datum("5"),
            datum("100"),
            datum("f"),
        ];
        assert_eq!(
            decoder.decode(&row).unwrap(),
            StreamMessage::Data {
                timestamp: 5,
                change: Change::Upsert {
                    key: vec![datum("us"), datum("9")],
                    value: vec![datum("100")],
                },
            }
        );
    }

    #[test]
    fn missing_metadata_column_is_rejected() {
        let columns = cols(&["mz_timestamp", "mz_diff", "id"]);
        let err = Decoder::new(&columns, &Envelope::Diff).unwrap_err();
        assert!(matches!(err, SubscribeError::Protocol(_)), "{err:?}");
    }

    #[test]
    fn unknown_key_column_is_rejected() {
        let columns = cols(&["id", "mz_state", "mz_timestamp", "mz_progressed"]);
        let envelope = Envelope::Upsert {
            key: vec!["nonexistent".to_string()],
        };
        assert!(Decoder::new(&columns, &envelope).is_err());
    }
}
