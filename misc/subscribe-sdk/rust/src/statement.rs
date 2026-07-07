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

//! Building the `SUBSCRIBE` statement, including the initial-versus-resume
//! `SNAPSHOT` and `AS OF` handling that must line up with the resume token.

use crate::envelope::Envelope;
use crate::token::ResumeToken;

/// What a subscription reads from.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Relation {
    /// A catalog object (table, view, materialized view, source, or index).
    /// Passed through verbatim, so a qualified name must already be a valid
    /// SQL object reference (e.g. `my_db.public.orders`).
    Object(String),
    /// An arbitrary `SELECT`. Wrapped in parentheses in the statement.
    Query(String),
}

/// A subscription specification: what to read, how to shape it, and where to
/// stop. Converted to SQL by [`Subscribe::to_sql_initial`] and
/// [`Subscribe::to_sql_resume`].
///
/// `PROGRESS` is always requested. It is the primitive the batcher relies on to
/// know when a timestamp is closed, and requesting it unconditionally keeps the
/// column layout stable.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Subscribe {
    relation: Relation,
    envelope: Envelope,
    up_to: Option<u64>,
}

impl Subscribe {
    /// Subscribes to a catalog object with the default (diff) envelope.
    pub fn object(name: impl Into<String>) -> Self {
        Subscribe {
            relation: Relation::Object(name.into()),
            envelope: Envelope::Diff,
            up_to: None,
        }
    }

    /// Subscribes to the result of a `SELECT` with the default (diff) envelope.
    pub fn query(sql: impl Into<String>) -> Self {
        Subscribe {
            relation: Relation::Query(sql.into()),
            envelope: Envelope::Diff,
            up_to: None,
        }
    }

    /// Switches to the upsert envelope keyed by the given columns. The server
    /// collapses diffs into per-key inserts, deletes, and key violations.
    pub fn envelope_upsert<I, S>(mut self, key: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.envelope = Envelope::Upsert {
            key: key.into_iter().map(Into::into).collect(),
        };
        self
    }

    /// Bounds the subscription: it terminates once the frontier reaches
    /// `timestamp` (exclusive), yielding a deterministic window.
    pub fn up_to(mut self, timestamp: u64) -> Self {
        self.up_to = Some(timestamp);
        self
    }

    /// The envelope this subscription uses. Needed to build a matching
    /// [`crate::envelope::Decoder`].
    pub fn envelope(&self) -> &Envelope {
        &self.envelope
    }

    /// Whether the subscription is bounded by an `UP TO` and will therefore
    /// terminate on its own.
    pub fn is_bounded(&self) -> bool {
        self.up_to.is_some()
    }

    /// A stable identifier for this subscription's shape (relation plus
    /// envelope), embedded in resume tokens so a changed query is caught on
    /// resume rather than silently mixing incompatible results.
    ///
    /// Stability across processes and SDK versions matters, so this uses a
    /// fixed FNV-1a hash rather than [`std::hash`], whose output is not
    /// guaranteed stable.
    pub fn fingerprint(&self) -> String {
        let mut h = Fnv1a::new();
        match &self.relation {
            Relation::Object(name) => {
                h.update(b"object:");
                h.update(name.as_bytes());
            }
            Relation::Query(sql) => {
                h.update(b"query:");
                h.update(sql.as_bytes());
            }
        }
        match &self.envelope {
            Envelope::Diff => h.update(b"|diff"),
            Envelope::Upsert { key } => {
                h.update(b"|upsert:");
                for k in key {
                    h.update(k.as_bytes());
                    h.update(b",");
                }
            }
        }
        format!("{:016x}", h.finish())
    }

    /// The SQL for an initial subscription: takes the snapshot and lets the
    /// server choose the `AS OF`.
    pub fn to_sql_initial(&self) -> String {
        self.to_sql(true, None)
    }

    /// The SQL for resuming from `token`: no snapshot, and `AS OF` set to
    /// `token.as_of()` so emission begins exactly where the token left off.
    pub fn to_sql_resume(&self, token: &ResumeToken) -> String {
        self.to_sql(false, Some(token.as_of()))
    }

    fn to_sql(&self, snapshot: bool, as_of: Option<u64>) -> String {
        let relation = match &self.relation {
            Relation::Object(name) => name.clone(),
            Relation::Query(sql) => format!("({sql})"),
        };

        let mut sql = format!("SUBSCRIBE {relation}");

        if let Envelope::Upsert { key } = &self.envelope {
            let keys = key
                .iter()
                .map(|k| quote_ident(k))
                .collect::<Vec<_>>()
                .join(", ");
            sql.push_str(&format!(" ENVELOPE UPSERT (KEY ({keys}))"));
        }

        sql.push_str(&format!(" WITH (PROGRESS, SNAPSHOT = {snapshot})"));

        if let Some(as_of) = as_of {
            sql.push_str(&format!(" AS OF {as_of}"));
        }
        if let Some(up_to) = self.up_to {
            sql.push_str(&format!(" UP TO {up_to}"));
        }
        sql
    }
}

/// Double-quotes a SQL identifier, escaping embedded quotes, so a key column
/// name cannot break out of the `KEY (...)` clause.
fn quote_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

/// A small, stable FNV-1a 64-bit hash. Stable output is the point: fingerprints
/// are persisted in resume tokens and compared across processes.
struct Fnv1a(u64);

impl Fnv1a {
    fn new() -> Self {
        Fnv1a(0xcbf2_9ce4_8422_2325)
    }

    fn update(&mut self, bytes: &[u8]) {
        for &b in bytes {
            self.0 ^= u64::from(b);
            self.0 = self.0.wrapping_mul(0x0000_0100_0000_01b3);
        }
    }

    fn finish(&self) -> u64 {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn initial_takes_snapshot_and_omits_as_of() {
        let sql = Subscribe::object("orders").to_sql_initial();
        assert_eq!(sql, "SUBSCRIBE orders WITH (PROGRESS, SNAPSHOT = true)");
    }

    #[test]
    fn resume_disables_snapshot_and_sets_as_of_to_frontier_minus_one() {
        let sub = Subscribe::object("orders");
        let token = ResumeToken::new(100, sub.fingerprint());
        let sql = sub.to_sql_resume(&token);
        assert_eq!(
            sql,
            "SUBSCRIBE orders WITH (PROGRESS, SNAPSHOT = false) AS OF 99"
        );
    }

    #[test]
    fn query_relation_is_parenthesized() {
        let sql = Subscribe::query("SELECT id FROM orders WHERE paid").to_sql_initial();
        assert_eq!(
            sql,
            "SUBSCRIBE (SELECT id FROM orders WHERE paid) WITH (PROGRESS, SNAPSHOT = true)"
        );
    }

    #[test]
    fn upsert_envelope_precedes_with_clause() {
        let sql = Subscribe::object("orders")
            .envelope_upsert(["id"])
            .to_sql_initial();
        assert_eq!(
            sql,
            "SUBSCRIBE orders ENVELOPE UPSERT (KEY (\"id\")) WITH (PROGRESS, SNAPSHOT = true)"
        );
    }

    #[test]
    fn multi_column_key_and_up_to() {
        let sql = Subscribe::object("sales")
            .envelope_upsert(["region", "id"])
            .up_to(500)
            .to_sql_initial();
        assert_eq!(
            sql,
            "SUBSCRIBE sales ENVELOPE UPSERT (KEY (\"region\", \"id\")) \
             WITH (PROGRESS, SNAPSHOT = true) UP TO 500"
        );
    }

    #[test]
    fn key_identifiers_are_quote_escaped() {
        let sql = Subscribe::object("t")
            .envelope_upsert(["weird\"name"])
            .to_sql_initial();
        assert!(sql.contains("KEY (\"weird\"\"name\")"), "{sql}");
    }

    #[test]
    fn fingerprint_matches_cross_language_vectors() {
        // These exact values are also asserted by the Python SDK, so both
        // compute the same fingerprint (and therefore interchangeable tokens)
        // for the same subscription shape.
        assert_eq!(Subscribe::object("orders").fingerprint(), "f44d7c35ad6f569c");
        assert_eq!(
            Subscribe::object("orders")
                .envelope_upsert(["id"])
                .fingerprint(),
            "a9392428ac0ed8c9"
        );
    }

    #[test]
    fn fingerprint_is_stable_and_distinguishes_shape() {
        let a = Subscribe::object("orders");
        // Stable across constructions.
        assert_eq!(a.fingerprint(), Subscribe::object("orders").fingerprint());
        // Different relation, envelope, or key changes the fingerprint.
        assert_ne!(a.fingerprint(), Subscribe::object("other").fingerprint());
        assert_ne!(
            a.fingerprint(),
            Subscribe::object("orders")
                .envelope_upsert(["id"])
                .fingerprint()
        );
        assert_ne!(
            Subscribe::object("orders")
                .envelope_upsert(["id"])
                .fingerprint(),
            Subscribe::object("orders")
                .envelope_upsert(["id", "region"])
                .fingerprint()
        );
        // UP TO does not change the shape, so it does not change the fingerprint.
        assert_eq!(
            a.fingerprint(),
            Subscribe::object("orders").up_to(9).fingerprint()
        );
    }
}
