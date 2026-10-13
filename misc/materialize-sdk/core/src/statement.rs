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

//! The SQL text of `SUBSCRIBE` and the cursor statements around it.

use std::collections::BTreeSet;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::batch::Start;
use crate::decode::Envelope;
use crate::error::UsageError;
use crate::wire;

/// A catalog object, with the catalog id its name resolved to.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObjectRef {
    /// The catalog id, such as `u123`.
    pub id: String,
    /// The database, or `None` for an object in an ambient schema such as
    /// `mz_catalog`.
    pub database: Option<String>,
    /// The schema.
    pub schema: String,
    /// The object's name.
    pub name: String,
}

/// What a subscription reads.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Target {
    /// A catalog object, read by catalog id.
    ///
    /// Reading by id keeps a rename or an `ALTER SCHEMA ... SWAP` between the
    /// catalog lookup and the `SUBSCRIBE` from changing what is read. A dropped
    /// id fails with "invalid id" (`resolve_item_name_id` in
    /// `src/sql/src/names.rs`).
    Object(ObjectRef),
    /// A query, for a live subscription that never resumes.
    ///
    /// The text is placed in the statement as written, so it must be one
    /// trusted query with no trailing `;`. A transport sends `DECLARE` with
    /// the extended protocol, where the server refuses more than one statement
    /// ("cannot insert multiple commands into a prepared statement" in
    /// `src/pgwire/src/protocol.rs`).
    Query {
        /// The query text.
        sql: String,
    },
}

/// A `SUBSCRIBE` statement, before its start is chosen.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Subscribe {
    target: Target,
    envelope: Envelope,
    #[serde(
        default,
        with = "wire::opt_u64_string",
        skip_serializing_if = "Option::is_none"
    )]
    up_to: Option<u64>,
}

impl Subscribe {
    /// Subscribes to `object` in the diff envelope.
    pub fn object(object: ObjectRef) -> Subscribe {
        Subscribe::new(Target::Object(object))
    }

    /// Subscribes to the result of `query` in the diff envelope. A query can
    /// only start with a snapshot.
    pub fn query(query: impl Into<String>) -> Subscribe {
        Subscribe::new(Target::Query { sql: query.into() })
    }

    fn new(target: Target) -> Subscribe {
        Subscribe {
            target,
            envelope: Envelope::Diff,
            up_to: None,
        }
    }

    /// Sets the output envelope.
    pub fn with_envelope(mut self, envelope: Envelope) -> Subscribe {
        self.envelope = envelope;
        self
    }

    /// Ends the subscription before `up_to`.
    pub fn with_up_to(mut self, up_to: u64) -> Subscribe {
        self.up_to = Some(up_to);
        self
    }

    /// What the subscription reads.
    pub fn target(&self) -> &Target {
        &self.target
    }

    /// The output envelope.
    pub fn envelope(&self) -> &Envelope {
        &self.envelope
    }

    /// The `UP TO` timestamp.
    pub fn up_to(&self) -> Option<u64> {
        self.up_to
    }

    /// The `SUBSCRIBE` statement that starts at `start`.
    pub fn sql(&self, start: Start) -> Result<String, UsageError> {
        let mut sql = String::from("SUBSCRIBE ");
        match (&self.target, start) {
            (Target::Object(object), _) => {
                let ObjectRef {
                    id,
                    database,
                    schema,
                    name,
                } = object;
                sql.push_str(&format!("[{} AS ", quote_ident(id)));
                if let Some(database) = database {
                    sql.push_str(&format!("{}.", quote_ident(database)));
                }
                sql.push_str(&format!("{}.{}]", quote_ident(schema), quote_ident(name)));
            }
            (Target::Query { .. }, Start::Resume { .. }) => {
                return Err(UsageError::new(
                    "a query cannot resume; subscribe to an object to resume",
                ));
            }
            // The newlines keep a trailing `--` comment in the query from
            // swallowing the closing parenthesis.
            (Target::Query { sql: query }, Start::Snapshot { .. }) => {
                sql.push_str(&format!("(\n{query}\n)"));
            }
        };
        if let Envelope::Upsert { key } = &self.envelope {
            if key.is_empty() {
                return Err(UsageError::new(
                    "ENVELOPE UPSERT needs at least one key column",
                ));
            }
            if key.iter().collect::<BTreeSet<_>>().len() != key.len() {
                return Err(UsageError::new("ENVELOPE UPSERT names a key column twice"));
            }
            let key: Vec<String> = key.iter().map(|k| quote_ident(k)).collect();
            sql.push_str(&format!(" ENVELOPE UPSERT (KEY ({}))", key.join(", ")));
        }
        let (snapshot, as_of) = match start {
            Start::Snapshot { as_of } => (true, as_of),
            Start::Resume { frontier } => (false, Some(frontier.resume_as_of())),
        };
        // The server refuses these bounds (`AbsurdSubscribeBounds`) with a
        // SQLSTATE it shares with lost history.
        if let (Some(as_of), Some(up_to)) = (as_of, self.up_to) {
            if as_of > up_to {
                return Err(UsageError::new(format!(
                    "AS OF {as_of} is past UP TO {up_to}"
                )));
            }
        }
        sql.push_str(&format!(" WITH (PROGRESS, SNAPSHOT = {snapshot})"));
        if let Some(as_of) = as_of {
            sql.push_str(&format!(" AS OF {as_of}"));
        }
        if let Some(up_to) = self.up_to {
            sql.push_str(&format!(" UP TO {up_to}"));
        }
        Ok(sql)
    }
}

/// `DECLARE cursor CURSOR FOR subscribe`. A cursor lives inside a
/// transaction, so the transport runs `BEGIN` first.
pub(crate) fn declare_sql(cursor: &str, subscribe: &str) -> String {
    format!("DECLARE {} CURSOR FOR {subscribe}", quote_ident(cursor))
}

/// The longest `FETCH` timeout Materialize accepts (`plan_fetch` in
/// `src/sql/src/plan/statement/scl.rs`).
pub const MAX_FETCH_TIMEOUT: Duration = Duration::from_secs(24 * 60 * 60);

/// `FETCH max_rows cursor`, waiting at most `timeout`, rounded down to
/// milliseconds.
///
/// Without a timeout, `FETCH` waits until at least one row is available or
/// the cursor is exhausted (`ExecuteTimeout::WaitOnce` in `send_rows`,
/// `src/pgwire/src/protocol.rs`), so an empty result means the stream has
/// ended. With a timeout, an empty result also means the timeout expired.
pub fn fetch_sql(
    cursor: &str,
    max_rows: u64,
    timeout: Option<Duration>,
) -> Result<String, UsageError> {
    if max_rows == 0 {
        return Err(UsageError::new("FETCH needs a row count of at least one"));
    }
    let mut sql = format!("FETCH {max_rows} {}", quote_ident(cursor));
    if let Some(timeout) = timeout {
        if timeout > MAX_FETCH_TIMEOUT {
            return Err(UsageError::new(format!(
                "a FETCH timeout of {timeout:?} exceeds the maximum of {MAX_FETCH_TIMEOUT:?}"
            )));
        }
        sql.push_str(&format!(
            " WITH (TIMEOUT = '{} milliseconds')",
            timeout.as_millis()
        ));
    }
    Ok(sql)
}

/// Quotes an identifier, doubling any `"` it contains.
pub(crate) fn quote_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::frontier::Frontier;

    fn orders() -> ObjectRef {
        ObjectRef {
            id: "u42".into(),
            database: Some("materialize".into()),
            schema: "public".into(),
            name: "orders".into(),
        }
    }

    const SNAPSHOT: Start = Start::Snapshot { as_of: None };

    fn resume(t: u64) -> Start {
        Start::Resume {
            frontier: Frontier::new(t).unwrap(),
        }
    }

    #[test]
    fn subscribes_to_an_object_by_id() {
        assert_eq!(
            Subscribe::object(orders()).sql(SNAPSHOT).unwrap(),
            r#"SUBSCRIBE ["u42" AS "materialize"."public"."orders"] WITH (PROGRESS, SNAPSHOT = true)"#
        );
    }

    #[test]
    fn resumes_at_one_below_the_frontier() {
        assert_eq!(
            Subscribe::object(orders()).sql(resume(100)).unwrap(),
            r#"SUBSCRIBE ["u42" AS "materialize"."public"."orders"] WITH (PROGRESS, SNAPSHOT = false) AS OF 99"#
        );
    }

    #[test]
    fn places_the_envelope_before_with_and_up_to_last() {
        let sql = Subscribe::object(orders())
            .with_envelope(Envelope::Upsert {
                key: vec!["region".into(), "id".into()],
            })
            .with_up_to(500)
            .sql(Start::Snapshot { as_of: Some(7) })
            .unwrap();
        assert_eq!(
            sql,
            r#"SUBSCRIBE ["u42" AS "materialize"."public"."orders"] ENVELOPE UPSERT (KEY ("region", "id")) WITH (PROGRESS, SNAPSHOT = true) AS OF 7 UP TO 500"#
        );
    }

    #[test]
    fn an_object_in_an_ambient_schema_has_no_database() {
        let object = ObjectRef {
            id: "s1".into(),
            database: None,
            schema: "mz_catalog".into(),
            name: "mz_tables".into(),
        };
        assert_eq!(
            Subscribe::object(object).sql(SNAPSHOT).unwrap(),
            r#"SUBSCRIBE ["s1" AS "mz_catalog"."mz_tables"] WITH (PROGRESS, SNAPSHOT = true)"#
        );
    }

    #[test]
    fn a_query_survives_a_trailing_comment() {
        assert_eq!(
            Subscribe::query("SELECT 1 -- one").sql(SNAPSHOT).unwrap(),
            "SUBSCRIBE (\nSELECT 1 -- one\n) WITH (PROGRESS, SNAPSHOT = true)"
        );
    }

    #[test]
    fn a_query_cannot_resume() {
        assert!(Subscribe::query("SELECT 1").sql(resume(5)).is_err());
    }

    #[test]
    fn as_of_past_up_to_is_refused() {
        let bounded = Subscribe::object(orders()).with_up_to(10);
        assert!(bounded.sql(Start::Snapshot { as_of: Some(11) }).is_err());
        assert!(bounded.sql(resume(12)).is_err());
        assert!(bounded.sql(resume(11)).is_ok(), "AS OF 10 equals UP TO 10");
    }

    #[test]
    fn target_json_is_tagged_by_kind() {
        let query = Subscribe::query("SELECT 1");
        assert_eq!(
            serde_json::to_value(query.target()).unwrap(),
            serde_json::json!({"kind": "query", "sql": "SELECT 1"})
        );
        let object = serde_json::json!({
            "kind": "object", "id": "u1", "database": null, "schema": "s", "name": "t"
        });
        assert!(serde_json::from_value::<Target>(object).is_ok());
        let typo = serde_json::json!({"kind": "query", "sql": "SELECT 1", "up_to": "5"});
        assert!(serde_json::from_value::<Target>(typo).is_err());
    }

    #[test]
    fn an_upsert_needs_a_key() {
        for key in [vec![], vec!["id".to_string(), "id".to_string()]] {
            let sql = Subscribe::object(orders())
                .with_envelope(Envelope::Upsert { key })
                .sql(SNAPSHOT);
            assert!(sql.is_err());
        }
    }

    #[test]
    fn identifiers_are_quoted() {
        let object = ObjectRef {
            id: "u1".into(),
            database: Some("d".into()),
            schema: "s".into(),
            name: r#"we"ird"#.into(),
        };
        let sql = Subscribe::object(object)
            .with_envelope(Envelope::Upsert {
                key: vec![r#"k"ey"#.into()],
            })
            .sql(SNAPSHOT)
            .unwrap();
        assert!(sql.contains(r#""s"."we""ird"]"#), "{sql}");
        assert!(sql.contains(r#"KEY ("k""ey")"#), "{sql}");
    }

    #[test]
    fn cursor_statements() {
        assert_eq!(
            declare_sql("c", "SUBSCRIBE t"),
            r#"DECLARE "c" CURSOR FOR SUBSCRIBE t"#
        );
        assert_eq!(fetch_sql("c", 1000, None).unwrap(), r#"FETCH 1000 "c""#);
        assert_eq!(
            fetch_sql("c", 10, Some(Duration::from_micros(1_500_900))).unwrap(),
            r#"FETCH 10 "c" WITH (TIMEOUT = '1500 milliseconds')"#
        );
        assert!(fetch_sql("c", 0, None).is_err());
        assert!(fetch_sql("c", 1, Some(MAX_FETCH_TIMEOUT)).is_ok());
        assert!(fetch_sql("c", 1, Some(MAX_FETCH_TIMEOUT + Duration::from_millis(1))).is_err());
    }
}
