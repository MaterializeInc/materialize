// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Anonymizes SQL using Materialize's own parser.
//!
//! This is the SQL-rewriting half of `bin/mz-workload-anonymize`. Doing the
//! work on the parsed AST — rather than with text substitution — is what makes
//! it correct: identifiers are renamed as whole tokens (no substring or
//! in-string corruption, no word-boundary or case guesswork), and literals are
//! redacted in every position the dialect allows, including option values like
//! connection hosts/users and sink topics. The engine's own redacted `Display`
//! is *not* enough here: it deliberately keeps connection metadata (broker
//! hosts, usernames, URLs) for telemetry, so this visitor redacts more
//! aggressively, reaching the typed `String` fields the dialect hides literals
//! in (`KafkaBroker::address`, broker matching-rule patterns).
//!
//! Two redaction levels:
//!
//! - `redact_literals` redacts string-like literals (strings, hex strings) and
//!   the typed option strings above. Intervals/durations (`'1s'`,
//!   `INTERVAL '60' DAY`) are kept — they are non-sensitive config replay needs.
//! - `redact_numbers` *additionally* redacts numeric literals, to the neutral
//!   literal `1` (NOT a string: `'<REDACTED>'` does not parse where a number is
//!   required — `LIMIT`/`OFFSET`, an `int` comparison, a `GROUP BY` ordinal —
//!   and `1` is a valid 1-based ordinal that avoids division-by-zero). It is set
//!   for queries AND for view/materialized-view/index *bodies*, where a number
//!   is a predicate value (`WHERE ssn = 123456789`) that leaks data. It is NOT
//!   set for tables/sources/sinks/clusters, whose numbers sit in option
//!   positions (sizes, ports, replication factors, numeric column defaults) that
//!   are config replay needs valid.
//!
//! Config statements (CREATE/ALTER CLUSTER, CLUSTER REPLICA, SET/RESET/SET
//! TRANSACTION, ALTER SYSTEM) are exempt from all literal redaction — see
//! [`preserves_literals`].
//!
//! Protocol: reads a JSON request on stdin and writes a JSON array on stdout,
//! one element per input statement — the rewritten SQL, or `null` when the
//! statement could not be parsed (the caller treats that as a hard error).
//!
//! ```json
//! {
//!   "mapping": {"orders": "table_1", "auction_house": "db_0"},
//!   "rename_identifiers": true,
//!   "redact_literals": true,
//!   "redact_numbers": true,
//!   "statements": ["SELECT * FROM orders WHERE id = 7", "..."]
//! }
//! ```

use std::collections::{BTreeMap, BTreeSet};
use std::io::{self, Read, Write};

use mz_sql_parser::ast::display::AstDisplay;
use mz_sql_parser::ast::visit::{self, Visit};
use mz_sql_parser::ast::visit_mut::{self, VisitMut};
use mz_sql_parser::ast::{
    ConnectionRulePattern, CreateIndexStatement, CsrSeedAvro, Ident, KafkaBroker,
    KafkaSinkConfigOption, KafkaSinkConfigOptionName, Raw, RawClusterName, RawDataType,
    RawItemName, RawNetworkPolicyName, Schema, SelectItem, SetVariableStatement, SetVariableTo,
    SetVariableValue, SourceIncludeMetadata, Statement, TableAlias, TableConstraint,
    UnresolvedDatabaseName, UnresolvedItemName, UnresolvedObjectName, UnresolvedSchemaName, Value,
    WithOptionValue,
};
use mz_sql_parser::parser;
use serde::Deserialize;

#[derive(Deserialize)]
struct Request {
    /// Original identifier -> anonymized identifier. Entries that map a name to
    /// itself (e.g. preserved keywords) are no-ops.
    #[serde(default)]
    mapping: BTreeMap<String, String>,
    #[serde(default)]
    rename_identifiers: bool,
    /// Redact string-like literals and typed option strings.
    #[serde(default)]
    redact_literals: bool,
    /// Additionally redact numeric literals (set for queries, not DDL).
    #[serde(default)]
    redact_numbers: bool,
    /// Rename string-valued NAMES consistently instead of collapsing them to
    /// `'<REDACTED>'`. Set for DDL: a topic, upstream database, publication, or
    /// external reference is an identifier the replay tooling extracts and
    /// cross-references, so each distinct original must map to the same valid
    /// replacement (its `mapping` entry, else a deterministic `redacted_<hash>`).
    /// Off for queries, whose string literals are data and stay `'<REDACTED>'`
    /// (which replay binds as NULL).
    #[serde(default)]
    consistent_names: bool,
    /// Per-run random salt mixed into the `redacted_<hash>` tokens, so they
    /// stay consistent within a run but cannot be reversed by an offline
    /// dictionary attack on the shared output. The caller passes the same salt
    /// for every invocation of one anonymization run.
    #[serde(default)]
    salt: String,
    /// SQL keywords and built-in object/function/type names (lowercased). Used
    /// to avoid renaming a query-local identifier that collides with a builtin.
    #[serde(default)]
    keywords: Vec<String>,
    /// When set, the helper does not rewrite: it parses each statement and
    /// returns the set of object-name identifiers it references that are not
    /// already in `mapping` and not keywords/builtins. The caller assigns them
    /// anonymized names and feeds an augmented `mapping` back for the rewrite.
    /// This catches objects created/dropped *during* the capture window, which
    /// are absent from the catalog snapshot the mapping is built from.
    #[serde(default)]
    collect: bool,
    statements: Vec<String>,
}

struct Anonymizer<'a> {
    mapping: &'a BTreeMap<String, String>,
    /// Per-statement renames for query-local identifiers (CTE names, column
    /// aliases, constraint/index names) that are not captured catalog objects.
    /// Checked before `mapping`.
    locals: &'a BTreeMap<String, String>,
    rename: bool,
    /// Redact string-like literals (strings, hex, intervals) and typed option
    /// strings (broker hosts, matching-rule patterns).
    redact: bool,
    /// Also redact numeric literals. Only consulted when `redact` is set.
    redact_numbers: bool,
    /// Consistently rename string-valued names instead of redacting them (DDL).
    consistent_names: bool,
    /// Per-run salt for `redacted_<hash>` tokens (see [`short_hash`]).
    salt: &'a str,
}

/// Whether a string is a bare datetime-*field* keyword (`'year'`, `'month'`,
/// `'day'`, ...) — the field argument to `date_trunc`/`date_part`/`extract`.
/// These are field specifiers, not data, in BOTH query and DDL contexts, so
/// they are kept everywhere; redacting one would make the expression invalid.
///
/// LIMITATION: because this is a value-shape heuristic with no expression
/// context, a short data value that happens to spell a datetime field
/// (`status = 'day'`) is kept verbatim. Such values are low-entropy and rare;
/// the alternative — redacting them — would break `date_trunc('day', ...)`.
fn is_date_field(s: &str) -> bool {
    const DATE_FIELDS: &[&str] = &[
        "microseconds",
        "microsecond",
        "milliseconds",
        "millisecond",
        "second",
        "minute",
        "hour",
        "day",
        "week",
        "month",
        "quarter",
        "year",
        "decade",
        "century",
        "millennium",
        "dow",
        "doy",
        "isodow",
        "isoyear",
        "epoch",
        "julian",
        "timezone",
        "timezone_hour",
        "timezone_minute",
    ];
    DATE_FIELDS.contains(&s.trim().to_ascii_lowercase().as_str())
}

/// Whether a string is a *number-bearing* duration/interval — `1s`, `500ms`,
/// `0.1s`, `1 day`, `2 hours 30 minutes`, `00:05:00`. In DDL these are
/// non-sensitive config (load-generator tick intervals, refresh/retention
/// windows, timeouts) that replay needs intact, so the DDL path keeps them; in
/// a query the same shape (`WHERE note = '5 minutes'`) is data, so it is
/// redacted. Both a numeric magnitude and a unit are *required*: a bare unit
/// word (`'y'`, `'min'`) and a bare number or number sequence (`'123456789'`,
/// `'555 1234'`) are data, not durations, and are always redacted. A `HH:MM:SS`
/// colon-time word counts as both.
fn is_number_interval(s: &str) -> bool {
    // Units accepted only with a numeric magnitude (`5 minutes`, `500ms`).
    const UNITS: &[&str] = &[
        "microseconds",
        "microsecond",
        "us",
        "milliseconds",
        "millisecond",
        "ms",
        "seconds",
        "second",
        "secs",
        "sec",
        "s",
        "minutes",
        "minute",
        "mins",
        "min",
        "m",
        "hours",
        "hour",
        "hrs",
        "hr",
        "h",
        "days",
        "day",
        "d",
        "weeks",
        "week",
        "w",
        "months",
        "month",
        "mons",
        "mon",
        "years",
        "year",
        "yrs",
        "yr",
        "y",
    ];
    let is_number = |t: &str| {
        !t.is_empty()
            && t.chars().all(|c| c.is_ascii_digit() || c == '.')
            && t.chars().any(|c| c.is_ascii_digit())
    };
    let s = s.trim();
    if s.is_empty() {
        return false;
    }
    let mut saw_number = false;
    let mut saw_unit = false;
    for word in s.split_whitespace() {
        let word = word.to_ascii_lowercase();
        if is_number(&word) {
            saw_number = true;
            continue;
        }
        // `HH:MM:SS[.fff]` is a self-contained duration.
        if word.contains(':')
            && word
                .chars()
                .all(|c| c.is_ascii_digit() || c == ':' || c == '.')
            && word.chars().any(|c| c.is_ascii_digit())
        {
            saw_number = true;
            saw_unit = true;
            continue;
        }
        if UNITS.contains(&word.as_str()) {
            saw_unit = true;
            continue;
        }
        // `<number><unit>` with no space, e.g. `500ms`, `0.1s`, `1d`.
        if let Some(idx) = word.find(|c: char| c.is_ascii_alphabetic()) {
            let (num, unit) = word.split_at(idx);
            if is_number(num) && UNITS.contains(&unit) {
                saw_number = true;
                saw_unit = true;
                continue;
            }
        }
        return false;
    }
    // A duration needs BOTH a magnitude and a unit (or a colon-time word). A bare
    // number or number sequence (`'123456789'`, `'555 1234'`) is data, not config.
    saw_number && saw_unit
}

/// Whether an item name is a `mz_load_generators.<generator>.<table>` reference
/// (e.g. `mz_load_generators.auction.accounts`). These name built-in
/// load-generator outputs; every component is fixed, so the whole reference is
/// kept verbatim. (The table names — `accounts`, `orders`, `users`, ... — would
/// otherwise collide with user identifiers, so this is matched on the namespace
/// rather than by adding the bare names to the keep set.)
fn is_load_generator_ref(idents: &[Ident]) -> bool {
    idents.first().map(Ident::as_str) == Some("mz_load_generators")
}

/// Salted hash (`salt` mixed in first), rendered as a stable hex suffix, using
/// the standard library's SipHash — purpose-built to be unguessable without the
/// key, which is exactly the threat model here. The same input yields the same
/// token *within a run* (so consistent renaming lines up — TEXT COLUMNS,
/// external references, topic/namespace), which is all the consistency replay
/// needs; cross-run stability is not required (the salt differs anyway). The
/// per-run random `salt` (chosen by the caller, never emitted) defeats an
/// offline dictionary attack on the output: without it, low-entropy originals (a
/// region code, a status, a short name) could be recovered by recomputing
/// `hash(candidate)`.
fn short_hash(salt: &str, s: &str) -> String {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    salt.hash(&mut hasher);
    s.hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

impl Anonymizer<'_> {
    fn rename_ident(&self, ident: &mut Ident) {
        if self.rename {
            if let Some(new) = self
                .locals
                .get(ident.as_str())
                .or_else(|| self.mapping.get(ident.as_str()))
            {
                *ident = Ident::new_unchecked(new.clone());
            }
        }
    }

    fn rename_idents(&self, idents: &mut [Ident]) {
        for ident in idents {
            self.rename_ident(ident);
        }
    }

    /// The consistent, valid replacement for a `Value::String` (a topic, host,
    /// connection `DATABASE`/`PUBLICATION`, SASL username, or a string-literal
    /// predicate in a view body). The same original always yields the same
    /// output, so the positions the replay tooling extracts stay linked — e.g. a
    /// Postgres connection's `DATABASE = 'x'` renders identically to where the
    /// same `x` appears as an identifier in an `EXTERNAL REFERENCE` (item-name
    /// positions take the same mapping path via `rename_ident`).
    ///
    /// A non-identity `mapping`/`locals` entry (a real catalog/collected object)
    /// wins. An *identity* entry — a kept keyword/builtin name mapped to itself,
    /// e.g. a column literally named `id` or the type name `text` — is NOT a
    /// rename; routing data through it would leak any literal that happens to
    /// spell such a name (a JSON key `'id'`, a `type = 'text'` predicate). For
    /// those, and for any string not in the mapping (genuine data: hosts,
    /// topics), fall back to a stable `redacted_<hash>`.
    fn anon_name(&self, original: &str) -> String {
        if let Some(new) = self
            .locals
            .get(original)
            .or_else(|| self.mapping.get(original))
        {
            if new != original {
                return new.clone();
            }
        }
        format!("redacted_{}", short_hash(self.salt, original))
    }

    /// Rename a catalog object that appears as a string value (e.g. the cluster
    /// in `SET cluster = 'name'`) to its anonymized catalog name, leaving
    /// builtins and other names not in `mapping`/`locals` (e.g. `quickstart`,
    /// `public`) untouched so the statement stays valid for replay. Unlike
    /// [`Self::anon_name`], this is for object references, not data, so it must
    /// use the catalog mapping rather than a hash.
    fn rename_object_string(&self, name: &str) -> String {
        self.locals
            .get(name)
            .or_else(|| self.mapping.get(name))
            .cloned()
            .unwrap_or_else(|| name.to_string())
    }

    /// Anonymize an inline Avro JSON schema in place, keeping it valid and
    /// replayable. A record/field `"name"` that is a captured column is renamed
    /// to its anonymized name (so the source's columns, the views that reference
    /// them, and the Avro records replay produces from those same columns all
    /// agree); structural names the decoder relies on (`before`, `after`, `op`,
    /// `Envelope`, ...) are not in the mapping and stay put. `"namespace"` /
    /// `"connect.name"` carry upstream package names (e.g.
    /// `acme.inventory.orders`) that must not leak; their non-structural
    /// components are hashed. Namespaces are not load-bearing for field
    /// resolution, so scrubbing them is safe.
    fn anonymize_schema_json(&self, raw: &str) -> String {
        match serde_json::from_str::<serde_json::Value>(raw) {
            Ok(mut v) => {
                self.scrub_schema_value(&mut v);
                serde_json::to_string(&v).unwrap_or_else(|_| raw.to_string())
            }
            // Not JSON (shouldn't happen for an Avro schema): leave untouched.
            Err(_) => raw.to_string(),
        }
    }

    fn scrub_schema_value(&self, v: &mut serde_json::Value) {
        match v {
            serde_json::Value::Object(map) => {
                for (key, val) in map.iter_mut() {
                    match (key.as_str(), &mut *val) {
                        ("name", serde_json::Value::String(s)) => {
                            *s = self.rename_object_string(s);
                        }
                        ("namespace" | "connect.name", serde_json::Value::String(s)) => {
                            *s = self.scrub_namespace(s);
                        }
                        _ => self.scrub_schema_value(val),
                    }
                }
            }
            serde_json::Value::Array(arr) => {
                for item in arr.iter_mut() {
                    self.scrub_schema_value(item);
                }
            }
            _ => {}
        }
    }

    /// Scrub a dotted Avro namespace. A well-known structural package
    /// (`io.debezium...`, `io.confluent...`) is kept — it must match the schema
    /// replay generates. Otherwise the namespace is an upstream one which, by
    /// the debezium convention, equals the source topic; it is anonymized *as a
    /// whole string* (not component-wise) so it hashes to the SAME token as that
    /// topic's `Value::String`. That keeps the SEED (reader) and the schema
    /// replay registers (writer, namespaced by topic) resolvable — Avro key/value
    /// decoding matches records by fully-qualified `namespace.name`.
    fn scrub_namespace(&self, ns: &str) -> String {
        const STRUCTURAL: &[&str] = &[
            "io",
            "org",
            "com",
            "apache",
            "debezium",
            "confluent",
            "connect",
            "connector",
            "mysql",
            "postgres",
            "postgresql",
            "sqlserver",
            "avro",
            "kafka",
        ];
        if ns
            .split('.')
            .all(|c| c.is_empty() || STRUCTURAL.contains(&c))
        {
            ns.to_string()
        } else {
            self.anon_name(ns)
        }
    }
}

impl<'ast> VisitMut<'ast, Raw> for Anonymizer<'_> {
    fn visit_ident_mut(&mut self, node: &'ast mut Ident) {
        self.rename_ident(node);
    }

    fn visit_value_mut(&mut self, node: &'ast mut Value) {
        if self.redact {
            match node {
                // In DDL a string value is usually a NAME (topic, upstream
                // database, publication, ...) the replay tooling extracts and
                // cross-references, so rename it consistently to a valid token.
                // In a query it is data, so collapse it to the `'<REDACTED>'`
                // sentinel (which the replay tooling binds as NULL); using a
                // string keeps the output reparseable even for numeric columns.
                Value::String(s) => {
                    // A datetime field (`date_trunc('day', ...)`) is a specifier,
                    // not data, in BOTH queries and DDL — keep it (redacting it
                    // makes the expression invalid). A number-bearing duration is
                    // non-sensitive config in DDL (tick intervals, refresh/
                    // retention windows) so it is kept there, but in a query the
                    // same shape (`WHERE note = '5 minutes'`) is data, so it is
                    // redacted. Everything else is renamed (DDL) or collapsed to
                    // the sentinel (queries).
                    if is_date_field(s) || (self.consistent_names && is_number_interval(s)) {
                        // keep
                    } else {
                        *node = Value::String(if self.consistent_names {
                            self.anon_name(s)
                        } else {
                            "<REDACTED>".to_string()
                        });
                    }
                }
                // An interval literal (the `Value::Interval` type, e.g.
                // `INTERVAL '60' DAY`) is a duration, never sensitive — keep it.
                Value::Interval(_) => {}
                Value::HexString(_) => {
                    *node = Value::String("<REDACTED>".to_string());
                }
                // A redacted number becomes the neutral literal `1`, NOT a
                // string: a string does not parse where a number is required
                // (`LIMIT`/`OFFSET`, an `int` comparison, a `GROUP BY`/`ORDER BY`
                // ordinal), and `1` specifically is a valid 1-based ordinal and
                // avoids the division/modulo-by-zero that `0` would introduce in
                // a redacted view body. The value itself is gone either way.
                Value::Number(_) if self.redact_numbers => {
                    *node = Value::Number("1".to_string());
                }
                // Numbers in DDL *option* positions (cluster size, port,
                // replication factor, numeric column default) are non-sensitive
                // config replay needs valid, so they are kept; `redact_numbers`
                // is set only for queries and view/MV/index bodies (data). And
                // booleans/NULL are never sensitive.
                Value::Number(_) | Value::Boolean(_) | Value::Null => {}
            }
        }
    }

    // Some sensitive option strings are typed `String` fields the dialect does
    // not model as `Value`, so the generic literal redaction above never sees
    // them. Override their visitors to redact the string directly while still
    // descending for any identifier renaming (e.g. an SSH tunnel reference).

    fn visit_kafka_broker_mut(&mut self, node: &'ast mut KafkaBroker<Raw>) {
        if self.redact {
            node.address = self.anon_name(&node.address);
        }
        visit_mut::visit_kafka_broker_mut(self, node);
    }

    fn visit_connection_rule_pattern_mut(&mut self, node: &'ast mut ConnectionRulePattern) {
        if self.redact {
            // The host:port match pattern is a broker address; rename it whole
            // and drop the wildcards (which would otherwise hint at structure).
            node.prefix_wildcard = false;
            node.literal_match = self.anon_name(&node.literal_match);
            node.suffix_wildcard = false;
        }
    }

    // Inline Avro schemas (`FORMAT AVRO USING SCHEMA '...'`, CSR `SEED KEY/VALUE
    // SCHEMA '...'`) hold the upstream record structure as typed `String` fields
    // the generic visitor leaves alone. They must stay valid JSON so replay can
    // decode against them, but they also embed column names (which must match
    // the renamed columns) and upstream package namespaces (which must not
    // leak). So rather than redact them whole, anonymize the JSON in place; see
    // `anonymize_schema_json`. (Protobuf descriptors are binary and not present
    // in our captures, so they fall through to the default visitor.)

    fn visit_schema_mut(&mut self, node: &'ast mut Schema) {
        if self.redact {
            node.schema = self.anonymize_schema_json(&node.schema);
        }
    }

    fn visit_csr_seed_avro_mut(&mut self, node: &'ast mut CsrSeedAvro) {
        if self.redact {
            if let Some(key_schema) = node.key_schema.as_mut() {
                *key_schema = self.anonymize_schema_json(key_schema);
            }
            node.value_schema = self.anonymize_schema_json(&node.value_schema);
            for schema in node.key_reference_schemas.iter_mut() {
                *schema = self.anonymize_schema_json(schema);
            }
            for schema in node.value_reference_schemas.iter_mut() {
                *schema = self.anonymize_schema_json(schema);
            }
        }
    }

    // Vetted preserve-list: a `COMPRESSION TYPE` is a fixed enum (none, gzip,
    // lz4, zstd) — non-sensitive config that the sink's replayed DDL needs valid,
    // so keep its value rather than hashing it. Every other sink option falls
    // through to normal redaction (a `TOPIC` or id prefix is still scrubbed), so
    // this stays fail-safe: only this explicitly-allowed option is preserved.
    fn visit_kafka_sink_config_option_mut(&mut self, node: &'ast mut KafkaSinkConfigOption<Raw>) {
        if matches!(node.name, KafkaSinkConfigOptionName::CompressionType) {
            return;
        }
        visit_mut::visit_kafka_sink_config_option_mut(self, node);
    }

    // `INCLUDE HEADER '<name>' AS <alias>`: the header key is a typed `String`
    // (not a `Value`), so redact it directly. The alias is renamed by descent.
    fn visit_source_include_metadata_mut(&mut self, node: &'ast mut SourceIncludeMetadata) {
        if self.redact {
            if let SourceIncludeMetadata::Header { key, .. } = node {
                *key = "<REDACTED>".to_string();
            }
        }
        visit_mut::visit_source_include_metadata_mut(self, node);
    }

    // A `MAP[...]` option value is preserved whole. The ONLY map-valued option
    // in the engine is Kafka's `TOPIC CONFIG = MAP['retention.ms' => '...']`
    // (`KafkaSinkConfigOption::TopicConfig: BTreeMap<String, String>` in
    // src/sql/src/kafka_util.rs — the sole `BTreeMap`-typed option), which holds
    // non-sensitive Kafka topic settings replay needs verbatim: redacting the
    // values would break replay, and map keys cannot be redacted without
    // colliding (the map would lose entries). If a *sensitive* map-valued option
    // is ever added, this blanket preservation must become an allowlist keyed on
    // the option name (as `visit_kafka_sink_config_option_mut` does).
    fn visit_with_option_value_mut(&mut self, node: &'ast mut WithOptionValue<Raw>) {
        if matches!(node, WithOptionValue::Map(_)) {
            return;
        }
        visit_mut::visit_with_option_value_mut(self, node);
    }

    // `PREPARE`/`DECLARE` carry their inner statement as `T::NestedStatement` —
    // another associated type the generic visitor treats as opaque, so without
    // this the whole prepared query (identifiers and literals) goes untouched.
    fn visit_nested_statement_mut(&mut self, node: &'ast mut Statement<Raw>) {
        self.visit_statement_mut(node);
    }

    // The `REFERENCE`/`FOR TABLES` external references name *upstream* objects
    // (the source's external database/schema/table). They are plain item-name
    // AST positions, so the default identifier renaming handles them — and must,
    // for replay: the collector picks up every referenced upstream name, so each
    // renames to its mapping entry (a captured object, a collected upstream name,
    // or a kept keyword) *exactly the same way wherever it appears*. That keeps a
    // source's connection `DATABASE`, its child `EXTERNAL REFERENCE`, the parent
    // `FOR TABLES` list, and the local tables replay provisions all lined up. We
    // deliberately do NOT route these through `anon_name`: it skips identity
    // entries (kept keywords) to a hash, which would desync an upstream table
    // named like a keyword (e.g. `comment`) from the rest of its setup.

    // `SET <var> = <value>` is preserved config in general (timeouts, isolation,
    // feature flags — see `preserves_literals`), but two cases carry sensitive
    // data the default handling would leak: the value of `cluster`/`database`/
    // `schema`/`search_path` is a captured-object reference (rename it), and
    // `application_name` is a free-form session label that tools fill with host
    // and database names (redact it). Other settings are left verbatim.
    fn visit_set_variable_statement_mut(&mut self, node: &'ast mut SetVariableStatement) {
        let SetVariableTo::Values(values) = &mut node.to else {
            return;
        };
        match node.variable.as_str().to_lowercase().as_str() {
            "cluster" | "database" | "schema" | "search_path" if self.rename => {
                // These name objects; rename consistently (the same cluster/db
                // is renamed identically wherever else it appears).
                for value in values.iter_mut() {
                    match value {
                        SetVariableValue::Ident(ident) => {
                            *ident =
                                Ident::new_unchecked(self.rename_object_string(ident.as_str()));
                        }
                        SetVariableValue::Literal(Value::String(name)) => {
                            *name = self.rename_object_string(name);
                        }
                        SetVariableValue::Literal(_) => {}
                    }
                }
            }
            "application_name" if self.redact => {
                for value in values.iter_mut() {
                    if let SetVariableValue::Literal(Value::String(label)) = value {
                        *label = "<REDACTED>".to_string();
                    }
                }
            }
            _ => {}
        }
    }

    // The `Raw` AstInfo associated types below have no-op default visitors (the
    // generic visitor cannot see into an associated type), yet they hold the
    // identifiers for object/cluster/type references. Override each to descend
    // to its `Ident`s so renaming reaches them.

    fn visit_item_name_mut(&mut self, node: &'ast mut RawItemName) {
        match node {
            RawItemName::Name(n) | RawItemName::Id(_, n, _) => self.rename_idents(&mut n.0),
        }
    }

    fn visit_unresolved_item_name_mut(&mut self, node: &'ast mut UnresolvedItemName) {
        if is_load_generator_ref(&node.0) {
            return;
        }
        self.rename_idents(&mut node.0);
    }

    fn visit_column_reference_mut(&mut self, node: &'ast mut Ident) {
        self.rename_ident(node);
    }

    fn visit_schema_name_mut(&mut self, node: &'ast mut UnresolvedSchemaName) {
        self.rename_idents(&mut node.0);
    }

    fn visit_database_name_mut(&mut self, node: &'ast mut UnresolvedDatabaseName) {
        self.rename_ident(&mut node.0);
    }

    fn visit_cluster_name_mut(&mut self, node: &'ast mut RawClusterName) {
        if let RawClusterName::Unresolved(ident) = node {
            self.rename_ident(ident);
        }
    }

    fn visit_network_policy_name_mut(&mut self, node: &'ast mut RawNetworkPolicyName) {
        if let RawNetworkPolicyName::Unresolved(ident) = node {
            self.rename_ident(ident);
        }
    }

    fn visit_data_type_mut(&mut self, node: &'ast mut RawDataType) {
        match node {
            RawDataType::Array(inner) | RawDataType::List(inner) => self.visit_data_type_mut(inner),
            RawDataType::Map {
                key_type,
                value_type,
            } => {
                self.visit_data_type_mut(key_type);
                self.visit_data_type_mut(value_type);
            }
            RawDataType::Other { name, .. } => self.visit_item_name_mut(name),
        }
    }

    fn visit_object_name_mut(&mut self, node: &'ast mut UnresolvedObjectName) {
        match node {
            UnresolvedObjectName::Cluster(i)
            | UnresolvedObjectName::Role(i)
            | UnresolvedObjectName::NetworkPolicy(i) => self.rename_ident(i),
            UnresolvedObjectName::Database(n) => self.rename_ident(&mut n.0),
            UnresolvedObjectName::Schema(n) => self.rename_idents(&mut n.0),
            UnresolvedObjectName::Item(n) => self.rename_idents(&mut n.0),
            UnresolvedObjectName::ClusterReplica(_) => visit_mut::visit_object_name_mut(self, node),
        }
    }
}

/// Statement kinds whose literals are non-sensitive configuration — cluster
/// sizing/replication and session/system settings (timeouts, isolation, feature
/// flags) — which replay needs preserved verbatim. Identifiers in these
/// statements are still renamed; only literal redaction is skipped.
fn preserves_literals(stmt: &Statement<Raw>) -> bool {
    matches!(
        stmt,
        Statement::CreateCluster(_)
            | Statement::CreateClusterReplica(_)
            | Statement::AlterCluster(_)
            | Statement::ResetVariable(_)
            | Statement::SetTransaction(_)
            | Statement::AlterSystemSet(_)
            | Statement::AlterSystemReset(_)
    )
    // `SetVariable` is intentionally absent: it is handled by
    // `visit_set_variable_statement_mut`, which preserves config values but
    // renames object references and redacts `application_name`.
}

/// Collects query-local identifiers — names a statement *defines* rather than
/// references: CTE names, derived-table/table aliases (and their column
/// aliases), `SELECT ... AS` aliases, and constraint/index names. These are not
/// captured catalog objects, so the `mapping` never covers them, yet they can
/// embed sensitive substrings (e.g. a customer name in `cte_<name>_members` or
/// `AS total_<name>`). Collecting only from these *definition* positions — never
/// from function- or type-name positions — keeps builtins untouched, and
/// excluding keyword/builtin names guards the rare alias that shadows one.
struct LocalNameCollector<'a> {
    keywords: &'a BTreeSet<String>,
    mapping: &'a BTreeMap<String, String>,
    names: BTreeSet<String>,
}

impl LocalNameCollector<'_> {
    fn add(&mut self, ident: &Ident) {
        let name = ident.as_str();
        if !self.mapping.contains_key(name) && !self.keywords.contains(&name.to_lowercase()) {
            self.names.insert(name.to_string());
        }
    }
}

impl<'ast> Visit<'ast, Raw> for LocalNameCollector<'_> {
    fn visit_table_alias(&mut self, node: &'ast TableAlias) {
        // Covers CTE names (a `Cte`'s alias) and table/derived-table aliases.
        self.add(&node.name);
        for column in &node.columns {
            self.add(column);
        }
    }

    fn visit_select_item(&mut self, node: &'ast SelectItem<Raw>) {
        if let SelectItem::Expr {
            alias: Some(alias), ..
        } = node
        {
            self.add(alias);
        }
        visit::visit_select_item(self, node);
    }

    fn visit_table_constraint(&mut self, node: &'ast TableConstraint<Raw>) {
        match node {
            TableConstraint::Unique { name, .. }
            | TableConstraint::ForeignKey { name, .. }
            | TableConstraint::Check { name, .. } => {
                if let Some(name) = name {
                    self.add(name);
                }
            }
        }
        visit::visit_table_constraint(self, node);
    }

    fn visit_create_index_statement(&mut self, node: &'ast CreateIndexStatement<Raw>) {
        if let Some(name) = &node.name {
            self.add(name);
        }
        visit::visit_create_index_statement(self, node);
    }
}

/// Collects object-name identifiers a statement references — table/view/source
/// names, `CREATE`/`DROP`/`ALTER` targets, schema/database/cluster names — that
/// are not already in `mapping` and not keywords/builtins. These are objects
/// created or dropped *during* the capture window (e.g. dbt-built views, a
/// transient deploy cluster), which never appear in the catalog snapshot the
/// mapping is built from. Type-name positions are skipped (unqualified type
/// aliases like `bigint` are not in the builtin list and must not be renamed);
/// builtin function names are excluded via the keyword set.
struct GlobalNameCollector<'a> {
    keywords: &'a BTreeSet<String>,
    mapping: &'a BTreeMap<String, String>,
    names: BTreeSet<String>,
}

impl GlobalNameCollector<'_> {
    fn add_str(&mut self, name: &str) {
        if !self.mapping.contains_key(name) && !self.keywords.contains(&name.to_lowercase()) {
            self.names.insert(name.to_string());
        }
    }

    fn add(&mut self, ident: &Ident) {
        self.add_str(ident.as_str());
    }

    fn add_idents(&mut self, idents: &[Ident]) {
        for ident in idents {
            self.add(ident);
        }
    }
}

impl<'ast> Visit<'ast, Raw> for GlobalNameCollector<'_> {
    // Object references in queries/DDL — `T::ItemName` (FROM, CREATE … FROM
    // SOURCE) and the concrete `UnresolvedItemName` (CREATE/COMMENT targets).
    fn visit_item_name(&mut self, node: &'ast RawItemName) {
        match node {
            RawItemName::Name(n) | RawItemName::Id(_, n, _) => self.add_idents(&n.0),
        }
    }

    fn visit_unresolved_item_name(&mut self, node: &'ast UnresolvedItemName) {
        // Built-in load-generator references are kept whole by the rewriter, so
        // don't collect (and thus rename) their components.
        if is_load_generator_ref(&node.0) {
            return;
        }
        self.add_idents(&node.0);
    }

    fn visit_unresolved_schema_name(&mut self, node: &'ast UnresolvedSchemaName) {
        self.add_idents(&node.0);
    }

    fn visit_unresolved_database_name(&mut self, node: &'ast UnresolvedDatabaseName) {
        self.add(&node.0);
    }

    fn visit_cluster_name(&mut self, node: &'ast RawClusterName) {
        if let RawClusterName::Unresolved(ident) = node {
            self.add(ident);
        }
    }

    // `DROP`/`ALTER`/`COMMENT`/`GRANT` targets are `UnresolvedObjectName`, whose
    // default descent sends cluster/role/network-policy names to `visit_ident`
    // (which we deliberately do not override). Handle every variant here so
    // those names — e.g. a `DROP SCHEMA <dev_schema>` or `DROP CLUSTER <deploy>`
    // for window-created objects — are collected.
    fn visit_unresolved_object_name(&mut self, node: &'ast UnresolvedObjectName) {
        match node {
            UnresolvedObjectName::Cluster(i)
            | UnresolvedObjectName::Role(i)
            | UnresolvedObjectName::NetworkPolicy(i) => self.add(i),
            UnresolvedObjectName::Database(n) => self.add(&n.0),
            UnresolvedObjectName::Schema(n) => self.add_idents(&n.0),
            UnresolvedObjectName::Item(n) => self.add_idents(&n.0),
            UnresolvedObjectName::ClusterReplica(_) => {}
        }
    }

    // `SET cluster/database/schema = '<name>'` names an object as a string
    // literal, not an item-name AST position, so the scans above miss it. A
    // cluster/database referenced only this way (never in IN CLUSTER / CREATE /
    // DROP) would otherwise survive. Mirror the rewriter's SET handling.
    fn visit_set_variable_statement(&mut self, node: &'ast SetVariableStatement) {
        let SetVariableTo::Values(values) = &node.to else {
            return;
        };
        if matches!(
            node.variable.as_str().to_lowercase().as_str(),
            "cluster" | "database" | "schema" | "search_path"
        ) {
            for value in values {
                match value {
                    SetVariableValue::Ident(ident) => self.add(ident),
                    SetVariableValue::Literal(Value::String(name)) => self.add_str(name),
                    SetVariableValue::Literal(_) => {}
                }
            }
        }
    }

    fn visit_data_type(&mut self, node: &'ast RawDataType) {
        // Skip the type name (`Other`); only descend into element/key/value
        // types so a nested object reference (rare) is still reached.
        match node {
            RawDataType::Array(inner) | RawDataType::List(inner) => self.visit_data_type(inner),
            RawDataType::Map {
                key_type,
                value_type,
            } => {
                self.visit_data_type(key_type);
                self.visit_data_type(value_type);
            }
            RawDataType::Other { .. } => {}
        }
    }
}

/// Anonymizes one workload entry (which may hold more than one statement),
/// returning `None` if it does not parse.
fn anonymize(
    sql: &str,
    mapping: &BTreeMap<String, String>,
    keywords: &BTreeSet<String>,
    rename: bool,
    redact: bool,
    redact_numbers: bool,
    consistent_names: bool,
    salt: &str,
) -> Option<String> {
    let stmts = parser::parse_statements(sql).ok()?;
    let rewritten: Vec<String> = stmts
        .into_iter()
        .map(|parsed| {
            let mut ast = parsed.ast;
            // Per-statement renames for query-local identifiers, given opaque
            // `local_N` names. Built fresh per statement: these names are scoped
            // to the statement, so the same alias in two queries is unrelated.
            let locals: BTreeMap<String, String> = if rename {
                let mut collector = LocalNameCollector {
                    keywords,
                    mapping,
                    names: BTreeSet::new(),
                };
                collector.visit_statement(&ast);
                collector
                    .names
                    .into_iter()
                    .enumerate()
                    .map(|(i, name)| (name, format!("local_{}", i + 1)))
                    .collect()
            } else {
                BTreeMap::new()
            };
            let mut visitor = Anonymizer {
                mapping,
                locals: &locals,
                rename,
                redact: redact && !preserves_literals(&ast),
                redact_numbers,
                consistent_names,
                salt,
            };
            visitor.visit_statement_mut(&mut ast);
            ast.to_ast_string_simple()
        })
        .collect();
    let result = rewritten.join("; ");
    // Fail-safe: re-parse the rewritten SQL. If a display/round-trip quirk
    // produced something that no longer parses, return `None` so the caller
    // treats it as a hard error rather than emitting corrupt (or worse,
    // subtly-changed) SQL. Cheap relative to the rewrite, and it has caught
    // real `AstDisplay` round-trip bugs before.
    parser::parse_statements(&result).ok()?;
    Some(result)
}

fn main() -> io::Result<()> {
    let mut input = String::new();
    io::stdin().read_to_string(&mut input)?;

    let req: Request =
        serde_json::from_str(&input).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    let keywords: BTreeSet<String> = req.keywords.iter().map(|k| k.to_lowercase()).collect();

    if req.collect {
        // Collection pass: gather unmapped object names across all statements.
        // Unparseable statements are skipped here — the rewrite pass reports them.
        let mut collector = GlobalNameCollector {
            keywords: &keywords,
            mapping: &req.mapping,
            names: BTreeSet::new(),
        };
        for sql in &req.statements {
            if let Ok(stmts) = parser::parse_statements(sql) {
                for parsed in stmts {
                    collector.visit_statement(&parsed.ast);
                }
            }
        }
        let names: Vec<String> = collector.names.into_iter().collect();
        io::stdout().write_all(serde_json::to_string(&names)?.as_bytes())?;
        return Ok(());
    }

    let result: Vec<Option<String>> = req
        .statements
        .iter()
        .map(|sql| {
            anonymize(
                sql,
                &req.mapping,
                &keywords,
                req.rename_identifiers,
                req.redact_literals,
                req.redact_numbers,
                req.consistent_names,
                &req.salt,
            )
        })
        .collect();

    io::stdout().write_all(serde_json::to_string(&result)?.as_bytes())?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use mz_sql_parser::ast::visit::Visit;
    use mz_sql_parser::parser;

    use super::{GlobalNameCollector, anonymize as anonymize_impl};

    /// Run the global object-name collector over some statements.
    fn collect(sqls: &[&str]) -> Vec<String> {
        let keywords: BTreeSet<String> = ["count", "now"].iter().map(|s| s.to_string()).collect();
        let mapping = BTreeMap::new();
        let mut collector = GlobalNameCollector {
            keywords: &keywords,
            mapping: &mapping,
            names: BTreeSet::new(),
        };
        for sql in sqls {
            for parsed in parser::parse_statements(sql).expect("parses") {
                collector.visit_statement(&parsed.ast);
            }
        }
        collector.names.into_iter().collect()
    }

    /// Test wrapper with no keyword exclusions (irrelevant to most cases).
    /// `consistent_names` follows the real caller: on for DDL (no number
    /// redaction), off for queries (number redaction).
    fn anonymize(
        sql: &str,
        mapping: &BTreeMap<String, String>,
        rename: bool,
        redact: bool,
        redact_numbers: bool,
    ) -> Option<String> {
        anonymize_impl(
            sql,
            mapping,
            &BTreeSet::new(),
            rename,
            redact,
            redact_numbers,
            !redact_numbers,
            "test-salt",
        )
    }

    fn map(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // can't call foreign function `rust_psm_stack_pointer` on OS `linux`
    fn renames_table_reference() {
        // The whole point of the AST approach: an object reference in FROM is an
        // item name (an AstInfo associated type), which the old regex mangled
        // and a naive visitor skips. It must be renamed as a whole token.
        let m = map(&[("orders", "table_1")]);
        let out = anonymize("SELECT id FROM orders", &m, true, false, false).expect("parses");
        assert!(out.contains("table_1"), "{out}");
        assert!(out.contains("id"), "id is unmapped, keep it: {out}");
        assert!(!out.contains("orders"), "{out}");
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // can't call foreign function `rust_psm_stack_pointer` on OS `linux`
    fn renames_qualified_reference() {
        let m = map(&[("mydb", "db_0"), ("myschema", "schema_1"), ("t", "table_1")]);
        let out =
            anonymize("SELECT * FROM mydb.myschema.t", &m, true, false, false).expect("parses");
        assert!(out.contains("db_0.schema_1.table_1"), "{out}");
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // can't call foreign function `rust_psm_stack_pointer` on OS `linux`
    fn does_not_rename_inside_other_identifiers() {
        // The old regex would rewrite `id` inside `user_id`; the AST does not.
        let m = map(&[("id", "column_1")]);
        let out = anonymize("SELECT user_id FROM t", &m, true, false, false).expect("parses");
        assert!(out.contains("user_id"), "{out}");
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // can't call foreign function `rust_psm_stack_pointer` on OS `linux`
    fn redacts_query_literals_including_numbers() {
        let m = map(&[]);
        let out = anonymize(
            "SELECT 'secret', 42 FROM t WHERE x = 'a'",
            &m,
            false,
            true,
            true,
        )
        .expect("parses");
        assert!(!out.contains("secret"), "{out}");
        assert!(!out.contains("42"), "{out}");
        assert!(out.contains("<REDACTED>"), "{out}");
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // can't call foreign function `rust_psm_stack_pointer` on OS `linux`
    fn does_not_rename_inside_string_literals() {
        // A literal containing a word that matches a renamed identifier must not
        // be touched by renaming (it is data).
        let m = map(&[("orders", "table_1")]);
        let out = anonymize("SELECT 'orders' FROM orders", &m, true, false, false).expect("parses");
        assert!(out.contains("'orders'"), "{out}");
        assert!(out.contains("table_1"), "{out}");
    }

    #[mz_ore::test]
    fn preserves_cluster_config_literals() {
        // Cluster sizing is config replay needs; rename the name, keep the size.
        let m = map(&[("prod", "cluster_0")]);
        let out = anonymize("CREATE CLUSTER prod (SIZE = '100cc')", &m, true, true, true)
            .expect("parses");
        assert!(out.contains("'100cc'"), "size must be preserved: {out}");
        assert!(out.contains("cluster_0"), "{out}");
    }

    #[mz_ore::test]
    fn preserves_set_config_literals() {
        let m = map(&[]);
        let out = anonymize("SET statement_timeout = '5s'", &m, true, true, true).expect("parses");
        assert!(out.contains("'5s'"), "timeout must be preserved: {out}");
    }

    #[mz_ore::test]
    fn redacts_kafka_broker_and_option_strings() {
        // Broker hosts are a typed `String` field, not a `Value`; the SASL
        // username is a `Value::String` option value. Both are DDL names, so
        // they are consistently renamed (to a stable `redacted_<hash>`) — the
        // original must not survive in any form.
        let m = map(&[("kafka_conn", "conn_1")]);
        let out = anonymize(
            "CREATE CONNECTION kafka_conn TO KAFKA (BROKER 'prod.internal.acme.com:9092', SASL USERNAME 'admin')",
            &m,
            true,
            true,
            false,
        )
        .expect("parses");
        assert!(
            !out.contains("prod.internal.acme.com"),
            "broker leaked: {out}"
        );
        assert!(!out.contains("admin"), "username leaked: {out}");
        assert!(out.contains("conn_1"), "{out}");
        assert!(
            out.contains("redacted_"),
            "renamed to a stable token: {out}"
        );
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // can't call foreign function `rust_psm_stack_pointer` on OS `linux`
    fn keeps_numbers_in_ddl_but_renames_strings() {
        // DDL redaction (redact_numbers = false): a numeric default is config
        // that must stay valid for replay; a string default is renamed to a
        // stable, valid `redacted_<hash>` rather than '<REDACTED>'.
        let m = map(&[]);
        let out = anonymize(
            "CREATE TABLE t (a int DEFAULT 5, b text DEFAULT 'secret')",
            &m,
            true,
            true,
            false,
        )
        .expect("parses");
        assert!(
            out.contains("DEFAULT 5"),
            "number must be kept in DDL: {out}"
        );
        assert!(!out.contains("secret"), "string default leaked: {out}");
        assert!(out.contains("redacted_"), "{out}");
    }

    #[mz_ore::test]
    fn renames_ddl_names_consistently() {
        // The same name appearing twice in DDL must get the same replacement,
        // so replay can keep e.g. a connection database and a source's external
        // reference lined up. Two sources sharing a topic → same token.
        let m = map(&[]);
        let out = anonymize(
            "CREATE SOURCE a FROM KAFKA CONNECTION c (TOPIC 'shared') FORMAT TEXT; \
             CREATE SOURCE b FROM KAFKA CONNECTION c (TOPIC 'shared') FORMAT TEXT",
            &m,
            true,
            true,
            false,
        )
        .expect("parses");
        assert!(!out.contains("shared"), "topic leaked: {out}");
        // Both `TOPIC = '...'` values must be the identical renamed token.
        let topics: Vec<&str> = out
            .match_indices("TOPIC = '")
            .map(|(i, m)| {
                let rest = &out[i + m.len()..];
                &rest[..rest.find('\'').unwrap()]
            })
            .collect();
        assert_eq!(topics.len(), 2, "two topics expected: {out}");
        assert!(topics[0].starts_with("redacted_"), "renamed: {out}");
        assert_eq!(topics[0], topics[1], "same token reused: {out}");
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // can't call foreign function `rust_psm_stack_pointer` on OS `linux`
    fn renames_and_redacts_inside_prepared_statement() {
        // PREPARE/DECLARE carry their inner query as an AstInfo associated type
        // (`NestedStatement`) the generic visitor treats as opaque; without the
        // override the whole inner query — identifiers and literals — leaked.
        let m = map(&[("orders", "table_1")]);
        let out = anonymize(
            "PREPARE p AS SELECT id FROM orders WHERE note = 'secret' AND id = 42",
            &m,
            true,
            true,
            true,
        )
        .expect("parses");
        assert!(out.contains("table_1"), "{out}");
        assert!(!out.contains("orders"), "{out}");
        assert!(!out.contains("secret"), "{out}");
        assert!(!out.contains("42"), "{out}");
    }

    #[mz_ore::test]
    fn anonymizes_inline_avro_schema_consistently() {
        // The Avro SEED schema defines the source's columns and is what replay
        // decodes against, so it must stay valid JSON AND its column field names
        // must match the renamed columns. A data field that is a captured column
        // (`width`) is renamed to its mapping name; a structural field the
        // decoder relies on (`before`) and the record name (`Envelope`) stay; the
        // upstream `namespace` is scrubbed so it cannot leak.
        let m = map(&[("width", "column_5"), ("csr", "conn_1")]);
        let out = anonymize(
            "CREATE SOURCE s FROM KAFKA CONNECTION c (TOPIC 't') \
             FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr \
             SEED VALUE SCHEMA '{\"type\":\"record\",\"name\":\"Envelope\",\
             \"namespace\":\"acme.inventory\",\"fields\":[\
             {\"name\":\"width\",\"type\":\"long\"},\
             {\"name\":\"before\",\"type\":\"string\"}]}'",
            &m,
            true,
            true,
            false,
        )
        .expect("parses");
        assert!(out.contains("column_5"), "column field renamed: {out}");
        assert!(!out.contains("width"), "original field name gone: {out}");
        assert!(out.contains("before"), "structural field kept: {out}");
        assert!(out.contains("Envelope"), "record name kept: {out}");
        assert!(!out.contains("acme"), "namespace scrubbed: {out}");
        assert!(out.contains("conn_1"), "csr connection renamed: {out}");
        // Still valid JSON.
        let schema_start = out.find("SCHEMA '").expect("seed") + "SCHEMA '".len();
        let schema = &out[schema_start..out[schema_start..].find('\'').unwrap() + schema_start];
        serde_json::from_str::<serde_json::Value>(schema).expect("valid json");
    }

    #[mz_ore::test]
    fn keeps_sink_compression_type_but_redacts_topic() {
        // COMPRESSION TYPE is a fixed enum the replayed sink DDL needs valid, so
        // it is kept; the TOPIC is sensitive and is still consistently renamed.
        let m = map(&[]);
        let out = anonymize(
            "CREATE SINK s FROM t INTO KAFKA CONNECTION c \
             (TOPIC 'prod-orders', COMPRESSION TYPE 'gzip') FORMAT JSON",
            &m,
            true,
            true,
            false,
        )
        .expect("parses");
        assert!(out.contains("'gzip'"), "compression type kept: {out}");
        assert!(!out.contains("prod-orders"), "topic redacted: {out}");
    }

    #[mz_ore::test]
    fn salt_changes_redacted_token() {
        // The `redacted_<hash>` token is salted: the same input under different
        // salts yields different tokens (so an offline dictionary attack on the
        // shared output cannot recover a low-entropy value like a region code),
        // while staying stable under one salt (consistent renaming within a run).
        let m = map(&[]);
        let sql = "CREATE SOURCE s FROM KAFKA CONNECTION c (TOPIC 'EMEA') FORMAT TEXT";
        let kw = BTreeSet::new();
        let a = anonymize_impl(sql, &m, &kw, true, true, false, true, "salt-a").unwrap();
        let b = anonymize_impl(sql, &m, &kw, true, true, false, true, "salt-b").unwrap();
        let a_again = anonymize_impl(sql, &m, &kw, true, true, false, true, "salt-a").unwrap();
        assert!(!a.contains("EMEA") && !b.contains("EMEA"), "topic redacted");
        assert_ne!(a, b, "different salts must yield different tokens: {a}");
        assert_eq!(a, a_again, "same salt must be stable");
    }

    #[mz_ore::test]
    fn keeps_load_generator_reference() {
        // `mz_load_generators.<gen>.<table>` names a built-in load-generator
        // output and must be kept whole, even though `accounts` would otherwise
        // be renamed as a colliding user identifier.
        let m = map(&[("accounts", "table_3")]);
        let out = anonymize(
            "CREATE SUBSOURCE sub (a int) OF SOURCE src \
             WITH (EXTERNAL REFERENCE = mz_load_generators.auction.accounts)",
            &m,
            true,
            true,
            false,
        )
        .expect("parses");
        assert!(
            out.contains("mz_load_generators.auction.accounts"),
            "load-generator reference kept whole: {out}"
        );
    }

    #[mz_ore::test]
    fn keeps_interval_config_values() {
        // Durations are non-sensitive config (load-generator tick intervals,
        // refresh/retention windows); they must survive verbatim so replay can
        // parse them, not be hashed into something that is no longer an interval.
        let m = map(&[]);
        let out = anonymize(
            "CREATE SOURCE s FROM LOAD GENERATOR COUNTER (TICK INTERVAL '500ms')",
            &m,
            true,
            true,
            false,
        )
        .expect("parses");
        assert!(out.contains("500ms"), "interval must be kept: {out}");
        assert!(!out.contains("redacted_"), "{out}");
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // can't call foreign function `rust_psm_stack_pointer` on OS `linux`
    fn keeps_query_date_field_but_redacts_query_duration() {
        // In a query (consistent_names = false, redact_numbers = true) a datetime
        // field is still a specifier and must be kept (else date_trunc/extract
        // break on replay), but a duration-shaped data value is redacted.
        let m = map(&[]);
        let out = anonymize(
            "SELECT date_trunc('hour', ts) FROM t WHERE note = '5 minutes'",
            &m,
            true,
            true,
            true, // query: consistent_names = false
        )
        .expect("parses");
        assert!(out.contains("'hour'"), "query date field kept: {out}");
        assert!(
            !out.contains("5 minutes"),
            "query duration data redacted: {out}"
        );
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // can't call foreign function `rust_psm_stack_pointer` on OS `linux`
    fn redacts_bare_unit_data_but_keeps_date_fields() {
        // A short unit-shaped data value in a DDL body (`flag = 'y'`) is data and
        // must be redacted, but a bare datetime field used by `date_trunc` is
        // kept — redacting it would make the expression invalid.
        let m = map(&[]);
        let out = anonymize(
            "CREATE VIEW v AS SELECT date_trunc('year', ts) FROM t WHERE flag = 'y'",
            &m,
            true,
            true,
            false, // DDL: consistent_names = true
        )
        .expect("parses");
        assert!(out.contains("'year'"), "date_trunc field kept: {out}");
        assert!(
            !out.contains("'y'"),
            "bare unit-shaped data redacted: {out}"
        );
    }

    #[mz_ore::test]
    fn is_number_interval_requires_magnitude_and_unit() {
        use super::is_number_interval;
        // Genuine durations: a magnitude plus a unit, or a colon-time word.
        assert!(is_number_interval("5 minutes"));
        assert!(is_number_interval("500ms"));
        assert!(is_number_interval("0.1s"));
        assert!(is_number_interval("1 day"));
        assert!(is_number_interval("2 hours 30 minutes"));
        assert!(is_number_interval("00:05:00"));
        // Bare numbers are data (SSNs, card/account numbers, ...), NOT durations.
        assert!(!is_number_interval("123456789"));
        assert!(!is_number_interval("555 1234"));
        assert!(!is_number_interval("4111111111111111"));
        // A bare unit with no magnitude is also data.
        assert!(!is_number_interval("minutes"));
        assert!(!is_number_interval("y"));
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // can't call foreign function `rust_psm_stack_pointer` on OS `linux`
    fn redacts_numeric_string_literal_in_body() {
        // A view/MV/index body is anonymized with consistent_names AND
        // redact_numbers (unlike a query, which has consistent_names off). A
        // quoted numeric-string predicate there is data (an SSN, account id, or
        // card number) and must be redacted, not mistaken for a bare-number
        // "interval" and kept verbatim.
        let m = map(&[("customers", "table_1"), ("ssn", "column_1")]);
        let out = anonymize_impl(
            "CREATE VIEW v AS SELECT * FROM customers WHERE ssn = '123456789'",
            &m,
            &BTreeSet::new(),
            true,
            true,
            true, // redact_numbers
            true, // consistent_names (the body path)
            "test-salt",
        )
        .expect("parses");
        assert!(
            !out.contains("123456789"),
            "numeric-string data leaked: {out}"
        );
        assert!(out.contains("column_1"), "{out}");
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // can't call foreign function `rust_psm_stack_pointer` on OS `linux`
    fn keeps_retain_history_duration_in_body() {
        // An MV's RETAIN HISTORY duration is a `Value::String` config value (it
        // carries a unit) that replay needs intact. The body path keeps it even
        // though it also redact_numbers, so it must survive the numeric-string
        // tightening. (Anonymizing bodies in query mode would wrongly redact it.)
        let m = map(&[("mv", "view_1")]);
        let out = anonymize_impl(
            "CREATE MATERIALIZED VIEW mv WITH (RETAIN HISTORY = FOR '7 days') AS SELECT 1",
            &m,
            &BTreeSet::new(),
            true,
            true,
            true,
            true,
            "test-salt",
        )
        .expect("parses");
        assert!(
            out.contains("'7 days'"),
            "retain-history duration must be kept: {out}"
        );
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // can't call foreign function `rust_psm_stack_pointer` on OS `linux`
    fn renames_query_local_identifiers() {
        // CTE names, column aliases, and constraint names are not captured
        // objects (not in the mapping) but can embed sensitive substrings. They
        // must be renamed (consistently: a CTE definition and its references) so
        // nothing recognizable survives.
        let m = map(&[("orders", "table_1")]);
        let out = anonymize(
            "WITH acme_cte AS (SELECT id AS acme_total FROM orders) \
             SELECT acme_total FROM acme_cte",
            &m,
            true,
            true,
            true,
        )
        .expect("parses");
        assert!(
            !out.contains("acme"),
            "local identifiers must be renamed: {out}"
        );
        assert!(out.contains("table_1"), "{out}");
        // The CTE name and its reference rename to the same token.
        assert!(out.contains("local_"), "{out}");
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // can't call foreign function `rust_psm_stack_pointer` on OS `linux`
    fn keeps_builtins_in_local_positions() {
        // A column alias that shadows a builtin must NOT be renamed (it is in the
        // keyword set), so functions/types keep working.
        let kw: BTreeSet<String> = ["count"].iter().map(|s| s.to_string()).collect();
        let m = map(&[]);
        let out = anonymize_impl(
            "SELECT count(*) AS count FROM t",
            &m,
            &kw,
            true,
            false,
            false,
            false,
            "test-salt",
        )
        .expect("parses");
        assert!(out.contains("count(*)"), "builtin must be intact: {out}");
        assert!(!out.contains("local_"), "{out}");
    }

    #[mz_ore::test]
    fn renames_set_cluster_and_redacts_application_name() {
        // `SET cluster/database` values are object references (rename them);
        // `application_name` is free-form and tools embed host/db names (redact).
        let m = map(&[("prod_cluster", "cluster_0")]);
        let out = anonymize("SET cluster = 'prod_cluster'", &m, true, true, true).expect("parses");
        assert!(
            out.contains("cluster_0") && !out.contains("prod_cluster"),
            "{out}"
        );

        let out = anonymize(
            "SET application_name = 'tool <db>'",
            &map(&[]),
            true,
            true,
            true,
        )
        .expect("parses");
        assert!(!out.contains("tool") && !out.contains("<db>"), "{out}");
        assert!(out.contains("<REDACTED>"), "{out}");

        // Other settings are non-sensitive config and preserved for replay.
        let out =
            anonymize("SET statement_timeout = '5s'", &map(&[]), true, true, true).expect("parses");
        assert!(out.contains("'5s'"), "config must be preserved: {out}");
    }

    #[mz_ore::test]
    fn preserves_config_map_and_redacts_header_key() {
        let m = map(&[]);
        // TOPIC CONFIG is non-sensitive Kafka config and must be preserved
        // verbatim; the sink TOPIC and the INCLUDE HEADER key are redacted.
        let sink = anonymize(
            "CREATE SINK s FROM t INTO KAFKA CONNECTION c \
             (TOPIC 'topicname', TOPIC CONFIG = MAP['retention.ms' => '259200000'])",
            &m,
            false,
            true,
            false,
        )
        .expect("parses");
        assert!(
            sink.contains("MAP['retention.ms' => '259200000']"),
            "config map must be preserved: {sink}"
        );
        assert!(
            !sink.contains("topicname"),
            "topic must be redacted: {sink}"
        );

        let source = anonymize(
            "CREATE SOURCE s FROM KAFKA CONNECTION c (TOPIC 't') INCLUDE HEADER 'hdr-name' AS h",
            &m,
            false,
            true,
            false,
        )
        .expect("parses");
        assert!(
            !source.contains("hdr-name"),
            "header key must be redacted: {source}"
        );
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // can't call foreign function `rust_psm_stack_pointer` on OS `linux`
    fn collect_finds_window_object_names() {
        // Objects absent from the catalog snapshot — DROP SCHEMA/CLUSTER targets
        // (route through visit_unresolved_object_name), FROM references, and a
        // cluster named only in `SET cluster = '...'`.
        let names = collect(&[
            "DROP SCHEMA mydb.dev_schema CASCADE",
            "DROP CLUSTER IF EXISTS deploy_cluster CASCADE",
            "SELECT * FROM window_view",
            "SET cluster = 'serve_cluster'",
        ]);
        for expected in [
            "dev_schema",
            "deploy_cluster",
            "window_view",
            "serve_cluster",
        ] {
            assert!(
                names.iter().any(|n| n == expected),
                "missing {expected}: {names:?}"
            );
        }
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // can't call foreign function `rust_psm_stack_pointer` on OS `linux`
    fn collect_skips_builtins_and_types() {
        // Builtins (via the keyword set) and type names must never be collected,
        // or the rewrite would corrupt function/type references.
        let names = collect(&["SELECT count(*), now() FROM t WHERE x::bigint > 0"]);
        assert!(
            names.iter().any(|n| n == "t"),
            "table ref expected: {names:?}"
        );
        assert!(
            !names.iter().any(|n| n == "bigint"),
            "type leaked: {names:?}"
        );
        assert!(
            !names.iter().any(|n| n == "count"),
            "builtin leaked: {names:?}"
        );
    }

    #[mz_ore::test]
    fn returns_none_on_parse_error() {
        assert_eq!(
            anonymize("SELEC not valid", &map(&[]), true, true, true),
            None
        );
    }
}
