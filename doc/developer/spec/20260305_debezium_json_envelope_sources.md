Created using https://github.com/bmad-code-org/BMAD-METHOD
---
title: 'Debezium JSON Envelope Support for Sources'
slug: 'debezium-json-envelope-sources'
created: '2026-03-05'
status: 'ready-for-dev'
stepsCompleted: [1, 2, 3, 4]
tech_stack: [rust, kafka, json, debezium, serde_json, mz_repr, mz_interchange, mz_storage, mz_sql, mz_sql_parser, mz_storage_types]
files_to_modify:
  - src/sql-parser/src/ast/defs/ddl.rs
  - src/sql-parser/src/parser.rs
  - src/sql/src/plan/statement/ddl.rs
  - src/storage-types/src/sources/envelope.rs
  - src/storage/src/render/sources.rs
  - test/testdrive/kafka-debezium-json-sources.td
code_patterns:
  - UpsertStyle enum variant for envelope style selection
  - typecheck_debezium for planning-time validation
  - upsert_commands match arm for render-time extraction
  - PreDelimitedFormat::Json for JSON byte decoding
  - DecodeResult { key, value, metadata } pipeline
test_patterns:
  - testdrive with kafka-ingest format=bytes for JSON payloads
  - key-value Kafka messages with separate key and value
  - SELECT verification of final materialized output
  - Insert/Update/Delete operation coverage
---

# Tech-Spec: Debezium JSON Envelope Support for Sources

**Created:** 2026-03-05

## Overview

### Problem Statement

Materialize only supports Debezium with Avro-encoded messages. Users ingesting change data from TiCDC (or other Debezium-compatible producers that emit JSON rather than Avro) cannot use `ENVELOPE DEBEZIUM`. The validation in `src/sql/src/plan/statement/ddl.rs` explicitly requires `VALUE FORMAT AVRO` for Debezium envelope, blocking JSON-based CDC pipelines.

### Solution

Lift the Avro-only restriction on `ENVELOPE DEBEZIUM` to also accept `VALUE FORMAT JSON`. Add a JSON-based Debezium envelope decoder that extracts `before`/`after`/`op` fields from the Debezium JSON payload and maps operations to upsert semantics. Support both generic Debezium JSON and TiCDC's dialect via an extensible `MODE` parameter.

### Scope

**In Scope:**
- `VALUE FORMAT JSON, ENVELOPE DEBEZIUM` with `KEY FORMAT JSON` on Kafka sources
- JSON Debezium envelope parsing (extract `before`/`after`/`op` from `payload`)
- TiCDC dialect via `MODE` parameter (e.g., `ENVELOPE DEBEZIUM (MODE = 'TICDC')`)
- Insert (`c`), update (`u`), delete (`d`) operation handling
- Output columns: `data` (jsonb, the "after" payload) and `envelope` (jsonb, the full Debezium envelope for CDC metadata access)
- Support both `payload`-wrapped and flat envelope formats
- Error handling for malformed Debezium JSON messages

**Out of Scope:**
- Schema-aware column projection from JSON payloads
- Keyless / upsert-from-payload sources
- `commit_ts`-based ordering from TiCDC source metadata (note: `commit_ts` is preserved in the `envelope` column for user queries, but not used for ordering)
- Spatial type handling for TiCDC

## Context for Development

### Codebase Patterns

**Avro Debezium Pipeline (existing):**
1. Avro decoder produces a Row with typed columns: `before` (Record, nullable), `after` (Record), `op` (String), `source` (Record)
2. `typecheck_debezium(&value_desc)` at planning time finds the column index of `after` in the RelationDesc → `after_idx`
3. Planning creates `UpsertStyle::Debezium { after_idx }` → wrapped in `UpsertEnvelope`
4. At render time, `upsert_commands()` in `sources.rs:549-560` does `row.iter().nth(after_idx)` to get `Datum::List(after)`, unpacks the record fields, appends metadata

**JSON Decode Pipeline (existing, no Debezium):**
1. Raw bytes → `PreDelimitedFormat::Json` → `Jsonb::from_slice(bytes)` → `Row` with single `data: Jsonb` column
2. No envelope extraction — the entire JSON blob becomes the `data` column

**Key Architectural Insight:**
For JSON + Debezium, the JSON decoder still produces a single Jsonb datum containing the ENTIRE Debezium envelope. The `typecheck_debezium()` path won't work because there's no "after" column in the RelationDesc — just a `data: Jsonb` column. Envelope extraction must happen in `upsert_commands()` via the new `UpsertStyle::DebeziumJson` variant, which parses the Jsonb value to extract `op` and `after` at render time.

**Envelope Extraction Location:** `src/storage/src/render/sources.rs` in `upsert_commands()` — same function where Avro Debezium extraction happens. New match arm for `UpsertStyle::DebeziumJson`:
1. Take the single Jsonb datum from the decoded row
2. Parse JSON to navigate to `payload.after` (or `after` for flat format) and `payload.op` (or `op`)
3. For op `d` → return `None` (delete retraction)
4. For op `c`/`u` → re-serialize `after` value as Jsonb datum, return as the row

**Error Handling Pattern:** Existing upsert sources surface decode errors through the source's error collection (see `DecodeResult` with `value: Option<Result<Row, DecodeError>>`). Malformed Debezium JSON should produce `Err(DecodeError)` values through the same mechanism.

### Files to Reference

| File | Purpose |
| ---- | ------- |
| `src/sql-parser/src/ast/defs/ddl.rs:636-644` | `SourceEnvelope` AST enum — `Debezium` variant needs MODE parameter |
| `src/sql-parser/src/parser.rs:2362-2398` | `parse_source_envelope()` — parse MODE option after DEBEZIUM keyword |
| `src/sql/src/plan/statement/ddl.rs:1394-1412` | Debezium planning — lift Avro-only gate, handle JSON path |
| `src/sql/src/plan/statement/ddl.rs:2212-2229` | `typecheck_debezium()` — Avro-specific, bypass for JSON |
| `src/sql/src/plan/statement/ddl.rs:2247-2255` | KEY FORMAT requirement validation — already covers Debezium |
| `src/storage-types/src/sources/envelope.rs:70-84` | `UpsertStyle` enum — add `DebeziumJson` variant |
| `src/storage-types/src/sources/envelope.rs:230-255` | `UnplannedSourceEnvelope` desc transform — add JSON path |
| `src/storage-types/src/sources/encoding.rs:92-100` | `DataEncoding` enum — `Json` variant already exists |
| `src/storage/src/decode.rs:208-227` | `PreDelimitedFormat::Json` — no changes needed, raw JSON decode stays as-is |
| `src/storage/src/render/sources.rs:549-560` | `upsert_commands()` Debezium match arm — add DebeziumJson arm |
| `src/interchange/src/envelopes.rs:141-165` | `dbz_envelope()` / `dbz_format()` — reference for envelope structure |
| `test/testdrive/kafka-upsert-debezium-sources.td` | Existing Avro Debezium test — pattern to follow for JSON tests |

### Technical Decisions

**DD-1: Single jsonb output column**
- **Decision:** The decoded "after" payload is emitted as a single `jsonb` column (consistent with how `VALUE FORMAT JSON` works today without Debezium).
- **Alternative considered:** Schema-aware column projection where users define typed columns in the CREATE SOURCE statement. This would require JSON-to-datum type coercion and schema validation. Deferred as a future enhancement.
- **Note:** This pushes schema validation entirely to query time. Users will use `data->>'field_name'` for column access with no type checking at ingestion. Acceptable for V1.

**DD-2: Require KEY FORMAT JSON**
- **Decision:** `KEY FORMAT JSON` is required for Debezium JSON sources, consistent with how Debezium Avro requires a key schema.
- **Open question:** Whether keyless sources with upsert-from-payload should be supported. To be revisited.

**DD-3: Kafka offset ordering (not commit_ts)**
- **Decision:** Use Kafka offset ordering for message sequencing, same as existing Debezium Avro sources.
- **Open question:** Whether TiCDC's `commit_ts` should be used for ordering to better reflect TiDB's distributed transaction semantics. To be revisited.

**DD-4: Extensible MODE parameter for dialect selection**
- **Decision:** Support both generic Debezium JSON and TiCDC's flavor via an extensible `MODE` parameter on the `ENVELOPE DEBEZIUM` clause. Default mode is generic Debezium. `MODE = 'TICDC'` selects TiCDC-specific handling (e.g., base64 binary encoding, float64 decimals).
- **Rationale (Party Mode):** A `MODE` parameter is more extensible than a boolean toggle. Future dialects (Maxwell, Canal, etc.) can be added without breaking syntax.

**DD-5: New UpsertStyle::DebeziumJson variant**
- **Decision:** Introduce a new `UpsertStyle::DebeziumJson` variant rather than reusing the existing `UpsertStyle::Debezium { after_idx }`. The Avro variant's `after_idx` is schema-index-based and not applicable to JSON, which extracts `payload.after` by key name from the Jsonb datum at render time.
- **Rationale (Party Mode):** Keeps Avro and JSON code paths cleanly separated, avoids overloading the existing variant with conditional logic.

**DD-6: Support both payload-wrapped and flat envelope formats**
- **Decision:** The decoder handles both `{ "payload": { "before": ..., "after": ..., "op": ... } }` (standard Debezium with schema) and flat `{ "before": ..., "after": ..., "op": ... }` (Debezium with `debezium-disable-schema=true` or similar configurations).
- **Rationale (Party Mode):** Some Debezium configurations strip the `payload` wrapper. Supporting both avoids a common production gotcha.

**DD-7: Error handling for malformed messages**
- **Decision:** Malformed Debezium JSON messages (missing `op`, unexpected `op` values, null `after` on insert, unparseable JSON) should be handled consistently with Materialize's existing error policy for upsert sources — surface errors through the source's error collection (`DecodeError`) rather than silently dropping records.
- **Rationale (Party Mode):** JSON is untyped, so malformed messages are a high-risk area. Explicit error handling must be in scope, not deferred.

**DD-8: Envelope extraction in upsert_commands (same location as Avro)**
- **Decision:** JSON Debezium envelope extraction happens in `upsert_commands()` in `src/storage/src/render/sources.rs`, the same function where Avro Debezium extraction occurs. The new `UpsertStyle::DebeziumJson` match arm parses the Jsonb datum to extract `op` and `after`, rather than using schema-based column indexing.
- **Rationale:** Keeps all envelope extraction logic co-located. The JSON path parses at render time (slightly different from Avro's index-based approach) but the upsert semantics (key + value → insert/update/delete) remain identical.

**DD-9: Include key as separate output column (resolves key_indices correctness) [F1, F3, F5, F6]**
- **Decision:** Unlike Avro Debezium (which embeds key fields inside the unpacked `after` Record), JSON Debezium includes the key as a separate `key: Jsonb` column prepended to the output row. The output schema is `[key: Jsonb, data: Jsonb, ...metadata]`. `key_indices` points to position `[0]` (the key column). This is necessary because: (a) `upsert_core` uses `UpsertKey::from_value(value_ref, &key_indices)` to reconstruct keys from persisted values during rehydration — empty/wrong `key_indices` would cause data corruption after restart; (b) the `data: Jsonb` column is opaque so `match_key_indices` cannot find named key fields within it; (c) users need access to key fields since they can't be extracted from the opaque `after` payload without knowing the schema.
- **Consequence:** `INCLUDE KEY` rejection logic (which fires for Avro Debezium because "Debezium values include all keys") must NOT fire for JSON Debezium — the key is always included as a separate column. Update the validation at `ddl.rs:1372-1384` accordingly.
- **To revisit:** Whether the key column should be named `key` (single Jsonb blob) or flattened into multiple columns if the key JSON has multiple top-level fields.

**DD-10: Parse-extract-reserialize performance cost [F12]**
- **Decision:** Every JSON Debezium message is parsed from `Jsonb` → `serde_json::Value`, the `after` field is extracted, and then re-serialized back to `Jsonb`. This is inherent to the approach of extracting envelope fields from an opaque JSON blob at render time.
- **Risk:** Additional CPU cost per message compared to Avro's index-based extraction. For V1 this is acceptable — JSON sources are already doing full JSON parsing in the decode layer.
- **Mitigation:** Profile under realistic throughput before GA. A future optimization could parse the raw bytes directly in the decode layer (before Jsonb conversion) to avoid the double-parse, but this would move envelope extraction out of `upsert_commands()` and diverge from DD-8.
- **To revisit:** Establish throughput benchmarks before shipping to production.

**DD-11: No ingestion-time schema validation for JSON [F12]**
- **Decision:** Unlike Avro Debezium (which validates `before`/`after` column types at planning time via `typecheck_debezium()`), JSON Debezium performs no structural validation at planning time. The value is an opaque `Jsonb` blob. Malformed `after` payloads (e.g., unexpected nested structures, wrong types) are not caught until query time when users access fields via `->>`/`->>` operators.
- **Risk:** Users may not discover data quality issues until downstream queries fail or return unexpected results.
- **Mitigation:** Envelope-level validation (missing `op`, null `after` on insert) is caught at render time per DD-7. Payload-content validation is deferred to future schema-aware column projection work (out of scope).

**DD-12: Handle `"r"` (read/snapshot) operation as insert [F2]**
- **Decision:** Debezium emits `op: "r"` for snapshot records (initial table scan). TiCDC and standard Debezium both produce these during initial sync. Treat `"r"` identically to `"c"` (create) — extract the `after` field and upsert the row.
- **Rationale:** Rejecting `"r"` would make any source unusable on tables that weren't empty when snapshotting began.
- **To revisit:** Whether snapshot-specific metadata should be exposed or handled differently.

**DD-13: Silently skip `"t"` (truncate) operations [F8]**
- **Decision:** Debezium can emit `op: "t"` for truncate events. These are silently skipped (not errored) because Materialize has no mechanism to bulk-retract all rows for a key range from within the upsert pipeline. A warning-level log message is emitted when a truncate event is encountered.
- **To revisit:** Whether truncate support should be added in a future iteration (e.g., by retracting all known keys for the source).

**DD-14: Null Kafka message values treated as deletes [F9]**
- **Decision:** On Kafka compacted topics, null-value records serve as tombstones. When the entire Kafka message value is null (not a JSON object with `op: "d"`, but literally absent), treat it as a delete for the given key. This is consistent with how `ENVELOPE UPSERT` handles null values in the existing decode pipeline (`result.value: None` at the `upsert_commands` level already maps to retraction).
- **Rationale:** This requires no new code — the existing `result.value: None` path in `upsert_commands` already handles this before the `UpsertStyle` match arm is reached.

**DD-15: TiCDC MODE is structural-only in V1 [F10]**
- **Decision:** The `MODE = 'TICDC'` parameter is parsed, stored, and round-trips through SQL display, but V1 runtime behavior is identical to generic mode. This establishes the syntax and plumbing for future TiCDC-specific handling (base64 binary, float64 decimals) without shipping dead behavioral code.
- **Rationale:** The parsing/storage cost is minimal and avoids a breaking syntax change later. The test for TiCDC mode (Task 8) verifies the plumbing works.
- **To revisit:** Define concrete V1 behavioral differences or defer MODE entirely if the plumbing cost is deemed not worth it during implementation.

**DD-16: Multi-partition ordering is a known constraint [F11]**
- **Decision:** For multi-partition Kafka topics, updates for the same key on different partitions can arrive out of order. This is the same limitation as existing Avro Debezium sources and is inherent to Kafka's per-partition ordering guarantee. TiCDC distributes events across partitions, so this is particularly relevant for the TiCDC use case.
- **Known constraint:** Users must ensure same-key events are routed to the same partition (standard Kafka key-based partitioning) for correct ordering.
- **To revisit:** Whether `commit_ts`-based ordering (DD-3) could resolve cross-partition ordering for TiCDC.

**DD-17: Pseudocode is illustrative, not compilable [F7, F13]**
- **Decision:** All pseudocode in this spec (particularly `extract_debezium_json` and the `upsert_commands` match arm) is illustrative of the intended logic, not compilable Rust. Implementation must handle Rust-specific concerns: (a) `Datum::Jsonb` wraps a borrowed `JsonbRef<'a>` — extracting and re-packing requires allocating into a `Row` via `RowPacker`; (b) the nested match structure in Task 5 should be simplified during implementation; (c) error types must use the concrete `DecodeErrorKind` variants available in the codebase.
- **Rationale:** Spec pseudocode communicates intent. The implementing developer is expected to adapt to Rust's ownership/lifetime model.

## Implementation Plan

### Tasks

- [ ] **Task 1: Add MODE parameter to SourceEnvelope AST**
  - File: `src/sql-parser/src/ast/defs/ddl.rs`
  - Action: Extend `SourceEnvelope::Debezium` from a unit variant to a struct variant with an optional `mode` field. Define a `DebeziumMode` enum with variants `None` (generic) and `TiCdc`.
    ```rust
    pub enum DebeziumMode { None, TiCdc }
    pub enum SourceEnvelope {
        // ...
        Debezium { mode: DebeziumMode },
        // ...
    }
    ```
  - Action: Update the `AstDisplay` impl for `SourceEnvelope::Debezium` to render `DEBEZIUM` or `DEBEZIUM (MODE = 'TICDC')`.
  - Action: Update `requires_all_input()` match arm accordingly.
  - Notes: All existing references to `SourceEnvelope::Debezium` across the codebase will need pattern updates (e.g., `SourceEnvelope::Debezium { .. }`). Search for all match arms.

- [ ] **Task 2: Parse MODE option in parse_source_envelope**
  - File: `src/sql-parser/src/parser.rs`
  - Action: In `parse_source_envelope()` (line ~2365), after matching the `DEBEZIUM` keyword, optionally parse `(MODE = '<ident>')`. If present, map `'TICDC'` to `DebeziumMode::TiCdc`. If absent, default to `DebeziumMode::None`.
    ```rust
    } else if self.parse_keyword(DEBEZIUM) {
        let mode = if self.consume_token(&Token::LParen) {
            self.expect_keyword(MODE)?;
            self.expect_token(&Token::Eq)?;
            let mode_str = self.parse_literal_string()?;
            self.expect_token(&Token::RParen)?;
            match mode_str.to_uppercase().as_str() {
                "TICDC" => DebeziumMode::TiCdc,
                other => return Err(/* unsupported mode error */),
            }
        } else {
            DebeziumMode::None
        };
        SourceEnvelope::Debezium { mode }
    }
    ```
  - Notes: The `MODE` keyword may need to be added to the keyword list if not already present.

- [ ] **Task 3: Add UpsertStyle::DebeziumJson variant**
  - File: `src/storage-types/src/sources/envelope.rs`
  - Action: Add a new variant to `UpsertStyle`:
    ```rust
    pub enum UpsertStyle {
        Default(KeyEnvelope),
        Debezium { after_idx: usize },
        DebeziumJson { mode: DebeziumJsonMode },
        ValueErrInline { key_envelope: KeyEnvelope, error_column: String },
    }
    ```
  - Action: Define `DebeziumJsonMode` enum (or reuse from AST layer via a storage-types equivalent):
    ```rust
    pub enum DebeziumJsonMode { Generic, TiCdc }
    ```
  - Notes: Must derive `Clone, Debug, Serialize, Deserialize, Eq, PartialEq` to match existing variants.

- [ ] **Task 4: Add UnplannedSourceEnvelope descriptor transform for DebeziumJson**
  - File: `src/storage-types/src/sources/envelope.rs`
  - Action: In the `UnplannedSourceEnvelope` match that builds the output `RelationDesc` (line ~230), add a new arm for `UpsertStyle::DebeziumJson`. Per DD-9, the output schema is `[key: Jsonb, data: Jsonb, envelope: Jsonb, ...metadata]`:
    ```rust
    UnplannedSourceEnvelope::Upsert {
        style: UpsertStyle::DebeziumJson { .. },
    } => {
        // Key is included as a separate column (unlike Avro Debezium where keys are in the after record)
        let mut desc = RelationDesc::builder()
            .with_column("key", ScalarType::Jsonb.nullable(false))
            .with_column("data", ScalarType::Jsonb.nullable(false))
            .finish();
        // key_indices = [0] — points to the key column for UpsertKey::from_value rehydration
        let key_indices = vec![0usize];
        desc = desc.with_key(key_indices.clone());
        let desc = desc.concat(metadata_desc);
        (
            self.into_source_envelope(Some(key_indices), None, Some(desc.arity())),
            desc,
        )
    }
    ```
  - Notes: This approach ensures `upsert_core` can reconstruct keys from persisted values during rehydration via `UpsertKey::from_value(value_ref, &key_indices)`. The key column at index 0 contains the JSON key blob. Skip `match_key_indices` entirely for this path.

- [ ] **Task 5: Lift Avro-only gate in DDL planning**
  - File: `src/sql/src/plan/statement/ddl.rs`
  - Action: Modify the `ast::SourceEnvelope::Debezium` match arm (lines 1394-1412) to handle JSON encoding separately:
    ```rust
    ast::SourceEnvelope::Debezium { mode } => {
        match encoding.as_ref().map(|e| &e.value) {
            Some(DataEncoding::Json) => {
                // JSON path: skip typecheck_debezium, use DebeziumJson style
                let storage_mode = match mode {
                    DebeziumMode::TiCdc => DebeziumJsonMode::TiCdc,
                    DebeziumMode::None => DebeziumJsonMode::Generic,
                };
                UnplannedSourceEnvelope::Upsert {
                    style: UpsertStyle::DebeziumJson { mode: storage_mode },
                }
            }
            _ => {
                // Existing Avro path (unchanged)
                let after_idx = match typecheck_debezium(&value_desc) {
                    Ok((_before_idx, after_idx)) => Ok(after_idx),
                    Err(type_err) => match encoding.as_ref().map(|e| &e.value) {
                        Some(DataEncoding::Avro(_)) => Err(type_err),
                        _ => Err(sql_err!(
                            "ENVELOPE DEBEZIUM requires that VALUE FORMAT is set to AVRO or JSON"
                        )),
                    },
                }?;
                UnplannedSourceEnvelope::Upsert {
                    style: UpsertStyle::Debezium { after_idx },
                }
            }
        }
    }
    ```
  - Action: Update all other match arms referencing `ast::SourceEnvelope::Debezium` to use `ast::SourceEnvelope::Debezium { .. }` pattern (lines 810, 1372-1384, and any others found by the compiler).
  - Action: Per DD-9, update `INCLUDE KEY` rejection logic at `ddl.rs:1372-1384` — the "Cannot use INCLUDE KEY with ENVELOPE DEBEZIUM" error must NOT fire when `DataEncoding::Json` is used. For JSON Debezium, key is always included as a separate column.
  - Notes: The error message changes from "AVRO" to "AVRO or JSON". KEY FORMAT validation at line 2247 already covers Debezium and requires no changes.

- [ ] **Task 6: Implement DebeziumJson extraction in upsert_commands**
  - File: `src/storage/src/render/sources.rs`
  - Action: Add a new match arm in `upsert_commands()` alongside the existing `UpsertStyle::Debezium { after_idx }` arm (line ~549):
    ```rust
    UpsertStyle::DebeziumJson { mode } => {
        // The row contains a single Jsonb datum with the full Debezium envelope
        let jsonb_datum = row.iter().next().unwrap();
        match extract_debezium_json(jsonb_datum, mode) {
            Ok(Some(after_jsonb_row)) => {
                // Per DD-9: prepend key, then after payload, then metadata
                let mut packer = row_buf.packer();
                packer.extend(key_row.iter()); // key at position 0
                packer.extend(after_jsonb_row.iter()); // data at position 1
                packer.extend_by_row(&metadata);
                Some(Ok(row_buf.clone()))
            }
            Ok(None) => None, // Delete operation
            Err(e) => Some(Err(DecodeError { kind: e, raw: vec![] })),
        }
    }
    ```
  - Action: Implement `extract_debezium_json()` helper function (in same file or in `src/interchange/src/json.rs`). Per DD-12/DD-13/DD-17, pseudocode is illustrative:
    ```rust
    fn extract_debezium_json(
        datum: Datum<'_>,
        mode: &DebeziumJsonMode,
    ) -> Result<Option<Row>, DecodeErrorKind> {
        // 1. Convert Jsonb datum to serde_json::Value (requires Row allocation per DD-17)
        // 2. Check for "payload" wrapper; if present, unwrap
        // 3. Extract "op" field:
        //    - "c" or "r" (per DD-12: snapshot reads treated as inserts) → extract after
        //    - "u" → extract after
        //    - "d" → return Ok(None)
        //    - "t" → log warning, return Ok(None) per DD-13
        //    - missing/other → return Err(DecodeErrorKind)
        // 4. Extract "after" field → must be non-null for c/r/u, else Err
        // 5. Serialize "after" back into a Row containing a single Jsonb datum
        // 6. Return Ok(Some(row))
    }
    ```
  - Action: Also update the key_row match arm (line ~517) to include `UpsertStyle::DebeziumJson { .. }` alongside `UpsertStyle::Debezium { .. }` for key handling.
  - Notes: Per DD-17, `Datum::Jsonb` wraps a borrowed `JsonbRef<'a>` — extracting and re-packing requires allocating into a new `Row` via `RowPacker`. Per DD-14, null Kafka values are already handled as deletes before this match arm is reached.

- [ ] **Task 7: Create testdrive tests for Debezium JSON sources**
  - File: `test/testdrive/kafka-debezium-json-sources.td` (new file)
  - Action: Create comprehensive testdrive test covering:
    - Source creation with `KEY FORMAT JSON, VALUE FORMAT JSON, ENVELOPE DEBEZIUM`
    - Insert operation (op="c"): publish JSON with `after` set, `before` null → verify row appears
    - Update operation (op="u"): publish JSON with both `before` and `after` → verify row updated
    - Delete operation (op="d"): publish JSON with `before` set, `after` null → verify row retracted
    - Payload-wrapped format: `{ "payload": { "op": "c", "after": {...} } }`
    - Flat format: `{ "op": "c", "after": {...} }`
    - Compound key: multiple fields in key JSON
    - Error case: malformed message (missing `op` field)
  - Notes: Use `kafka-ingest format=bytes` for raw JSON messages (not Avro). Follow patterns from `kafka-upsert-debezium-sources.td` for structure. Key and value are separate JSON blobs sent as raw bytes.

- [ ] **Task 8: Create testdrive tests for TiCDC mode**
  - File: `test/testdrive/kafka-debezium-json-ticdc-sources.td` (new file)
  - Action: Create tests specifically for TiCDC dialect:
    - Source creation with `ENVELOPE DEBEZIUM (MODE = 'TICDC')`
    - TiCDC-formatted messages with `commit_ts` and `cluster_id` in source metadata
    - Insert/Update/Delete operations with TiCDC envelope structure
  - Notes: TiCDC messages have the same `payload.before/after/op` structure as generic Debezium. The MODE parameter is parsed and stored but V1 behavior differences are minimal — this test establishes the pattern for future TiCDC-specific handling.

### Acceptance Criteria

**Happy Path:**
- [ ] AC-1: Given a Kafka source with `KEY FORMAT JSON, VALUE FORMAT JSON, ENVELOPE DEBEZIUM`, when a JSON message with `"op": "c"` and a non-null `"after"` field is published, then a row appears in the source with `key` column containing the JSON key and `data` column containing the `after` JSON object.
- [ ] AC-2: Given an existing row from AC-1, when a JSON message with `"op": "u"`, matching key, and updated `"after"` is published, then the row's `data` column reflects the new `after` value.
- [ ] AC-3: Given an existing row from AC-1, when a JSON message with `"op": "d"` and null `"after"` is published, then the row is retracted from the source.
- [ ] AC-4: Given a Debezium JSON message wrapped in a `"payload"` object (e.g., `{ "payload": { "op": "c", "after": {...} } }`), when ingested, then envelope extraction succeeds identically to flat format.
- [ ] AC-5: Given a Debezium JSON message in flat format (e.g., `{ "op": "c", "after": {...} }`), when ingested, then envelope extraction succeeds.
- [ ] AC-6: Given a Debezium JSON message with `"op": "r"` (snapshot/read), when ingested, then it is treated as an insert (same as `"c"`). [DD-12]

**TiCDC Mode:**
- [ ] AC-7: Given `ENVELOPE DEBEZIUM (MODE = 'TICDC')` in the CREATE SOURCE statement, when a TiCDC-formatted JSON message is published, then the source processes it correctly. The full Debezium envelope (including `commit_ts`, `cluster_id`, and other TiCDC metadata) is preserved in the `envelope` column for user queries (e.g., `envelope->'source'->>'commit_ts'`).

**Error Handling:**
- [ ] AC-8: Given a JSON message with a missing `"op"` field, when ingested, then a decode error is surfaced through the source's error collection (not silently dropped).
- [ ] AC-9: Given a JSON message with an unrecognized `"op"` value (e.g., `"op": "x"`), when ingested, then a decode error is surfaced.
- [ ] AC-10: Given a JSON message with `"op": "c"` but null `"after"`, when ingested, then a decode error is surfaced.
- [ ] AC-11: Given a Debezium JSON message with `"op": "t"` (truncate), when ingested, then it is silently skipped (not errored). [DD-13]
- [ ] AC-12: Given a null Kafka message value (tombstone), when ingested, then it is treated as a delete for the given key. [DD-14]

**SQL Validation:**
- [ ] AC-13: Given `VALUE FORMAT JSON, ENVELOPE DEBEZIUM` without `KEY FORMAT`, when CREATE SOURCE is executed, then it fails with an error requiring KEY FORMAT.
- [ ] AC-14: Given `VALUE FORMAT TEXT, ENVELOPE DEBEZIUM`, when CREATE SOURCE is executed, then it fails with an error requiring AVRO or JSON format.
- [ ] AC-15: Given `VALUE FORMAT JSON, ENVELOPE DEBEZIUM` with `KEY FORMAT JSON`, when CREATE SOURCE is executed, then it succeeds.

**Restart/Rehydration [F4]:**
- [ ] AC-16: Given a running Debezium JSON source with ingested data, when the source is restarted (cluster restart or rehydration from persist), then all previously ingested rows are correctly restored with matching key and data values.

**Regression:**
- [ ] AC-17: Given existing `VALUE FORMAT AVRO, ENVELOPE DEBEZIUM` sources, when the same Avro Debezium tests are run, then behavior is unchanged (no regressions).

## Additional Context

### Dependencies

- `serde_json` — already in the dependency tree, needed for parsing Debezium JSON envelope fields in `upsert_commands()`
- `mz_repr::adt::jsonb` — existing Jsonb type used for the output datum
- No new external crate dependencies required

### Testing Strategy

**Testdrive (integration):**
- `test/testdrive/kafka-debezium-json-sources.td` — primary test file covering generic Debezium JSON: insert, update, delete, payload-wrapped, flat, compound key, error cases
- `test/testdrive/kafka-debezium-json-ticdc-sources.td` — TiCDC dialect mode tests

**Regression:**
- Run existing `test/testdrive/kafka-upsert-debezium-sources.td` and related Avro Debezium tests to ensure no breakage from the `SourceEnvelope::Debezium` AST change

**Unit tests (if applicable):**
- `extract_debezium_json()` helper function: test payload-wrapped vs flat, all op types, malformed inputs
- Can be added inline in the implementing module with `#[cfg(test)]` block

**Manual verification:**
- Create a Kafka topic, publish raw JSON Debezium messages via `kcat` or similar, create source in Materialize, verify `SELECT * FROM source_table` returns expected results

### Notes

- All identified risks are captured as design decisions DD-9 through DD-17.
- TiCDC MODE is parsed and stored but V1 behavior is identical to generic mode (DD-15).
- Multi-partition ordering is a known constraint documented in DD-16.

**Future considerations (out of scope):**
- Schema-aware column projection (`CREATE SOURCE ... (id int, name text) FORMAT JSON ENVELOPE DEBEZIUM`)
- Keyless upsert where key is derived from the Debezium payload
- `commit_ts`-based ordering for TiCDC (the field is already preserved in the `envelope` column; future work is using it for message ordering)
- Additional MODE values: Maxwell, Canal, etc.
- Truncate operation support (DD-13)

---

## Party Mode Changelog

Changes made during Party Mode review (Step 1):

| # | Change | Source | Section |
|---|--------|--------|---------|
| 1 | Added `MODE` parameter syntax instead of boolean toggle for dialect selection | Winston (Architect) | DD-4, Scope |
| 2 | Added `UpsertStyle::DebeziumJson` as new variant (not reusing Avro's `after_idx`) | Amelia (Developer) | DD-5 (new) |
| 3 | Added support for both `payload`-wrapped and flat envelope formats | Winston (Architect) | DD-6 (new), Scope |
| 4 | Added error handling for malformed messages as in-scope requirement | John (PM) + Murat (Test Architect) | DD-7 (new), Scope |
| 5 | Added note about schema validation being deferred to query time | Winston (Architect) | DD-1 |
| 6 | Refined solution description to reference extensible MODE parameter | All | Solution |

## Adversarial Review Changelog

Findings from adversarial review, resolved as design decisions:

| Finding | Severity | Resolution | Design Decision |
|---------|----------|------------|-----------------|
| F1: key_indices correctness unresolved | Critical | Include key as separate output column; key_indices=[0] | DD-9 |
| F2: `"r"` (snapshot) op type ignored | Critical | Treat "r" as insert, same as "c" | DD-12 |
| F3: INCLUDE KEY broken for JSON Debezium | Critical | Key always included as separate column; skip INCLUDE KEY rejection | DD-9 |
| F4: No restart/rehydration AC | High | Added AC-16 for restart correctness | AC-16 |
| F5: Task 4 pseudocode self-contradictory | High | Rewrote Task 4 with correct key_indices approach | DD-9, Task 4 |
| F6: DebeziumJson arm doesn't prepend key | High | Updated Task 6 pseudocode to prepend key_row | DD-9, Task 6 |
| F7: extract_debezium_json lifetime unclear | High | Noted pseudocode is illustrative; impl must handle Row allocation | DD-17 |
| F8: `"t"` (truncate) op not addressed | Medium | Silently skip with warning log | DD-13, AC-11 |
| F9: Null Kafka values (tombstones) not discussed | Medium | Treated as deletes; existing pipeline handles this | DD-14, AC-12 |
| F10: TiCDC MODE does nothing in V1 | Medium | Acknowledged as structural-only; establishes syntax for future | DD-15 |
| F11: Multi-partition ordering not discussed | Medium | Documented as known constraint | DD-16 |
| F12: No performance ACs | Low | Added "profile before GA" to DD-10 mitigation | DD-10 |
| F13: Nested match structure confusing | Low | Noted in DD-17; simplify during implementation | DD-17 |
