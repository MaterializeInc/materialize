# spec.md

**Feature Branch**: `001-debezium-json`
**Created**: 2026-03-05
**Status**: Draft
**Input**: User description: "Currently Materialize only supports Avro with Debezium. Add support for Debezium JSON from TiCDC."

## Clarifications

### Session 2026-03-05

- Q: Should Materialize support both wrapped (schema+payload) and unwrapped Debezium JSON forms? → A: Yes, support both with auto-detection.
- Q: How should Kafka tombstone messages (null value) be handled? → A: Treat tombstones as delete operations using the key to identify the row.
- Q: How should unrecognized operation types (e.g., `"t"` for truncate) be handled? → A: Error with a clear message; noted as a design decision to revisit later.
- Q: Should `FORMAT JSON ENVELOPE DEBEZIUM` be limited to Kafka sources? → A: No, support any source type that accepts JSON format; noted as a design decision.
- Q: Should round-trip sink-to-source validation be in scope? → A: Yes. Primary deliverable is ingestion; additionally, automated round-trip validation against the existing Debezium JSON sink format is in scope.
- Q: How should extra/missing columns in JSON row payload be handled? → A: Configurable; default to error on both. Support ignoring extra columns and null-filling missing nullable columns. Design decision.
- Q: Should this feature be behind a feature flag per constitution Principle IV? → A: No; this extends existing ENVELOPE DEBEZIUM syntax to a new format without changing existing behavior.
- Q: What happens when both `before` and `after` are null? → A: Produce a decode error (no actionable row data).

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Ingest TiCDC Debezium JSON into Materialize (Priority: P1)

A TiDB user wants to replicate tables from TiDB into Materialize via
TiCDC using the Debezium JSON output format over Kafka. They create a
Kafka source and then create tables using `FORMAT JSON ENVELOPE DEBEZIUM`,
allowing Materialize to interpret the Debezium envelope (before/after/op
fields) and maintain a correct, up-to-date materialized view of the
upstream TiDB table.

**Why this priority**: This is the core feature. Without basic ingestion
of Debezium JSON messages, no other stories are possible. TiCDC only
outputs Debezium in JSON format (not Avro), so this unblocks the entire
TiDB-to-Materialize integration.

**Independent Test**: Can be fully tested by publishing Debezium JSON
messages (INSERT, UPDATE, DELETE) to a Kafka topic and verifying that a
Materialize table with `FORMAT JSON ENVELOPE DEBEZIUM` reflects the
correct final state.

**Acceptance Scenarios**:

1. **Given** a Kafka topic receiving TiCDC Debezium JSON messages with
   INSERT operations (`"op": "c"`), **When** a user creates a table with
   `FORMAT JSON ENVELOPE DEBEZIUM`, **Then** all inserted rows appear in
   the table with correct column values and types.
2. **Given** a Kafka topic receiving UPDATE operations (`"op": "u"`) with
   both `before` and `after` fields populated, **When** the table is
   queried, **Then** the row reflects the updated values from the `after`
   field.
3. **Given** a Kafka topic receiving DELETE operations (`"op": "d"`) with
   `before` populated and `after` set to null, **When** the table is
   queried, **Then** the deleted row is no longer present.

---

### User Story 2 - Standard Debezium JSON from MySQL/PostgreSQL Connectors (Priority: P2)

A user running a standard Debezium connector (MySQL, PostgreSQL, or
other) that outputs JSON-encoded Debezium messages wants to ingest data
into Materialize without requiring Avro or a Schema Registry. They use
the same `FORMAT JSON ENVELOPE DEBEZIUM` syntax.

**Why this priority**: While TiCDC is the motivating use case, Debezium
JSON is a widely-used format across all Debezium connectors. Supporting
it generically (not just TiCDC) maximizes the value of this feature.

**Independent Test**: Can be tested by publishing standard Debezium
JSON messages (as produced by debezium/connect with JSON converter) to
Kafka and verifying correct ingestion.

**Acceptance Scenarios**:

1. **Given** Debezium JSON messages from a MySQL connector with standard
   envelope structure, **When** a user creates a table with `FORMAT JSON
   ENVELOPE DEBEZIUM`, **Then** data is ingested correctly with proper
   type mapping.
2. **Given** Debezium JSON messages that include optional fields like
   `source` and `ts_ms`, **When** the messages are ingested, **Then**
   Materialize ignores the extra metadata fields and extracts only the
   row data from `before`/`after`.

---

### User Story 3 - Round-Trip Sink-to-Source Validation (Priority: P3)

A user has an existing Materialize Debezium JSON sink writing to a
Kafka topic. They want to consume that same topic in another
Materialize environment (or the same one) using `FORMAT JSON ENVELOPE
DEBEZIUM`, achieving identical table state on both sides.

**Why this priority**: Primary deliverable is Debezium JSON ingestion;
additionally, we will add automated round-trip validation against the
existing Debezium JSON sink format. This validates end-to-end
correctness but depends on P1 being complete.

**Independent Test**: Can be tested by creating a sink with
`ENVELOPE DEBEZIUM` in JSON format, then creating a source table
reading from that sink topic with `FORMAT JSON ENVELOPE DEBEZIUM`,
and asserting identical table contents.

**Acceptance Scenarios**:

1. **Given** a Materialize table sinking to a Kafka topic via a
   Debezium JSON sink (key + value, including deletes and tombstones),
   **When** another table reads from that topic using `FORMAT JSON
   ENVELOPE DEBEZIUM`, **Then** the resulting table state is identical
   to the original source table after all changes have propagated.

---

### User Story 4 - Error Handling for Malformed Debezium JSON (Priority: P4)

A user's Debezium JSON stream contains occasional malformed messages
(missing `after` field, invalid JSON, unexpected types). The system
MUST handle these gracefully without crashing or halting ingestion of
the entire source.

**Why this priority**: Production streams are imperfect. Robust error
handling is essential but secondary to basic functionality.

**Independent Test**: Can be tested by publishing a mix of valid and
invalid Debezium JSON messages and verifying that valid messages are
ingested while invalid ones produce appropriate error behavior.

**Acceptance Scenarios**:

1. **Given** a malformed Debezium JSON message (e.g., missing `after`
   field on an INSERT), **When** the message is encountered, **Then**
   the system reports an error for that message but continues processing
   subsequent valid messages.
2. **Given** a Debezium JSON message with a type mismatch (e.g., string
   value where integer expected), **When** the message is processed,
   **Then** the system surfaces the error without halting ingestion.

---

### Edge Cases

- What happens when the `before` field is absent entirely (not null,
  but missing from the JSON object)? Materialize MUST treat this the
  same as a source that does not provide `before` data.
- What happens when the Debezium JSON message contains nested JSON
  objects in column values? The system MUST map these to Materialize's
  `jsonb` type or report a clear error.
- What happens when the JSON payload contains columns not in the table
  definition (extra columns)? By default, the system MUST report an
  error. A configuration option MUST allow ignoring extra columns.
- What happens when the JSON payload is missing columns declared in
  the table definition? By default, the system MUST report an error.
  A configuration option MUST allow null-filling missing nullable
  columns (non-nullable missing columns always error).
  **Design decision**: strict defaults protect against silent data
  loss from schema drift; relaxed modes support schema evolution
  workflows where upstream adds/removes columns over time.
- How does the system handle the Debezium `"r"` (read/snapshot)
  operation type? It MUST be treated as equivalent to an INSERT (`"c"`).
- What happens when both `before` and `after` are null in a Debezium
  JSON message? The system MUST produce a decode error for that
  message, as it contains no actionable row data.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: System MUST accept `FORMAT JSON ENVELOPE DEBEZIUM` in
  `CREATE TABLE ... FROM SOURCE` statements for any source type that
  supports JSON format (not limited to Kafka). **Design decision**:
  while Kafka is the primary use case for Debezium, the envelope
  parsing logic is format-level and not transport-specific; restricting
  to Kafka would be an artificial limitation.
- **FR-002**: System MUST parse the Debezium JSON envelope structure,
  extracting row data from the `after` field for inserts/updates and
  from the `before` field for deletes.
- **FR-003**: System MUST support the standard Debezium operation types:
  `"c"` (create/insert), `"u"` (update), `"d"` (delete), and `"r"`
  (read/snapshot, treated as insert). Unrecognized operation types
  (e.g., `"t"` for truncate) MUST produce a clear error message
  identifying the unsupported op value. **Design decision**: strict
  validation of op types may need to be revisited if connectors that
  emit `"t"` (truncate) or other extended ops become common; a future
  iteration could add explicit truncate support or an ignore mode.
- **FR-004**: System MUST correctly map JSON scalar types (string,
  number, boolean, null) to corresponding Materialize column types
  based on the table's declared schema.
- **FR-005**: System MUST require that `KEY FORMAT JSON` is specified
  when using `ENVELOPE DEBEZIUM` with JSON, consistent with the
  existing Avro Debezium requirement for key format.
- **FR-006**: System MUST ignore extra envelope-level fields (e.g.,
  `source`, `ts_ms`, `transaction`) that are not part of the row data.
- **FR-007**: System MUST support both Debezium JSON message forms:
  the wrapped form (`{"schema": ..., "payload": {...}}`) and the
  unwrapped form (envelope fields at top level). The system MUST
  auto-detect which form is present by checking for a top-level
  `payload` field.
- **FR-008**: System MUST handle the case where `before` is absent or
  null on insert/snapshot operations without error.
- **FR-009**: System MUST produce clear, actionable error messages when
  a Debezium JSON message cannot be decoded (malformed JSON, missing
  required fields, type mismatches).
- **FR-010**: System MUST handle Kafka tombstone messages (null value
  with a non-null key) by treating them as delete operations, using
  the key to identify the row to retract.
- **FR-011**: System MUST provide configurable column mismatch
  handling for Debezium JSON decoding. By default, extra columns in
  the JSON payload and missing columns MUST produce decode errors.
  Configuration options MUST allow: (a) ignoring extra columns,
  (b) null-filling missing nullable columns. Non-nullable missing
  columns MUST always error regardless of configuration.
  **Design decision**: strict defaults prevent silent data loss;
  relaxed modes support schema evolution workflows.

### Key Entities

- **Debezium JSON Envelope**: The top-level message structure containing
  `before` (nullable record), `after` (nullable record), `op` (string),
  and optional metadata fields (`source`, `ts_ms`, `transaction`).
- **Row Payload**: The nested JSON object within `before`/`after` that
  contains the actual column name-value pairs for the upstream table row.
- **Operation Type**: A string field (`op`) indicating the change type.
  Valid values: `"c"`, `"u"`, `"d"`, `"r"`.

### Assumptions

- TiCDC Debezium JSON format follows the standard Debezium envelope
  structure with minor extensions (`commit_ts`, `cluster_id` in source
  metadata). These extensions are safely ignored.
- Users will declare the table schema explicitly in Materialize (column
  names and types), as there is no schema registry for JSON. The JSON
  payload is decoded according to this declared schema.
- The existing `ENVELOPE DEBEZIUM` upsert logic (tracking state via
  the `after` field index) can be reused; the primary new work is in
  JSON decoding of the Debezium envelope structure.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: Users can create a table from a Kafka source using
  `FORMAT JSON ENVELOPE DEBEZIUM` and see correct data with latency
  characteristics comparable to existing Avro Debezium sources.
- **SC-002**: All four Debezium operations (insert, update, delete,
  snapshot) produce correct results, verified by end-to-end tests that
  assert final table state matches expected state after a sequence of
  changes.
- **SC-003**: Malformed messages do not halt ingestion; the system
  continues processing valid messages after encountering errors.
- **SC-004**: The feature works with messages from at least two
  different Debezium producers (TiCDC and standard Debezium MySQL
  connector) without producer-specific configuration.
- **SC-005**: A Materialize Debezium JSON sink topic (key + value,
  including deletes and tombstone behavior) is consumable by
  `FORMAT JSON ENVELOPE DEBEZIUM` with identical resulting table
  state, verified by an automated round-trip test.

# plan.md

**Branch**: `001-debezium-json` | **Date**: 2026-03-05 | **Spec**: [spec.md](spec.md)
**Input**: Feature specification from `/specs/001-debezium-json/spec.md`

## Summary

Add support for `FORMAT JSON ENVELOPE DEBEZIUM` in Materialize sources,
enabling ingestion of Debezium-formatted JSON CDC messages from TiCDC,
standard Debezium connectors, and Materialize's own Debezium JSON sinks.

The core work involves: (1) removing the Avro-only validation gate in
the SQL planner, (2) creating a specialized JSON Debezium decoder that
parses the envelope structure and produces `before`/`after` Record
columns compatible with the existing `UpsertStyle::Debezium` runtime
path, and (3) adding comprehensive testdrive tests.

## Technical Context

**Language/Version**: Rust (latest pinned in `rust-toolchain.toml`)
**Primary Dependencies**: `serde_json` (already in tree), `mz_repr`,
`mz_interchange`, `mz_storage`, `mz_sql`, `mz_sql_parser`
**Storage**: N/A (uses existing Kafka source infrastructure)
**Testing**: testdrive (primary), cargo test (unit tests for decoder)
**Target Platform**: Linux (CI), macOS (dev)
**Project Type**: Feature addition to streaming database
**Performance Goals**: No regression vs existing JSON source throughput
**Constraints**: Must not break existing Avro Debezium sources
**Scale/Scope**: Single feature across ~5 crates; ~500-1000 lines new code

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

| Principle | Status | Notes |
|-----------|--------|-------|
| I. Correctness First | PASS | All Debezium ops (c/u/d/r) + tombstones covered; edge cases specified |
| II. Simplicity | PASS | Reuses existing UpsertStyle::Debezium runtime; new decoder is minimal |
| III. Testing Discipline | PASS | Testdrive tests for all ops + edge cases + round-trip planned |
| IV. Backward Compatibility | JUSTIFIED DEVIATION | Additive change; existing Avro path untouched. Constitution says "New features MUST be gated behind feature flags." This feature extends existing `ENVELOPE DEBEZIUM` syntax to accept an additional format (JSON) without altering any existing behavior. It is analogous to adding a new type cast or function—not a new user-visible surface area. The PR description MUST note this deviation per governance rules. |
| V. Performance Awareness | PASS | JSON parsing is O(n) in message size; no new allocations beyond existing JSON path |
| VI. Code Clarity | PASS | Decoder is self-contained; planner change is small and well-scoped |
| VII. Observability | PASS | Decode errors produce actionable messages identifying the problem field/value. Tracing spans and structured logging MUST be added for the new decoder path (task T021). |

**Post-design re-check**: All gates still PASS. No new complexity
introduced beyond what the spec requires.

## Project Structure

### Documentation (this feature)

```text
specs/001-debezium-json/
├── plan.md              # This file
├── research.md          # Phase 0 output
├── data-model.md        # Phase 1 output
├── quickstart.md        # Phase 1 output
├── contracts/
│   └── sql-syntax.md    # SQL interface contract
└── tasks.md             # Phase 2 output (via /speckit.tasks)
```

### Source Code (repository root)

```text
src/
├── sql-parser/src/
│   └── parser.rs                    # No changes needed (DEBEZIUM already parsed)
├── sql/src/plan/statement/
│   └── ddl.rs                       # Remove Avro-only gate; handle JSON+Debezium planning
├── storage-types/src/sources/
│   ├── encoding.rs                  # No changes needed (DataEncoding::Json exists)
│   └── envelope.rs                  # No changes needed (UpsertStyle::Debezium exists)
├── interchange/src/
│   └── json.rs                      # Add Debezium JSON decoder (new module or extend)
├── storage/src/
│   ├── decode.rs                    # Wire up new JSON Debezium decoder
│   └── render/sources.rs            # No changes needed (Debezium upsert path exists)
test/
└── testdrive/
    ├── kafka-debezium-json.td       # New: core Debezium JSON tests
    └── kafka-debezium-json-roundtrip.td  # New: sink→source round-trip test
doc/
└── user/
    └── release-notes.md             # Update with new feature
```

**Structure Decision**: This is a feature addition to the existing
Materialize codebase. Changes span the SQL planner, interchange
(decoder), storage (decode pipeline), and test suites. No new crates
or top-level directories are introduced.

## Architecture: Decode Pipeline

### Current (Avro + Debezium)

```
Kafka bytes → Avro decoder (schema from CSR) → Row[before, after, op, source, ...]
  → typecheck_debezium() validates schema → UpsertStyle::Debezium { after_idx }
  → runtime extracts after field → final Row
```

### New (JSON + Debezium)

```
Kafka bytes → JSON Debezium decoder:
  1. Parse JSON (serde_json)
  2. Auto-detect wrapped vs unwrapped (check for "payload" key)
  3. Extract envelope object
  4. Read "op" field → validate (c/u/d/r or error)
  5. Read "after" field → decode against declared table schema → Record
  6. Read "before" field → decode against declared table schema → Record (or null)
  7. Pack Row[before, after]
→ UpsertStyle::Debezium { after_idx }  (same as Avro path)
→ runtime extracts after field → final Row

Tombstone (null value):
  → treated as delete using key for row identification
```

### Key Design Decisions

1. **Dedicated decoder, not generic JSON post-processing**: The JSON
   Debezium decoder is a purpose-built component that produces the
   same Row structure as the Avro Debezium decoder. This means the
   downstream `UpsertStyle::Debezium` runtime code needs zero changes.

2. **Schema from table definition**: Unlike Avro (which gets schema
   from CSR), the JSON decoder uses the column names and types from
   the user's `CREATE TABLE` statement to decode the `before`/`after`
   JSON objects. The planner passes this schema to the decoder.

3. **Auto-detect wrapper**: One JSON parse, check for `payload` key,
   no user configuration needed.

4. **Strict op validation**: Unknown ops error (design decision per
   clarification, may be revisited).

## Complexity Tracking

No constitution violations to justify. All changes are minimal and
directly required by the spec.

# research.md

## R1: JSON Decoding Pipeline for Debezium Envelope

**Decision**: Create a new Debezium-specific JSON decoder that produces
`before`/`after` Record columns, mirroring the structure the Avro
Debezium path produces.

**Rationale**: The existing `DataEncoding::Json` produces a single
`data: jsonb` column. The Debezium envelope processing
(`UpsertStyle::Debezium { after_idx }`) expects `before` and `after`
columns as Record types. Rather than retrofitting the generic JSON
decoder, a dedicated decoder can handle envelope extraction (wrapped
vs unwrapped), `op` field validation, and type-safe mapping of the
`before`/`after` payloads to the user-declared table schema.

**Alternatives considered**:
- Post-process the single JSONB column at the envelope layer: rejected
  because it would require duplicating type coercion logic and would
  not produce the Record-typed columns that `typecheck_debezium` expects.
- Add a generic "structured JSON" decoder: over-engineering for this
  use case; YAGNI per constitution.

## R2: Planner Validation Gate

**Decision**: Modify the `apply_source_envelope_encoding()` function in
`src/sql/src/plan/statement/ddl.rs` to accept `DataEncoding::Json` in
addition to `DataEncoding::Avro` when `ENVELOPE DEBEZIUM` is specified.

**Rationale**: The current code at lines 1401-1405 rejects all
non-Avro formats with "ENVELOPE DEBEZIUM requires that VALUE FORMAT
is set to AVRO". This is the primary gate to remove. For JSON, we
skip `typecheck_debezium` (which validates Avro schema structure) and
instead rely on the new JSON Debezium decoder to produce the correct
column structure at runtime.

**Alternatives considered**:
- Making `typecheck_debezium` work for JSON: not feasible because
  JSON's `desc()` returns a single JSONB column, not the envelope
  structure. The schema structure is only known after decoding.

## R3: Schema Wrapper Auto-Detection

**Decision**: The JSON Debezium decoder checks for a top-level
`payload` key. If present, extracts `payload` as the envelope object.
Otherwise, treats the entire JSON object as the envelope.

**Rationale**: TiCDC and standard Debezium (with `schemas.enable=true`,
the default) produce `{"schema": ..., "payload": {...}}`. Some
configurations produce the envelope directly. Auto-detection is
simple (one key check) and handles both cases.

**Alternatives considered**:
- SQL-level configuration flag: unnecessary complexity; auto-detection
  is unambiguous since `payload` is not a valid Debezium envelope field.

## R4: Tombstone Handling

**Decision**: Null-value Kafka messages (tombstones) with non-null keys
are treated as delete operations.

**Rationale**: Per clarification session. The key identifies the row to
retract. This is consistent with upsert semantics where a null value
means "delete this key."

**Alternatives considered**:
- Skip silently: would miss legitimate deletes from some producers.
- Error: would break compacted topics.

## R5: Sink Round-Trip Format

**Decision**: Materialize's existing Debezium JSON sink produces
unwrapped envelopes (`{"before": ..., "after": ...}`) without a
schema wrapper. The new decoder handles this natively via
auto-detection (no `payload` key → unwrapped form).

**Rationale**: Confirmed from sink encoder code and test files. No
changes to the sink are needed; round-trip works if the decoder
supports the unwrapped form.

## R6: Key Implementation Files

| Area | File |
|------|------|
| Planner validation | `src/sql/src/plan/statement/ddl.rs` (lines ~1390-1411) |
| DataEncoding enum | `src/storage-types/src/sources/encoding.rs` |
| JSON decode runtime | `src/storage/src/decode.rs` |
| Envelope types | `src/storage-types/src/sources/envelope.rs` |
| Envelope utilities | `src/interchange/src/envelopes.rs` |
| JSON encoder (sink) | `src/interchange/src/json.rs` |
| Upsert processing | `src/storage/src/render/sources.rs` (lines ~550) |
| SQL parser | `src/sql-parser/src/parser.rs` (lines ~2362) |
| Existing tests | `test/testdrive/kafka-upsert-debezium-sources.td` |

# data-model.md

## Entities

### Debezium JSON Envelope (input message)

The top-level JSON structure received from Kafka (or other sources).

| Field | Type | Required | Notes |
|-------|------|----------|-------|
| `schema` | object | No | Present when `schemas.enable=true`; ignored |
| `payload` | object | No | When present, contains the actual envelope |
| `before` | object or null | Yes* | Row state before change; null for inserts |
| `after` | object or null | Yes* | Row state after change; null for deletes |
| `op` | string | Yes* | Operation type: `"c"`, `"u"`, `"d"`, `"r"` |
| `source` | object | No | Connector metadata; ignored |
| `ts_ms` | integer | No | Timestamp; ignored |
| `transaction` | object | No | Transaction metadata; ignored |

*Required within the envelope (either at top level or inside `payload`).

**Wrapped form**: `{"schema": {...}, "payload": {"before": ..., "after": ..., "op": ...}}`
**Unwrapped form**: `{"before": ..., "after": ..., "op": ...}`
**Detection rule**: If top-level key `payload` exists, use wrapped form.

### Row Payload (within before/after)

A flat JSON object whose keys are column names matching the
user-declared table schema.

| Aspect | Rule |
|--------|------|
| Column matching | JSON keys matched by name to declared columns |
| Missing columns | Error by default; configurable to null-fill nullable columns |
| Extra columns | Error by default; configurable to silently ignore |
| Type coercion | JSON scalar → Materialize type per declared schema |

### Key Payload (Kafka message key)

A JSON object containing primary key columns. Used for upsert
semantics and tombstone delete identification.

### Tombstone Message

A Kafka message with non-null key and null value. Treated as a
delete operation keyed by the key payload.

## Type Mapping (JSON → Materialize)

| JSON Type | Materialize Type | Notes |
|-----------|-----------------|-------|
| string | text, varchar, char | Direct mapping |
| number (integer) | int2, int4, int8 | Range-checked |
| number (float) | float4, float8, numeric | Precision rules apply |
| boolean | bool | Direct mapping |
| null | any nullable type | Column must be nullable |
| object | jsonb | Nested objects mapped to jsonb |
| array | jsonb | Arrays mapped to jsonb |
| string (ISO 8601) | timestamp, date, time | When target column is temporal |

## State Transitions (per row, keyed by primary key)

```
         ┌──────────┐
    c/r  │          │  c/r
   ───── │ ABSENT   │ ─────
   │     │          │     │
   │     └──────────┘     │
   ▼                      ▼
┌──────────┐         ┌──────────┐
│ PRESENT  │───u────▶│ PRESENT  │
│ (v1)     │         │ (v2)     │
└──────────┘         └──────────┘
   │                      │
   │ d / tombstone        │ d / tombstone
   ▼                      ▼
┌──────────┐         ┌──────────┐
│ ABSENT   │         │ ABSENT   │
└──────────┘         └──────────┘
```

Operations:
- `c` (create) / `r` (read/snapshot): ABSENT → PRESENT
- `u` (update): PRESENT(v1) → PRESENT(v2)
- `d` (delete) / tombstone: PRESENT → ABSENT

# contracts/sql-syntax.md

## CREATE TABLE with Debezium JSON

```sql
CREATE TABLE <table_name>
  FROM SOURCE <source_name> (REFERENCE "<topic>")
  KEY FORMAT JSON
  VALUE FORMAT JSON
  ENVELOPE DEBEZIUM;
```

### Columns

The table definition provides the schema for decoding `before`/`after`
JSON payloads. Column names MUST match keys in the JSON payload.

```sql
CREATE TABLE orders (
    id int8 NOT NULL,
    product text,
    quantity int4,
    price numeric
)
FROM SOURCE kafka_src (REFERENCE "dbz.inventory.orders")
KEY FORMAT JSON
VALUE FORMAT JSON
ENVELOPE DEBEZIUM;
```

### With INCLUDE metadata

```sql
CREATE TABLE orders (
    id int8 NOT NULL,
    product text
)
FROM SOURCE kafka_src (REFERENCE "dbz.inventory.orders")
KEY FORMAT JSON
VALUE FORMAT JSON
INCLUDE PARTITION, OFFSET
ENVELOPE DEBEZIUM;
```

### Column Mismatch Configuration

By default, extra and missing columns produce decode errors. Relaxed
handling can be configured via `VALUE DECODING ERRORS`:

```sql
CREATE TABLE orders (
    id int8 NOT NULL,
    product text
)
FROM SOURCE kafka_src (REFERENCE "dbz.inventory.orders")
KEY FORMAT JSON
VALUE FORMAT JSON
ENVELOPE DEBEZIUM (VALUE DECODING ERRORS = (
    IGNORE EXTRA COLUMNS,
    NULL MISSING COLUMNS
));
```

| Option | Behavior |
|--------|----------|
| (default) | Error on extra columns and missing columns |
| `IGNORE EXTRA COLUMNS` | Silently skip JSON keys not in table definition |
| `NULL MISSING COLUMNS` | Fill missing nullable columns with null; NOT NULL columns still error |

Options can be combined or used individually.

### Constraints

- `KEY FORMAT JSON` is REQUIRED (error if omitted).
- `INCLUDE KEY` is NOT allowed with `ENVELOPE DEBEZIUM` (keys are
  already part of the value payload).
- Column types in the table definition determine how JSON values are
  coerced. Type mismatches produce per-row decode errors.

## Error Messages

| Condition | Message |
|-----------|---------|
| Missing KEY FORMAT | `ENVELOPE [DEBEZIUM] UPSERT requires that KEY FORMAT be specified` |
| INCLUDE KEY used | `Cannot use INCLUDE KEY with ENVELOPE DEBEZIUM: Debezium values include all keys.` |
| Missing `after` in message | `Debezium JSON message missing required 'after' field` |
| Missing `op` in message | `Debezium JSON message missing required 'op' field` |
| Unknown op value | `Unsupported Debezium operation type: "<op>"` |
| Type mismatch | `Failed to decode Debezium JSON: expected <type> for column "<col>", got <json_type>` |

## Input Message Formats

### Wrapped (schemas.enable=true, default)

```json
{
  "schema": { "type": "struct", "fields": [...] },
  "payload": {
    "before": null,
    "after": {"id": 1, "product": "widget", "quantity": 10, "price": 9.99},
    "op": "c",
    "source": {"connector": "TiCDC", "db": "inventory", "table": "orders"}
  }
}
```

### Unwrapped (schemas.enable=false)

```json
{
  "before": null,
  "after": {"id": 1, "product": "widget", "quantity": 10, "price": 9.99},
  "op": "c",
  "source": {"connector": "TiCDC", "db": "inventory", "table": "orders"}
}
```

### Tombstone (Kafka log compaction)

Key: `{"id": 1}`
Value: `null`

# quickstart.md

## Prerequisites

- Materialize running locally (`bin/environmentd`)
- Kafka broker available
- A Kafka topic with Debezium JSON messages (e.g., from TiCDC or
  standard Debezium connector)

## Steps

### 1. Create a Kafka connection

```sql
CREATE CONNECTION kafka_conn TO KAFKA (BROKER 'localhost:9092');
```

### 2. Create a source

```sql
CREATE SOURCE dbz_source
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'dbz.inventory.orders')
  KEY FORMAT JSON
  VALUE FORMAT JSON
  ENVELOPE DEBEZIUM;
```

### 3. Create a table from the source

```sql
CREATE TABLE orders (
    id int8 NOT NULL,
    product text,
    quantity int4,
    price numeric
)
FROM SOURCE dbz_source (REFERENCE "dbz.inventory.orders")
KEY FORMAT JSON
VALUE FORMAT JSON
ENVELOPE DEBEZIUM;
```

### 4. Query the table

```sql
SELECT * FROM orders;
```

### 5. Verify CDC operations

Publish a sequence of Debezium JSON messages to the topic:

**Insert** (`op: "c"`):
```json
{"before": null, "after": {"id": 1, "product": "widget", "quantity": 10, "price": 9.99}, "op": "c"}
```

**Update** (`op: "u"`):
```json
{"before": {"id": 1, "product": "widget", "quantity": 10, "price": 9.99}, "after": {"id": 1, "product": "widget", "quantity": 5, "price": 9.99}, "op": "u"}
```

**Delete** (`op: "d"`):
```json
{"before": {"id": 1, "product": "widget", "quantity": 5, "price": 9.99}, "after": null, "op": "d"}
```

After each operation, query `SELECT * FROM orders` to verify the table
reflects the expected state.

## Round-Trip Test

To verify a Materialize Debezium JSON sink can be consumed back:

```sql
-- Create a sink
CREATE SINK orders_sink
  FROM orders
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'orders-sink')
  KEY (id)
  FORMAT JSON
  ENVELOPE DEBEZIUM;

-- Create a table reading from the sink topic
CREATE TABLE orders_roundtrip (
    id int8 NOT NULL,
    product text,
    quantity int4,
    price numeric
)
FROM SOURCE another_source (REFERENCE "orders-sink")
KEY FORMAT JSON
VALUE FORMAT JSON
ENVELOPE DEBEZIUM;

-- Verify identical state
SELECT * FROM orders EXCEPT ALL SELECT * FROM orders_roundtrip;
-- Should return 0 rows
```

# checklists/requirements.md

**Purpose**: Validate specification completeness and quality before proceeding to planning
**Created**: 2026-03-05
**Feature**: [spec.md](../spec.md)

## Content Quality

- [x] No implementation details (languages, frameworks, APIs)
- [x] Focused on user value and business needs
- [x] Written for non-technical stakeholders
- [x] All mandatory sections completed

## Requirement Completeness

- [x] No [NEEDS CLARIFICATION] markers remain
- [x] Requirements are testable and unambiguous
- [x] Success criteria are measurable
- [x] Success criteria are technology-agnostic (no implementation details)
- [x] All acceptance scenarios are defined
- [x] Edge cases are identified
- [x] Scope is clearly bounded
- [x] Dependencies and assumptions identified

## Feature Readiness

- [x] All functional requirements have clear acceptance criteria
- [x] User scenarios cover primary flows
- [x] Feature meets measurable outcomes defined in Success Criteria
- [x] No implementation details leak into specification

## Notes

- All items pass validation.
- SQL syntax (`FORMAT JSON ENVELOPE DEBEZIUM`) is referenced as the
  user-facing interface, not as implementation detail - this is the
  product surface area.
- Assumptions section documents reasonable defaults for schema
  declaration approach and TiCDC compatibility.

# checklists/comprehensive.md

**Purpose**: Validate completeness, clarity, consistency, and coverage of requirements across the full spec before implementation
**Created**: 2026-03-05
**Feature**: [spec.md](../spec.md)
**Depth**: Standard (~30 items)
**Audience**: Reviewer (PR/planning validation)

## Requirement Completeness

- [ ] CHK001 - Are all supported Debezium operation types explicitly enumerated with their semantics? [Completeness, Spec FR-003]
- [ ] CHK002 - Are requirements for the `"r"` (read/snapshot) operation documented with enough detail to distinguish it from `"c"` (create)? [Completeness, Spec Edge Cases]
- [ ] CHK003 - Is the full set of JSON-to-Materialize type mappings specified (string, number, boolean, null, nested objects, arrays, temporal types)? [Completeness, Spec FR-004]
- [ ] CHK004 - Are requirements defined for handling missing columns in the JSON payload (column in table definition but absent from JSON)? [Gap]
- [ ] CHK005 - Are requirements defined for handling extra columns in the JSON payload (column in JSON but not in table definition)? [Gap]
- [ ] CHK006 - Is the behavior for `null` key messages (non-tombstone, e.g., keyless topics) specified? [Gap]
- [ ] CHK007 - Are requirements for `INCLUDE PARTITION` and `INCLUDE OFFSET` metadata with Debezium JSON documented? [Gap]

## Requirement Clarity

- [ ] CHK008 - Is the auto-detection logic for wrapped vs unwrapped forms unambiguously specified (what constitutes a "top-level `payload` field")? [Clarity, Spec FR-009]
- [ ] CHK009 - Is "clear, actionable error message" quantified with specific message content or structure for each error class? [Clarity, Spec FR-008]
- [ ] CHK010 - Is the phrase "any source type that supports JSON format" exhaustively defined (which source types currently support JSON)? [Clarity, Spec FR-001]
- [ ] CHK011 - Are the exact conditions under which a tombstone is treated as a delete precisely defined (null value + non-null key, or other conditions)? [Clarity, Spec FR-010]
- [ ] CHK012 - Is the scope of "type mismatch" errors defined (which specific mismatches produce errors vs coercion)? [Clarity, Spec FR-008]

## Requirement Consistency

- [ ] CHK013 - Are the key format requirements consistent between Debezium JSON and existing Debezium Avro (both require KEY FORMAT)? [Consistency, Spec FR-005]
- [ ] CHK014 - Is the `INCLUDE KEY` prohibition consistent with the stated rationale that "Debezium values include all keys"? [Consistency, Spec Contracts]
- [ ] CHK015 - Are error handling requirements in User Story 4 consistent with FR-008 (same error categories and behaviors)? [Consistency]
- [ ] CHK016 - Is the tombstone-as-delete behavior (FR-010) consistent with the existing Avro Debezium tombstone handling? [Consistency]

## Acceptance Criteria Quality

- [ ] CHK017 - Are success criteria SC-001 through SC-005 all independently measurable without implementation knowledge? [Measurability, Spec SC]
- [ ] CHK018 - Is "standard source ingestion latency" in SC-001 quantified or referenced to a baseline? [Measurability, Spec SC-001]
- [ ] CHK019 - Does SC-004 specify what "without producer-specific configuration" means concretely (same SQL statement works for both)? [Measurability, Spec SC-004]
- [ ] CHK020 - Is "identical resulting table state" in SC-005 defined precisely (row-for-row, including ordering, nullability, type fidelity)? [Measurability, Spec SC-005]

## Scenario Coverage

- [ ] CHK021 - Are requirements defined for the case where a Debezium JSON message has both `before` and `after` as null? [Coverage, Edge Case]
- [ ] CHK022 - Are requirements defined for messages with an `op` field but empty/null `after` on a create operation? [Coverage, Edge Case]
- [ ] CHK023 - Are requirements specified for handling duplicate keys across messages (idempotency of upsert)? [Coverage, Gap]
- [ ] CHK024 - Are requirements defined for interleaved wrapped and unwrapped messages on the same topic? [Coverage, Edge Case]
- [ ] CHK025 - Are requirements for handling very large JSON payloads (e.g., >1MB per message) documented? [Coverage, Gap]

## Edge Case Coverage

- [ ] CHK026 - Is the behavior for nested JSON objects in `before`/`after` fields clearly specified for all Materialize column types (not just jsonb)? [Edge Case, Spec Edge Cases]
- [ ] CHK027 - Are requirements for Unicode/encoding edge cases in JSON string values documented? [Edge Case, Gap]
- [ ] CHK028 - Is the behavior for numeric precision loss (JSON number → Materialize numeric) specified? [Edge Case, Spec FR-004]

## Non-Functional Requirements

- [ ] CHK029 - Are performance requirements specified (throughput target, latency budget for JSON parsing overhead vs raw JSON format)? [Non-Functional, Gap]
- [ ] CHK030 - Are observability requirements defined (logging/tracing for decode errors, metrics for decode success/failure rates)? [Non-Functional, Gap]
- [ ] CHK031 - Are documentation requirements specified (user-facing docs, release notes, removal of the "Avro only" notice)? [Non-Functional, Gap]

## Dependencies & Assumptions

- [ ] CHK032 - Is the assumption that "existing UpsertStyle::Debezium runtime path can be reused" validated against the JSON decode output format? [Assumption, Spec Assumptions]
- [ ] CHK033 - Is the assumption that "users will declare the table schema explicitly" documented as a user-facing requirement (not just an implementation note)? [Assumption, Spec Assumptions]
- [ ] CHK034 - Are the design decisions marked for future revisitation (op type strictness, source type scope) tracked in a way that ensures follow-up? [Traceability, Spec FR-001, FR-003]

## Notes

- Items marked [Gap] indicate requirements that may need to be added to the spec.
- Items marked [Assumption] should be validated before implementation begins.
- Design decisions flagged for revisitation should have tracking issues created.
