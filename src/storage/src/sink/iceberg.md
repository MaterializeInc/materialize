# Iceberg Sink Implementation

This document describes the implementation of the Iceberg sink in Materialize, which allows streaming data from Materialize to Apache Iceberg tables.

## Overview

The Iceberg sink is implemented as a Timely Dataflow pipeline that:

1. Initializes or loads an existing Iceberg table
2. Mints time-based batch descriptions for grouping writes
3. Writes data to Parquet files using Iceberg's DeltaWriter
4. Commits completed batches as Iceberg snapshots with frontier metadata

The sink supports **exactly-once semantics** through frontier tracking stored in Iceberg snapshot metadata, enabling resume after restarts.

## Architecture

```
┌─────────────────────┐
│  Input Collection   │
│  (Row, DiffPair)    │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│  init_table_and_    │  Creates/loads Iceberg table
│  extract_schema     │  Single worker operation
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│  mint_batch_        │  Generates time-based batch boundaries
│  descriptions       │  Handles resume from snapshots
└──────────┬──────────┘
           │
     ┌─────┴─────┐
     │           │
     ▼           ▼
┌─────────┐  ┌─────────────────┐
│  Data   │  │ Batch Descs     │
│  Stream │  │ (lower, upper)  │
└────┬────┘  └───────┬─────────┘
     │               │
     └───────┬───────┘
             ▼
┌─────────────────────┐
│  write_data_files   │  Writes Parquet files per batch
│                     │  Handles row stashing
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│  commit_to_iceberg  │  Commits batches as snapshots
│                     │  Stores frontier in metadata
└─────────────────────┘
```

## Constants

| Constant | Value | Description |
|----------|-------|-------------|
| `DEFAULT_ARRAY_BUILDER_ITEM_CAPACITY` | 1024 | Initial capacity for Arrow array builders |
| `DEFAULT_ARRAY_BUILDER_DATA_CAPACITY` | 1024 | Initial byte capacity for string/binary builders |
| `PARQUET_FILE_PREFIX` | `"mz_data"` | Prefix for generated Parquet files |
| `INITIAL_DESCRIPTIONS_TO_MINT` | 3 | Number of batch descriptions to keep in-flight |

## Key Types

### `DeltaWriterType`

```rust
type DeltaWriterType = DeltaWriter<
    DataFileWriter<ParquetWriterType>,
    PositionDeleteFileWriter<ParquetWriterType>,
    EqualityDeleteFileWriter<ParquetWriterType>,
>;
```

The combined writer that handles inserts, position deletes, and equality deletes in a single interface.

### `BoundedDataFile`

Associates a data file with its batch boundaries:

```rust
struct BoundedDataFile {
    data_file: SerializableDataFile,
    batch_desc: (Antichain<Timestamp>, Antichain<Timestamp>),
}
```

### `SerializableDataFile`

Wraps Iceberg's `DataFile` with custom serialization using Avro encoding (required because `DataFile` doesn't implement `Serialize`/`Deserialize` directly).

## Helper Functions

### `add_field_ids_to_arrow_schema`

Adds Parquet field IDs to an Arrow schema. Iceberg requires these IDs for schema evolution tracking.

```rust
fn add_field_ids_to_arrow_schema(schema: ArrowSchema) -> ArrowSchema
```

Field IDs are stored in the `PARQUET_FIELD_ID_META_KEY` metadata field and are 1-indexed.

### `merge_materialize_metadata_into_iceberg_schema`

Merges Materialize's Arrow extension metadata (e.g., `ARROW:extension:name`) into the Iceberg-derived Arrow schema. This preserves:

- Iceberg's data types and field IDs
- Materialize's extension names for `ArrowBuilder` compatibility

### `row_to_recordbatch`

Converts a `DiffPair<Row>` into an Arrow `RecordBatch` with an `__op` column:

| Operation | `__op` Value |
|-----------|--------------|
| Delete (before) | -1 |
| Insert (after) | 1 |

The DeltaWriter uses this column to route rows to the appropriate writer (data vs delete files).

### `retrieve_upper_from_snapshots`

Scans Iceberg snapshots (newest first) to find the most recent Materialize frontier stored in the `mz-frontier` property. Skips snapshots with `operation="replace"` (compactions).

Returns: `Option<(Antichain<Timestamp>, Option<u64>)>` - the frontier and optional sink version.

### `load_or_create_table`

Loads an existing Iceberg table or creates it if it doesn't exist. Uses the catalog's default warehouse location.

### `reload_table`

Reloads a table from the catalog, verifying that schema and partition spec IDs haven't changed (evolution not yet supported).

## Dataflow Operators

### 1. `init_table_and_extract_schema`

**Purpose**: Initialize the Iceberg table on startup.

**Worker Selection**: Only the worker where `sink_id.hashed() % peers == index` performs the initialization.

**Behavior**:
- Connects to the Iceberg catalog
- Calls `load_or_create_table` to ensure the table exists
- Passes through all input data unchanged

**Error Handling**: Emits `HealthStatusMessage` with `halting` status on failure.

### 2. `mint_batch_descriptions`

**Purpose**: Generate time-based batch boundaries for grouping writes into snapshots.

**Worker Selection**: Single active worker (same selection as init).

**Resume Logic**:
1. Loads the table and retrieves `resume_upper` from snapshots
2. Checks for overcompaction (input `as_of` beyond `resume_upper`)
3. Checks for version fencing (newer sink version in snapshots)
4. Creates a catch-up batch if resuming from a prior frontier

**Batch Minting**:
- Maintains a sliding window of `INITIAL_DESCRIPTIONS_TO_MINT` future batches
- Each batch spans `commit_interval` milliseconds
- When the oldest batch becomes ready (frontier advances past its upper), a new future batch is minted

**Output**:
- Passes through input data unchanged
- Emits `(lower, upper)` batch descriptions on a separate stream

### 3. `write_data_files`

**Purpose**: Write rows to Parquet data files grouped by batch.

**Key Data Structures**:

| Structure | Purpose |
|-----------|---------|
| `stashed_rows` | Rows that arrived before their batch description |
| `in_flight_batches` | Batches currently being written, keyed by `(lower, upper)` |

**Writer Setup**:
- Creates a `DeltaWriter` per batch with:
  - Data file writer for inserts
  - Position delete file writer
  - Equality delete file writer (using key columns)
- Uses UUID suffix in filenames to avoid collisions

**Row Processing**:
1. When a batch description arrives, drain any stashed rows belonging to it
2. When a row arrives, find its in-flight batch or stash it
3. When frontiers advance past a batch's upper, close the writer and emit data files

**S3 Tables Workaround**: Corrects table location when the catalog incorrectly sets it to the metadata file path instead of the warehouse root.

### 4. `commit_to_iceberg`

**Purpose**: Commit completed batches as Iceberg snapshots.

**Worker Selection**: Single active worker (same as mint).

**Commit Process**:
1. Collect data files for each batch
2. Sort batches by timestamp for ordered commits
3. For each batch:
   - Separate data files from delete files
   - Create transaction with `row_delta` action
   - Store frontier in snapshot properties:
     - `mz-sink-id`: The sink's GlobalId
     - `mz-frontier`: JSON-serialized frontier elements
     - `mz-sink-version`: Sink version for fencing
   - Commit with retry on conflicts

**Conflict Handling**:
- On `CatalogCommitConflicts`, reload the table and check for fencing
- If another writer advanced the frontier beyond ours, fail with a fatal error
- Otherwise, retry up to 5 times

**Persist Integration**:
- After committing to Iceberg, advances the persist shard frontier via `compare_and_append`
- Updates the shared `write_frontier` for cluster visibility

## Frontier and Capability Handling

This section explains the Timely Dataflow frontier and capability semantics used throughout the sink.

### Background: Timely Concepts

| Concept | Description |
|---------|-------------|
| **Capability** | A token that grants permission to produce output at a specific timestamp. Operators cannot emit data at time `t` without holding a capability for `t`. |
| **CapabilitySet** | A collection of capabilities that can be downgraded (moved forward in time) but never upgraded. |
| **Frontier** | An `Antichain<Timestamp>` indicating that no more data will arrive at times *before* the frontier. When a frontier advances past `t`, all data at `t` has been delivered. |
| **Disconnected Input** | An input that doesn't contribute to the operator's output capability. Used when you need to read data without holding back downstream progress. |

### Frontier Types in the Sink

The sink tracks several distinct frontiers:

```
┌─────────────────────────────────────────────────────────────────────┐
│                        Frontier Flow                                │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  input_frontier ──► batch_description_frontier ──► write_frontier   │
│        │                      │                          │          │
│   "All data at              "All batch descs           "All data    │
│    times < F has             at times < F have          committed   │
│    been received"            been minted"               to Iceberg" │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

| Frontier | Tracked In | Meaning |
|----------|------------|---------|
| `input_frontier` | `write_data_files`, `commit_to_iceberg` | All input rows at times before this frontier have been received |
| `batch_description_frontier` | `write_data_files`, `commit_to_iceberg` | All batch descriptions at times before this frontier have been minted |
| `write_frontier` | `commit_to_iceberg` (shared via `Rc<RefCell>`) | All data up to this frontier has been durably committed to Iceberg |

### Capability Management by Operator

#### `init_table_and_extract_schema`

```rust
let [capset]: &mut [_; 1] = caps.try_into().unwrap();
*capset = CapabilitySet::new();  // Immediately drop all capabilities
```

- **Capabilities**: Immediately cleared
- **Behavior**: Pure pass-through; doesn't hold back the dataflow frontier
- **Rationale**: Table initialization is a one-time side effect that shouldn't delay data flow

#### `mint_batch_descriptions`

```rust
let [data_capset, capset]: &mut [_; 2] = caps.try_into().unwrap();
*data_capset = CapabilitySet::new();  // Data output: pass-through
// capset is retained for batch description output
```

- **Two outputs**: Data (pass-through) and batch descriptions (controlled)
- **Data capabilities**: Cleared immediately—data flows through unchanged
- **Batch description capabilities**: Retained and carefully managed

**Capability lifecycle for batch descriptions**:

1. **Initial minting**: When first initialized, mint `INITIAL_DESCRIPTIONS_TO_MINT` batches
   ```rust
   let cap = capset.try_delayed(desc.0.as_option().unwrap())?;
   batch_desc_output.give(&cap, desc);
   ```
   Each description is emitted with a capability delayed to the batch's lower bound.

2. **Downgrade**: After emitting initial batches, downgrade to the newest upper
   ```rust
   capset.downgrade(new_upper);
   ```
   This signals that no batch descriptions at earlier times will be produced.

3. **Sliding window**: As the oldest batch becomes ready, mint a new future batch
   ```rust
   // When observed_frontier >= oldest_upper:
   minted_batches.pop_front();
   minted_batches.push_back(new_batch_description);
   capset.downgrade(new_upper);
   ```

**Why this matters**: The capability set controls the `batch_description_frontier`. Downstream operators use this to know when all batch descriptions for a time range have arrived.

#### `write_data_files`

```rust
let [capset]: &mut [_; 1] = caps.try_into().unwrap();
*capset = CapabilitySet::new();  // Clear output capabilities

let mut batch_desc_input = builder.new_input_for(&batch_desc_input.broadcast(), Pipeline, &output);
let mut input = builder.new_disconnected_input(&input.inner, Pipeline);
```

- **Output capabilities**: Cleared; output timing is driven by stored per-batch capabilities
- **Batch description input**: Connected (contributes to output frontier)
- **Data input**: Disconnected (doesn't hold back output frontier)

**Per-batch capability storage**:
```rust
in_flight_batches: HashMap<
    (Antichain<Timestamp>, Antichain<Timestamp>),
    (Capability<Timestamp>, DeltaWriterType),
>
```

Each in-flight batch stores a capability derived from when its description arrived:
```rust
let prev = in_flight_batches.insert(batch_desc, (cap.delayed(cap.time()), delta_writer));
```

**Batch completion condition**:
```rust
// A batch is ready to close when:
// 1. Its description's frontier has advanced (batch_desc minter moved on)
// 2. All data for the batch has arrived (input frontier past batch upper)
let ready = PartialOrder::less_than(lower, &batch_description_frontier)
         && PartialOrder::less_than(upper, &input_frontier);
```

**Why disconnected input for data?** The data input is disconnected because:
1. We don't want data arrival to hold back the output frontier
2. We track `input_frontier` manually for batch completion logic
3. Output timing is controlled by the stored per-batch capabilities

#### `commit_to_iceberg`

```rust
let mut input = builder.new_disconnected_input(&batch_input, Exchange::new(move |_| hashed_id));
let mut batch_desc_input = builder.new_disconnected_input(batch_desc_input, Exchange::new(move |_| hashed_id));
```

- **No output capabilities**: This operator has no outputs (only side effects)
- **Both inputs disconnected**: Frontiers tracked manually

**Frontier tracking**:
```rust
let mut batch_description_frontier = Antichain::from_elem(Timestamp::minimum());
let mut input_frontier = Antichain::from_elem(Timestamp::minimum());
```

**Batch ready condition**:
```rust
// A batch is ready to commit when all its data files have arrived
let done_batches = batch_descriptions.keys()
    .filter(|(lower, _upper)| PartialOrder::less_than(lower, &input_frontier))
    .cloned()
    .collect();
```

**Write frontier update**:
```rust
// After successful Iceberg commit and persist frontier advance:
write_frontier.borrow_mut().clone_from(&frontier);
```

This shared `Rc<RefCell<Antichain<Timestamp>>>` is visible to the storage controller for progress tracking.

### Batch Description Lifecycle

```
Time ─────────────────────────────────────────────────────────────►

      ┌─────────┐ ┌─────────┐ ┌─────────┐
      │ Batch 1 │ │ Batch 2 │ │ Batch 3 │   ... (sliding window)
      │ [t0,t1) │ │ [t1,t2) │ │ [t2,t3) │
      └─────────┘ └─────────┘ └─────────┘
           │           │           │
           ▼           ▼           ▼
      ┌─────────┐ ┌─────────┐ ┌─────────┐
      │ Writer  │ │ Writer  │ │ Writer  │   (in write_data_files)
      │ + Cap   │ │ + Cap   │ │ + Cap   │
      └─────────┘ └─────────┘ └─────────┘

When input_frontier advances past t1:
  - Batch 1's writer is closed
  - Data files emitted with Batch 1's capability
  - Batch 4 [t3,t4) is minted to maintain window size
```

### Resume and Frontier Recovery

On restart, the minter recovers the frontier from Iceberg snapshots:

```rust
let (resume_upper, resume_version) = match resume {
    Some((f, v)) => (f, v.unwrap_or(0)),
    None => (Antichain::from_elem(Timestamp::minimum()), 0),
};
```

**Catch-up batch**: If resuming, a batch is created from `resume_upper` to the current frontier:
```rust
if PartialOrder::less_than(&resume_upper, &current_upper) {
    let batch_description = (resume_upper.clone(), current_upper.clone());
    batch_descriptions.push(batch_description);
}
```

**Overcompaction check**: The sink fails if the input has compacted beyond the resume point:
```rust
let overcompacted = *resume_upper != [Timestamp::minimum()]
    && PartialOrder::less_than(&resume_upper, &as_of);
```

### Initialization Barrier

The minter waits until the observed frontier advances past both `resume_upper` and `as_of`:

```rust
if PartialOrder::less_equal(&observed_frontier, &resume_upper)
    || PartialOrder::less_equal(&observed_frontier, &as_of) {
    continue;  // Don't mint yet
}
```

This ensures:
1. We don't mint batches for data we'll never see (compacted away)
2. We don't mint batches before we know where to resume from

## SinkRender Implementation

The `IcebergSinkConnection::render_sink` method orchestrates the full pipeline:

1. Opens a persist write handle for frontier tracking
2. Converts `RelationDesc` → Arrow Schema → Iceberg Schema
3. Chains the four operators together
4. Concatenates health status streams from all operators
5. Returns the combined status stream and drop buttons

## Error Handling

All operators use the `build_fallible` pattern which:
- Wraps the operator body in `Box::pin(async move { ... })`
- Returns `anyhow::Result<()>`
- Emits errors on a separate stream
- Converts errors to `HealthStatusMessage` with `halting` status

## Exactly-Once Semantics

The sink achieves exactly-once through:

1. **Frontier tracking**: Each snapshot stores the `mz-frontier` property
2. **Resume on restart**: The minter reads the last frontier from snapshots
3. **Catch-up batches**: Creates a batch from `resume_upper` to current frontier
4. **Fencing**: Sink version prevents older replicas from overwriting newer data
5. **Persist integration**: Advances persist frontier after successful commits

## Limitations

- **Schema evolution**: Not supported; schema changes cause errors
- **Partition spec evolution**: Not supported
- **Compaction**: Only skipped via heuristic (`operation="replace"`)
- **Key columns required**: Equality deletes require key columns to be defined

## File Naming

Generated files follow the pattern:
```
mz_data-{sequence}-{uuid}.parquet
```

Where:
- `mz_data` is the `PARQUET_FILE_PREFIX`
- `sequence` is incremented per file
- `uuid` is generated at writer creation for uniqueness
