// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Iceberg sink implementation.
//!
//! This code renders a [`IcebergSinkConnection`] into a dataflow that writes
//! data to an Iceberg table. The dataflow consists of three operators:
//!
//! ```text
//!        ┏━━━━━━━━━━━━━━┓
//!        ┃   persist    ┃
//!        ┃    source    ┃
//!        ┗━━━━━━┯━━━━━━━┛
//!               │ row data, the input to this module
//!               │
//!        ┏━━━━━━v━━━━━━━┓
//!        ┃     mint     ┃ (single worker)
//!        ┃    batch     ┃ loads/creates the Iceberg table,
//!        ┃ descriptions ┃ determines resume upper
//!        ┗━━━┯━━━━━┯━━━━┛
//!            │     │ batch descriptions (broadcast)
//!       rows │     ├─────────────────────────┐
//!            │     │                         │
//!        ┏━━━v━━━━━v━━━━┓    ╭─────────────╮ │
//!        ┃    write     ┃───>│ S3 / object │ │
//!        ┃  data files  ┃    │   storage   │ │
//!        ┗━━━━━━┯━━━━━━━┛    ╰─────────────╯ │
//!               │ file metadata              │
//!               │                            │
//!        ┏━━━━━━v━━━━━━━━━━━━━━━━━━━━━━━━━━━━v┓
//!        ┃           commit to               ┃ (single worker)
//!        ┃             iceberg               ┃
//!        ┗━━━━━━━━━━━━━━┯━━━━━━━━━━━━━━━━━━━━━┛
//!                      │
//!              ╭───────v───────╮
//!              │ Iceberg table │
//!              │  (snapshots)  │
//!              ╰───────────────╯
//! ```
//! # Minting batch descriptions
//! The "mint batch descriptions" operator is responsible for generating
//! time-based batch boundaries that group writes into Iceberg snapshots.
//! It maintains a sliding window of future batch descriptions so that
//! writers can start processing data even while earlier batches are still being written.
//! Knowing the batch boundaries ahead of time is important because we need to
//! be able to make the claim that all data files written for a given batch
//! include all data up to the upper `t` but not beyond it.
//! This could be trivially achieved by waiting for all data to arrive up to a certain
//! frontier, but that would prevent us from streaming writes out to object storage
//! until the entire batch is complete, which would increase latency and reduce throughput.
//!
//! # Writing data files
//! The "write data files" operator receives rows along with batch descriptions.
//! It matches rows to batches by timestamp; if a batch description hasn't arrived yet,
//! rows are stashed until it does. This allows batches to be minted ahead of data arrival.
//! The operator uses an Iceberg `DeltaWriter` to write Parquet data files
//! (and position delete files if necessary) to object storage.
//! It outputs metadata about the written files along with their batch descriptions
//! for the commit operator to consume.
//!
//! # Committing to Iceberg
//! The "commit to iceberg" operator receives metadata about written data files
//! along with their batch descriptions. It groups files by batch and creates
//! Iceberg snapshots that include all files for each batch. It updates the Iceberg
//! table's metadata to reflect the new snapshots, including updating the
//! `mz-frontier` property to track progress.

use std::cmp::Ordering;
use std::collections::{BTreeMap, VecDeque};
use std::{cell::RefCell, rc::Rc, sync::Arc};

use anyhow::{Context, anyhow};
use arrow::array::Int32Array;
use arrow::datatypes::{DataType, Field};
use arrow::{
    array::RecordBatch, datatypes::Schema as ArrowSchema, datatypes::SchemaRef as ArrowSchemaRef,
};
use differential_dataflow::lattice::Lattice;
use differential_dataflow::{AsCollection, Hashable, VecCollection};
use futures::StreamExt;
use iceberg::ErrorKind;
use iceberg::arrow::{arrow_schema_to_schema, schema_to_arrow_schema};
use iceberg::spec::{
    DataFile, FormatVersion, Snapshot, StructType, read_data_files_from_avro,
    write_data_files_to_avro,
};
use iceberg::spec::{Schema, SchemaRef};
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::DataFileWriter;
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::base_writer::equality_delete_writer::{
    EqualityDeleteFileWriter, EqualityDeleteFileWriterBuilder, EqualityDeleteWriterConfig,
};
use iceberg::writer::base_writer::position_delete_writer::PositionDeleteFileWriter;
use iceberg::writer::base_writer::position_delete_writer::{
    PositionDeleteFileWriterBuilder, PositionDeleteWriterConfig,
};
use iceberg::writer::combined_writer::delta_writer::{DeltaWriter, DeltaWriterBuilder};
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{Catalog, NamespaceIdent, TableCreation, TableIdent};
use itertools::Itertools;
use mz_arrow_util::builder::ArrowBuilder;
use mz_interchange::avro::DiffPair;
use mz_ore::cast::CastFrom;
use mz_ore::error::ErrorExt;
use mz_ore::future::InTask;
use mz_ore::result::ResultExt;
use mz_ore::retry::{Retry, RetryResult};
use mz_persist_client::Diagnostics;
use mz_persist_client::write::WriteHandle;
use mz_persist_types::codec_impls::UnitSchema;
use mz_repr::{Diff, GlobalId, Row, Timestamp};
use mz_storage_types::StorageDiff;
use mz_storage_types::configuration::StorageConfiguration;
use mz_storage_types::controller::CollectionMetadata;
use mz_storage_types::errors::DataflowError;
use mz_storage_types::sinks::{IcebergSinkConnection, StorageSinkDesc};
use mz_storage_types::sources::SourceData;
use mz_timely_util::antichain::AntichainExt;
use mz_timely_util::builder_async::{Event, OperatorBuilder, PressOnDropButton};
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use parquet::file::properties::WriterProperties;
use serde::{Deserialize, Serialize};
use timely::PartialOrder;
use timely::container::CapacityContainerBuilder;
use timely::dataflow::channels::pact::{Exchange, Pipeline};
use timely::dataflow::operators::{Broadcast, CapabilitySet, Concatenate, Map, ToStream};
use timely::dataflow::{Scope, Stream};
use timely::progress::{Antichain, Timestamp as _};

use crate::healthcheck::{HealthStatusMessage, HealthStatusUpdate, StatusNamespace};
use crate::render::sinks::SinkRender;
use crate::storage_state::StorageState;

/// Set the default capacity for the array builders inside the ArrowBuilder. This is the
/// number of items each builder can hold before it needs to allocate more memory.
const DEFAULT_ARRAY_BUILDER_ITEM_CAPACITY: usize = 1024;
/// Set the default buffer capacity for the string and binary array builders inside the
/// ArrowBuilder. This is the number of bytes each builder can hold before it needs to allocate
/// more memory.
const DEFAULT_ARRAY_BUILDER_DATA_CAPACITY: usize = 1024;

/// The prefix for Parquet files written by this sink.
const PARQUET_FILE_PREFIX: &str = "mz_data";
/// The number of batch descriptions to mint ahead of the observed frontier. This determines how
/// many batches we have in-flight at any given time.
const INITIAL_DESCRIPTIONS_TO_MINT: u64 = 3;

type ParquetWriterType = ParquetWriterBuilder<DefaultLocationGenerator, DefaultFileNameGenerator>;
type DeltaWriterType = DeltaWriter<
    DataFileWriter<ParquetWriterType>,
    PositionDeleteFileWriter<ParquetWriterType>,
    EqualityDeleteFileWriter<ParquetWriterType>,
>;

/// Add Parquet field IDs to an Arrow schema. Iceberg requires field IDs in the
/// Parquet metadata for schema evolution tracking.
/// TODO: Support nested data types with proper field IDs.
fn add_field_ids_to_arrow_schema(schema: ArrowSchema) -> ArrowSchema {
    let fields: Vec<Field> = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(i, field)| {
            let mut metadata = field.metadata().clone();
            metadata.insert(PARQUET_FIELD_ID_META_KEY.to_string(), (i + 1).to_string());
            Field::new(field.name(), field.data_type().clone(), field.is_nullable())
                .with_metadata(metadata)
        })
        .collect();

    ArrowSchema::new(fields).with_metadata(schema.metadata().clone())
}

/// Merge Materialize extension metadata into Iceberg's Arrow schema.
/// This uses Iceberg's data types (e.g. Utf8) and field IDs while preserving
/// Materialize's extension names for ArrowBuilder compatibility.
fn merge_materialize_metadata_into_iceberg_schema(
    materialize_arrow_schema: &ArrowSchema,
    iceberg_schema: &Schema,
) -> anyhow::Result<ArrowSchema> {
    // First, convert Iceberg schema to Arrow (this gives us the correct data types)
    let iceberg_arrow_schema = schema_to_arrow_schema(iceberg_schema)
        .context("Failed to convert Iceberg schema to Arrow schema")?;

    // Now merge in the Materialize extension metadata
    let fields: Vec<Field> = iceberg_arrow_schema
        .fields()
        .iter()
        .map(|iceberg_field| {
            // Find the corresponding Materialize field by name to get extension metadata
            let mz_field = materialize_arrow_schema
                .field_with_name(iceberg_field.name())
                .with_context(|| {
                    format!(
                        "Field '{}' not found in Materialize schema",
                        iceberg_field.name()
                    )
                })?;

            // Start with Iceberg field's metadata (which includes field IDs)
            let mut metadata = iceberg_field.metadata().clone();

            // Add Materialize extension name
            if let Some(extension_name) = mz_field.metadata().get("ARROW:extension:name") {
                metadata.insert("ARROW:extension:name".to_string(), extension_name.clone());
            }

            // Use Iceberg's data type and field ID, but add Materialize extension metadata
            Ok(Field::new(
                iceberg_field.name(),
                iceberg_field.data_type().clone(),
                iceberg_field.is_nullable(),
            )
            .with_metadata(metadata))
        })
        .collect::<anyhow::Result<Vec<_>>>()?;

    Ok(ArrowSchema::new(fields).with_metadata(iceberg_arrow_schema.metadata().clone()))
}

async fn reload_table(
    catalog: &dyn Catalog,
    namespace: String,
    table_name: String,
    current_table: Table,
) -> anyhow::Result<Table> {
    let namespace_ident = NamespaceIdent::new(namespace.clone());
    let table_ident = TableIdent::new(namespace_ident, table_name.clone());
    let current_schema = current_table.metadata().current_schema_id();
    let current_partition_spec = current_table.metadata().default_partition_spec_id();

    match catalog.load_table(&table_ident).await {
        Ok(table) => {
            let reloaded_schema = table.metadata().current_schema_id();
            let reloaded_partition_spec = table.metadata().default_partition_spec_id();
            if reloaded_schema != current_schema {
                return Err(anyhow::anyhow!(
                    "Iceberg table '{}' schema changed during operation but schema evolution isn't supported, expected schema ID {}, got {}",
                    table_name,
                    current_schema,
                    reloaded_schema
                ));
            }

            if reloaded_partition_spec != current_partition_spec {
                return Err(anyhow::anyhow!(
                    "Iceberg table '{}' partition spec changed during operation but partition spec evolution isn't supported, expected partition spec ID {}, got {}",
                    table_name,
                    current_partition_spec,
                    reloaded_partition_spec
                ));
            }

            Ok(table)
        }
        Err(err) => Err(err).context("Failed to reload Iceberg table"),
    }
}

/// Load an existing Iceberg table or create it if it doesn't exist.
async fn load_or_create_table(
    catalog: &dyn Catalog,
    namespace: String,
    table_name: String,
    schema: &Schema,
) -> anyhow::Result<iceberg::table::Table> {
    let namespace_ident = NamespaceIdent::new(namespace.clone());
    let table_ident = TableIdent::new(namespace_ident.clone(), table_name.clone());

    // Try to load the table first
    match catalog.load_table(&table_ident).await {
        Ok(table) => {
            // Table exists, return it
            // TODO: Add proper schema evolution/validation to ensure compatibility
            Ok(table)
        }
        Err(err) => {
            if matches!(err.kind(), ErrorKind::TableNotFound { .. }) {
                // Table doesn't exist, create it
                // Note: location is not specified, letting the catalog determine the default location
                // based on its warehouse configuration
                let table_creation = TableCreation::builder()
                    .name(table_name.clone())
                    .schema(schema.clone())
                    // Use unpartitioned spec by default
                    // TODO: Consider making partition spec configurable
                    // .partition_spec(UnboundPartitionSpec::builder().build())
                    .build();

                catalog
                    .create_table(&namespace_ident, table_creation)
                    .await
                    .with_context(|| {
                        format!(
                            "Failed to create Iceberg table '{}' in namespace '{}'",
                            table_name, namespace
                        )
                    })
            } else {
                // Some other error occurred
                Err(err).context("Failed to load Iceberg table")
            }
        }
    }
}

/// Find the most recent Materialize frontier from Iceberg snapshots.
/// We store the frontier in snapshot metadata to track where we left off after restarts.
/// Snapshots with operation="replace" (compactions) don't have our metadata and are skipped.
/// The input slice will be sorted by sequence number in descending order.
fn retrieve_upper_from_snapshots(
    snapshots: &mut [Arc<Snapshot>],
) -> anyhow::Result<Option<(Antichain<Timestamp>, u64)>> {
    snapshots.sort_by(|a, b| Ord::cmp(&b.sequence_number(), &a.sequence_number()));

    for snapshot in snapshots {
        let props = &snapshot.summary().additional_properties;
        if let (Some(frontier_json), Some(sink_version_str)) =
            (props.get("mz-frontier"), props.get("mz-sink-version"))
        {
            let frontier: Vec<Timestamp> = serde_json::from_str(frontier_json)
                .context("Failed to deserialize frontier from snapshot properties")?;
            let frontier = Antichain::from_iter(frontier);

            let sink_version = sink_version_str
                .parse::<u64>()
                .context("Failed to parse mz-sink-version from snapshot properties")?;

            return Ok(Some((frontier, sink_version)));
        }
        if snapshot.summary().operation.as_str() != "replace" {
            // This is a bad heuristic, but we have no real other way to identify compactions
            // right now other than assume they will be the only operation writing "replace" operations.
            // That means if we find a snapshot with some other operation, but no mz-frontier, we are in an
            // inconsistent state and have to error out.
            anyhow::bail!(
                "Iceberg table is in an inconsistent state: snapshot {} has operation '{}' but is missing 'mz-frontier' property. Schema or partition spec evolution is not supported.",
                snapshot.snapshot_id(),
                snapshot.summary().operation.as_str(),
            );
        }
    }

    Ok(None)
}

/// Convert a Materialize RelationDesc into Arrow and Iceberg schemas.
/// Returns the Arrow schema (with field IDs) for writing Parquet files,
/// and the Iceberg schema for table creation/validation.
fn relation_desc_to_iceberg_schema(
    desc: &mz_repr::RelationDesc,
) -> anyhow::Result<(ArrowSchema, SchemaRef)> {
    let arrow_schema = mz_arrow_util::builder::desc_to_schema(desc)
        .context("Failed to convert RelationDesc to Arrow schema")?;

    let arrow_schema_with_ids = add_field_ids_to_arrow_schema(arrow_schema);

    let iceberg_schema = arrow_schema_to_schema(&arrow_schema_with_ids)
        .context("Failed to convert Arrow schema to Iceberg schema")?;

    Ok((arrow_schema_with_ids, Arc::new(iceberg_schema)))
}

/// Build a new Arrow schema by adding an __op column to the existing schema.
fn build_schema_with_op_column(schema: &ArrowSchema) -> ArrowSchema {
    let mut fields: Vec<Field> = schema.fields().iter().map(|f| f.as_ref().clone()).collect();
    fields.push(Field::new("__op", DataType::Int32, false));
    ArrowSchema::new(fields)
}

/// Convert a Materialize DiffPair into an Arrow RecordBatch with an __op column.
/// The __op column indicates whether each row is an insert (1) or delete (-1), which
/// the DeltaWriter uses to generate the appropriate Iceberg data/delete files.
fn row_to_recordbatch(
    row: DiffPair<Row>,
    schema: ArrowSchemaRef,
    schema_with_op: ArrowSchemaRef,
) -> anyhow::Result<RecordBatch> {
    let mut builder = ArrowBuilder::new_with_schema(
        Arc::clone(&schema),
        DEFAULT_ARRAY_BUILDER_ITEM_CAPACITY,
        DEFAULT_ARRAY_BUILDER_DATA_CAPACITY,
    )
    .context("Failed to create builder")?;

    let mut op_values = Vec::new();

    if let Some(before) = row.before {
        builder
            .add_row(&before)
            .context("Failed to add delete row to builder")?;
        op_values.push(-1i32); // Delete operation
    }
    if let Some(after) = row.after {
        builder
            .add_row(&after)
            .context("Failed to add insert row to builder")?;
        op_values.push(1i32); // Insert operation
    }

    let batch = builder
        .to_record_batch()
        .context("Failed to create record batch")?;

    let mut columns = Vec::with_capacity(batch.columns().len() + 1);
    columns.extend_from_slice(batch.columns());
    let op_column = Arc::new(Int32Array::from(op_values));
    columns.push(op_column);

    let batch_with_op = RecordBatch::try_new(schema_with_op, columns)
        .context("Failed to create batch with op column")?;
    Ok(batch_with_op)
}

/// Generate time-based batch boundaries for grouping writes into Iceberg snapshots.
/// Batches are minted with configurable windows to balance write efficiency with latency.
/// We maintain a sliding window of future batch descriptions so writers can start
/// processing data even while earlier batches are still being written.
fn mint_batch_descriptions<G, D>(
    name: String,
    sink_id: GlobalId,
    input: &VecCollection<G, D, Diff>,
    sink: &StorageSinkDesc<CollectionMetadata, Timestamp>,
    connection: IcebergSinkConnection,
    storage_configuration: StorageConfiguration,
    initial_schema: SchemaRef,
) -> (
    VecCollection<G, D, Diff>,
    Stream<G, (Antichain<Timestamp>, Antichain<Timestamp>)>,
    Stream<G, HealthStatusMessage>,
    PressOnDropButton,
)
where
    G: Scope<Timestamp = Timestamp>,
    D: Clone + 'static,
{
    let scope = input.scope();
    let name_for_error = name.clone();
    let mut builder = OperatorBuilder::new(name, scope.clone());
    let sink_version = sink.version;

    let hashed_id = sink_id.hashed();
    let is_active_worker = usize::cast_from(hashed_id) % scope.peers() == scope.index();
    let (output, output_stream) = builder.new_output();
    let (batch_desc_output, batch_desc_stream) =
        builder.new_output::<CapacityContainerBuilder<_>>();
    let mut input =
        builder.new_input_for_many(&input.inner, Pipeline, [&output, &batch_desc_output]);

    let as_of = sink.as_of.clone();
    let commit_interval = sink
        .commit_interval
        .expect("the planner should have enforced this")
        .clone();

    let (button, errors): (_, Stream<G, Rc<anyhow::Error>>) = builder.build_fallible(move |caps| {
        Box::pin(async move {
            let [data_capset, capset]: &mut [_; 2] = caps.try_into().unwrap();
            *data_capset = CapabilitySet::new();

            if !is_active_worker {
                *capset = CapabilitySet::new();
                *data_capset = CapabilitySet::new();
                while let Some(event) = input.next().await {
                    match event {
                        Event::Data([output_cap, _], mut data) => {
                            output.give_container(&output_cap, &mut data);
                        }
                        Event::Progress(_) => {}
                    }
                }
                return Ok(());
            }

            let catalog = connection
                .catalog_connection
                .connect(&storage_configuration, InTask::Yes)
                .await
                .context("Failed to connect to iceberg catalog")?;

            let table = load_or_create_table(
                catalog.as_ref(),
                connection.namespace.clone(),
                connection.table.clone(),
                initial_schema.as_ref(),
            )
            .await?;

            let mut snapshots: Vec<_> = table.metadata().snapshots().cloned().collect();
            let resume = retrieve_upper_from_snapshots(&mut snapshots)?;
            let (resume_upper, resume_version) = match resume {
                Some((f, v)) => (f, v),
                None => (Antichain::from_elem(Timestamp::minimum()), 0),
            };

            // The input has overcompacted if
            let overcompacted =
                // ..we have made some progress in the past
                *resume_upper != [Timestamp::minimum()] &&
                // ..but the since frontier is now beyond that
                PartialOrder::less_than(&resume_upper, &as_of);

            if overcompacted {
                let err = format!(
                    "{name_for_error}: input compacted past resume upper: as_of {}, resume_upper: {}",
                    as_of.pretty(),
                    resume_upper.pretty()
                );
                // This would normally be an assertion but because it can happen after a
                // Materialize backup/restore we log an error so that it appears on Sentry but
                // leaves the rest of the objects in the cluster unaffected.
                return Err(anyhow::anyhow!("{err}"));
            };

            if resume_version > sink_version {
                anyhow::bail!("Fenced off by newer sink version: resume_version {}, sink_version {}", resume_version, sink_version);
            }

            let mut initialized = false;
            let mut observed_frontier;
            // Track minted batches to maintain a sliding window of open batch descriptions.
            // This is needed to know when to retire old batches and mint new ones.
            // It's "sortedness" is derived from the monotonicity of batch descriptions,
            // and the fact that we only ever push new descriptions to the back and pop from the front.
            let mut minted_batches = VecDeque::new();
            loop {
                if let Some(event) = input.next().await {
                    match event {
                        Event::Data([output_cap, _], mut data) => {
                            output.give_container(&output_cap, &mut data);
                            continue;
                        }
                        Event::Progress(frontier) => {
                            observed_frontier = frontier;
                        }
                    }
                } else {
                    return Ok(());
                }

                if !initialized {
                    // We only start minting after we've reached as_of and resume_upper to avoid
                    // minting batches that would be immediately skipped.
                    if PartialOrder::less_equal(&observed_frontier, &resume_upper) || PartialOrder::less_equal(&observed_frontier, &as_of) {
                        continue;
                    }

                    let mut batch_descriptions = vec![];
                    let mut current_upper = observed_frontier.clone();
                    let current_upper_ts = current_upper.as_option().unwrap().clone();

                    // If we're resuming, create a catch-up batch from resume_upper to current frontier
                    if PartialOrder::less_than(&resume_upper, &current_upper) {
                        let batch_description = (resume_upper.clone(), current_upper.clone());
                        batch_descriptions.push(batch_description);
                    }

                    // Mint initial future batch descriptions at configurable intervals
                    for i in 1..INITIAL_DESCRIPTIONS_TO_MINT + 1 {
                        let duration_millis = commit_interval.as_millis()
                            .checked_mul(u128::from(i))
                            .expect("commit interval multiplication overflow");
                        let duration_ts = Timestamp::new(u64::try_from(duration_millis)
                            .expect("commit interval too large for u64"));
                        let desired_batch_upper = Antichain::from_elem(current_upper_ts.step_forward_by(&duration_ts));

                        let batch_description = (current_upper.clone(), desired_batch_upper);
                        current_upper = batch_description.1.clone();
                        batch_descriptions.push(batch_description);
                    }

                    minted_batches.extend(batch_descriptions.clone());

                    for desc in batch_descriptions {
                        batch_desc_output.give(&capset[0], desc);
                    }

                    capset.downgrade(current_upper);

                    initialized = true;
                } else {
                    // Maintain a sliding window: when the oldest batch becomes ready, retire it
                    // and mint a new future batch to keep the pipeline full
                    while let Some(oldest_desc) = minted_batches.front() {
                        let oldest_upper = &oldest_desc.1;
                        if !PartialOrder::less_equal(oldest_upper, &observed_frontier) {
                            break;
                        }

                        let newest_upper = minted_batches.back().unwrap().1.clone();
                        let new_lower = newest_upper.clone();
                        let duration_ts = Timestamp::new(commit_interval.as_millis()
                            .try_into()
                            .expect("commit interval too large for u64"));
                        let new_upper = Antichain::from_elem(newest_upper
                            .as_option()
                            .unwrap()
                            .step_forward_by(&duration_ts));

                        let new_batch_description = (new_lower.clone(), new_upper.clone());
                        minted_batches.pop_front();
                        minted_batches.push_back(new_batch_description.clone());

                        batch_desc_output.give(&capset[0], new_batch_description);

                        capset.downgrade(new_upper);
                    }
                }
            }
        })
    });

    let statuses = errors.map(|error| HealthStatusMessage {
        id: None,
        update: HealthStatusUpdate::halting(format!("{}", error.display_with_causes()), None),
        namespace: StatusNamespace::Iceberg,
    });
    (
        output_stream.as_collection(),
        batch_desc_stream,
        statuses,
        button.press_on_drop(),
    )
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(try_from = "AvroDataFile", into = "AvroDataFile")]
struct SerializableDataFile {
    pub data_file: DataFile,
    pub schema: Schema,
}

/// A wrapper around Iceberg's DataFile that implements Serialize and Deserialize.
/// This is slightly complicated by the fact that Iceberg's DataFile doesn't implement
/// these traits directly, so we serialize to/from Avro bytes (which Iceberg supports natively).
/// The avro ser(de) also requires the Iceberg schema to be provided, so we include that as well.
/// It is distinctly possible that this is overkill, but it avoids re-implementing
/// Iceberg's serialization logic here.
/// If at some point this becomes a serious overhead, we can revisit this decision.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct AvroDataFile {
    pub data_file: Vec<u8>,
    pub schema: Schema,
}

impl From<SerializableDataFile> for AvroDataFile {
    fn from(value: SerializableDataFile) -> Self {
        let mut data_file = Vec::new();
        write_data_files_to_avro(
            &mut data_file,
            [value.data_file],
            &StructType::new(vec![]),
            FormatVersion::V2,
        )
        .expect("serialization into buffer");
        AvroDataFile {
            data_file,
            schema: value.schema,
        }
    }
}

impl TryFrom<AvroDataFile> for SerializableDataFile {
    type Error = String;

    fn try_from(value: AvroDataFile) -> Result<Self, Self::Error> {
        let data_files = read_data_files_from_avro(
            &mut &*value.data_file,
            &value.schema,
            0,
            &StructType::new(vec![]),
            FormatVersion::V2,
        )
        .map_err_to_string_with_causes()?;
        let Some(data_file) = data_files.into_iter().next() else {
            return Err("No DataFile found in Avro data".into());
        };
        Ok(SerializableDataFile {
            data_file,
            schema: value.schema,
        })
    }
}

/// A DataFile along with its associated batch description (lower and upper bounds).
#[derive(Clone, Debug, Serialize, Deserialize)]
struct BoundedDataFile {
    pub data_file: SerializableDataFile,
    pub batch_desc: (Antichain<Timestamp>, Antichain<Timestamp>),
}

impl BoundedDataFile {
    pub fn new(
        file: DataFile,
        schema: Schema,
        batch_desc: (Antichain<Timestamp>, Antichain<Timestamp>),
    ) -> Self {
        Self {
            data_file: SerializableDataFile {
                data_file: file,
                schema,
            },
            batch_desc,
        }
    }

    pub fn batch_desc(&self) -> &(Antichain<Timestamp>, Antichain<Timestamp>) {
        &self.batch_desc
    }

    pub fn data_file(&self) -> &DataFile {
        &self.data_file.data_file
    }

    pub fn into_data_file(self) -> DataFile {
        self.data_file.data_file
    }
}

/// A set of DataFiles along with their associated batch descriptions.
#[derive(Clone, Debug, Default)]
struct BoundedDataFileSet {
    pub data_files: Vec<BoundedDataFile>,
}

/// Write rows into Parquet data files bounded by batch descriptions.
/// Rows are matched to batches by timestamp; if a batch description hasn't arrived yet,
/// rows are stashed until it does. This allows batches to be minted ahead of data arrival.
fn write_data_files<G>(
    name: String,
    input: VecCollection<G, (Option<Row>, DiffPair<Row>), Diff>,
    batch_desc_input: &Stream<G, (Antichain<Timestamp>, Antichain<Timestamp>)>,
    connection: IcebergSinkConnection,
    storage_configuration: StorageConfiguration,
    materialize_arrow_schema: Arc<ArrowSchema>,
) -> (
    Stream<G, BoundedDataFile>,
    Stream<G, HealthStatusMessage>,
    PressOnDropButton,
)
where
    G: Scope<Timestamp = Timestamp>,
{
    let scope = input.scope();
    let mut builder = OperatorBuilder::new(name, scope.clone());

    let (output, output_stream) = builder.new_output::<CapacityContainerBuilder<_>>();

    let mut batch_desc_input =
        builder.new_input_for(&batch_desc_input.broadcast(), Pipeline, &output);
    let mut input = builder.new_disconnected_input(&input.inner, Pipeline);

    let (button, errors) = builder.build_fallible(|caps| {
        Box::pin(async move {
            let [capset]: &mut [_; 1] = caps.try_into().unwrap();
            let catalog = connection
                .catalog_connection
                .connect(&storage_configuration, InTask::Yes)
                .await
                .context("Failed to connect to iceberg catalog")?;

            let namespace_ident = NamespaceIdent::new(connection.namespace.clone());
            let table_ident = TableIdent::new(namespace_ident, connection.table.clone());
            let table = catalog
                .load_table(&table_ident)
                .await
                .context("Failed to load Iceberg table")?;

            let table_metadata = table.metadata().clone();
            let current_schema = Arc::clone(table_metadata.current_schema());

            let arrow_schema = Arc::new(
                merge_materialize_metadata_into_iceberg_schema(
                    materialize_arrow_schema.as_ref(),
                    current_schema.as_ref(),
                )
                .context("Failed to merge Materialize metadata into Iceberg schema")?,
            );

            let schema_with_op = Arc::new(build_schema_with_op_column(&arrow_schema));

            // WORKAROUND: S3 Tables catalog incorrectly sets location to the metadata file path
            // instead of the warehouse root. Strip off the /metadata/*.metadata.json suffix.
            // No clear way to detect this properly right now, so we use heuristics.
            let location = table_metadata.location();
            let corrected_location = match location.rsplit_once("/metadata/") {
                Some((a, b)) if b.ends_with(".metadata.json") => a,
                _ => location,
            };

            let data_location = format!("{}/data", corrected_location);
            let location_generator = DefaultLocationGenerator::with_data_location(data_location);

            // Add a unique suffix to avoid filename collisions across restarts and workers
            let unique_suffix = format!("-{}", uuid::Uuid::new_v4());
            let file_name_generator = DefaultFileNameGenerator::new(
                PARQUET_FILE_PREFIX.to_string(),
                Some(unique_suffix),
                iceberg::spec::DataFileFormat::Parquet,
            );

            let file_io = table.file_io().clone();

            let Some((_, equality_indices)) = connection.key_desc_and_indices else {
                return Err(anyhow::anyhow!(
                    "Iceberg sink requires key columns for equality deletes"
                ));
            };

            let equality_ids: Vec<i32> = equality_indices
                .iter()
                .map(|u| i32::try_from(*u).map(|v| v + 1))
                .collect::<Result<Vec<i32>, _>>()
                .context("Failed to convert equality index to i32 (index too large)")?;

            let writer_properties = WriterProperties::new();

            let create_delta_writer = || async {
                let data_parquet_writer = ParquetWriterBuilder::new(
                    writer_properties.clone(),
                    Arc::clone(&current_schema),
                    None,
                    file_io.clone(),
                    location_generator.clone(),
                    file_name_generator.clone(),
                );
                let data_writer_builder = DataFileWriterBuilder::new(data_parquet_writer, None, 0);

                let pos_arrow_schema = PositionDeleteWriterConfig::arrow_schema();
                let pos_schema =
                    Arc::new(arrow_schema_to_schema(&pos_arrow_schema).context(
                        "Failed to convert position delete Arrow schema to Iceberg schema",
                    )?);
                let pos_config = PositionDeleteWriterConfig::new(None, 0, None);
                let pos_parquet_writer = ParquetWriterBuilder::new(
                    writer_properties.clone(),
                    pos_schema,
                    None,
                    file_io.clone(),
                    location_generator.clone(),
                    file_name_generator.clone(),
                );
                let pos_delete_writer_builder =
                    PositionDeleteFileWriterBuilder::new(pos_parquet_writer, pos_config);

                let eq_config = EqualityDeleteWriterConfig::new(
                    equality_ids.clone(),
                    Arc::clone(&current_schema),
                    None,
                    0,
                )
                .context("Failed to create EqualityDeleteWriterConfig")?;

                let eq_schema = Arc::new(
                    arrow_schema_to_schema(eq_config.projected_arrow_schema_ref()).context(
                        "Failed to convert equality delete Arrow schema to Iceberg schema",
                    )?,
                );

                let eq_parquet_writer = ParquetWriterBuilder::new(
                    writer_properties.clone(),
                    eq_schema,
                    None,
                    file_io.clone(),
                    location_generator.clone(),
                    file_name_generator.clone(),
                );
                let eq_delete_writer_builder =
                    EqualityDeleteFileWriterBuilder::new(eq_parquet_writer, eq_config);

                DeltaWriterBuilder::new(
                    data_writer_builder,
                    pos_delete_writer_builder,
                    eq_delete_writer_builder,
                    equality_ids.clone(),
                )
                .build()
                .await
                .context("Failed to create DeltaWriter")
            };

            // Rows can arrive before their batch description due to dataflow parallelism.
            // Stash them until we know which batch they belong to.
            let mut stashed_rows: BTreeMap<Timestamp, Vec<(Option<Row>, DiffPair<Row>)>> =
                BTreeMap::new();

            // Track batches currently being written. When a row arrives, we check if it belongs
            // to an in-flight batch. When frontiers advance past a batch's upper, we close the
            // writer and emit its data files downstream.
            // Antichains don't implement Ord, so we use a HashMap with tuple keys instead.
            #[allow(clippy::disallowed_types)]
            let mut in_flight_batches: std::collections::HashMap<
                (Antichain<Timestamp>, Antichain<Timestamp>),
                DeltaWriterType,
            > = std::collections::HashMap::new();

            let mut batch_description_frontier = Antichain::from_elem(Timestamp::minimum());
            let mut processed_batch_description_frontier =
                Antichain::from_elem(Timestamp::minimum());
            let mut input_frontier = Antichain::from_elem(Timestamp::minimum());
            let mut processed_input_frontier = Antichain::from_elem(Timestamp::minimum());

            while !(batch_description_frontier.is_empty() && input_frontier.is_empty()) {
                tokio::select! {
                    _ = batch_desc_input.ready() => {},
                    _ = input.ready() => {}
                }

                while let Some(event) = batch_desc_input.next_sync() {
                    match event {
                        Event::Data(_cap, data) => {
                            for batch_desc in data {
                                let (lower, upper) = &batch_desc;
                                let mut delta_writer = create_delta_writer().await?;
                                // Drain any stashed rows that belong to this batch
                                let row_ts_keys: Vec<_> = stashed_rows.keys().cloned().collect();
                                for row_ts in row_ts_keys {
                                    let ts = Antichain::from_elem(row_ts.clone());
                                    if PartialOrder::less_equal(lower, &ts)
                                        && PartialOrder::less_than(&ts, upper)
                                    {
                                        if let Some(rows) = stashed_rows.remove(&row_ts) {
                                            for (_row, diff_pair) in rows {
                                                let record_batch = row_to_recordbatch(
                                                    diff_pair.clone(),
                                                    Arc::clone(&arrow_schema),
                                                    Arc::clone(&schema_with_op),
                                                )
                                                .context("failed to convert row to recordbatch")?;
                                                delta_writer.write(record_batch).await.context(
                                                    "Failed to write row to DeltaWriter",
                                                )?;
                                            }
                                        }
                                    }
                                }
                                let prev =
                                    in_flight_batches.insert(batch_desc.clone(), delta_writer);
                                if prev.is_some() {
                                    anyhow::bail!(
                                        "Duplicate batch description received for description {:?}",
                                        batch_desc
                                    );
                                }
                            }
                        }
                        Event::Progress(frontier) => {
                            batch_description_frontier = frontier;
                        }
                    }
                }

                let ready_events = std::iter::from_fn(|| input.next_sync()).collect_vec();
                for event in ready_events {
                    match event {
                        Event::Data(_cap, data) => {
                            for ((row, diff_pair), ts, _diff) in data {
                                let row_ts = ts.clone();
                                let ts_antichain = Antichain::from_elem(row_ts.clone());
                                let mut written = false;
                                // Try writing the row to any in-flight batch it belongs to...
                                for (batch_desc, delta_writer) in in_flight_batches.iter_mut() {
                                    let (lower, upper) = batch_desc;
                                    if PartialOrder::less_equal(lower, &ts_antichain)
                                        && PartialOrder::less_than(&ts_antichain, upper)
                                    {
                                        let record_batch = row_to_recordbatch(
                                            diff_pair.clone(),
                                            Arc::clone(&arrow_schema),
                                            Arc::clone(&schema_with_op),
                                        )
                                        .context("failed to convert row to recordbatch")?;
                                        delta_writer
                                            .write(record_batch)
                                            .await
                                            .context("Failed to write row to DeltaWriter")?;
                                        written = true;
                                        break;
                                    }
                                }
                                if !written {
                                    // ...otherwise stash it for later
                                    let entry = stashed_rows.entry(row_ts).or_default();
                                    entry.push((row, diff_pair));
                                }
                            }
                        }
                        Event::Progress(frontier) => {
                            input_frontier = frontier;
                        }
                    }
                }

                // Check if frontiers have advanced, which may unlock batches ready to close
                if PartialOrder::less_equal(
                    &processed_batch_description_frontier,
                    &batch_description_frontier,
                ) || PartialOrder::less_than(&processed_input_frontier, &input_frontier)
                {
                    // Close batches whose upper is now in the past
                    let ready_batches: Vec<_> = in_flight_batches
                        .extract_if(|(lower, upper), _| {
                            PartialOrder::less_than(lower, &batch_description_frontier)
                                && PartialOrder::less_than(upper, &input_frontier)
                        })
                        .collect();

                    if !ready_batches.is_empty() {
                        let mut max_upper = Antichain::from_elem(Timestamp::minimum());
                        for (desc, mut delta_writer) in ready_batches {
                            let data_files = delta_writer
                                .close()
                                .await
                                .context("Failed to close DeltaWriter")?;
                            for data_file in data_files {
                                let file = BoundedDataFile::new(
                                    data_file,
                                    current_schema.as_ref().clone(),
                                    desc.clone(),
                                );
                                output.give(&capset[0], file);
                            }

                            max_upper = max_upper.join(&desc.1);
                        }

                        capset.downgrade(max_upper);
                    }
                    processed_batch_description_frontier.clone_from(&batch_description_frontier);
                    processed_input_frontier.clone_from(&input_frontier);
                }
            }
            Ok(())
        })
    });

    let statuses = errors.map(|error| HealthStatusMessage {
        id: None,
        update: HealthStatusUpdate::halting(format!("{}", error.display_with_causes()), None),
        namespace: StatusNamespace::Iceberg,
    });
    (output_stream, statuses, button.press_on_drop())
}

/// Commit completed batches to Iceberg as snapshots.
/// Batches are committed in timestamp order to ensure strong consistency guarantees downstream.
/// Each snapshot includes the Materialize frontier in its metadata for resume support.
fn commit_to_iceberg<G>(
    name: String,
    sink_id: GlobalId,
    sink_version: u64,
    batch_input: &Stream<G, BoundedDataFile>,
    batch_desc_input: &Stream<G, (Antichain<Timestamp>, Antichain<Timestamp>)>,
    write_frontier: Rc<RefCell<Antichain<Timestamp>>>,
    connection: IcebergSinkConnection,
    storage_configuration: StorageConfiguration,
    write_handle: impl Future<
        Output = anyhow::Result<WriteHandle<SourceData, (), Timestamp, StorageDiff>>,
    > + 'static,
) -> (Stream<G, HealthStatusMessage>, PressOnDropButton)
where
    G: Scope<Timestamp = Timestamp>,
{
    let scope = batch_input.scope();
    let mut builder = OperatorBuilder::new(name, scope.clone());

    let hashed_id = sink_id.hashed();
    let is_active_worker = usize::cast_from(hashed_id) % scope.peers() == scope.index();

    let mut input = builder.new_disconnected_input(batch_input, Exchange::new(move |_| hashed_id));
    let mut batch_desc_input =
        builder.new_disconnected_input(batch_desc_input, Exchange::new(move |_| hashed_id));

    let (button, errors) = builder.build_fallible(move |_caps| {
        Box::pin(async move {
            if !is_active_worker {
                write_frontier.borrow_mut().clear();
                return Ok(());
            }

            let catalog = connection
                .catalog_connection
                .connect(&storage_configuration, InTask::Yes)
                .await
                .context("Failed to connect to iceberg catalog")?;

            let mut write_handle = write_handle.await?;

            let namespace_ident = NamespaceIdent::new(connection.namespace.clone());
            let table_ident = TableIdent::new(namespace_ident, connection.table.clone());
            let mut table = catalog
                .load_table(&table_ident)
                .await
                .context("Failed to load Iceberg table")?;

            #[allow(clippy::disallowed_types)]
            let mut batch_descriptions: std::collections::HashMap<
                (Antichain<Timestamp>, Antichain<Timestamp>),
                BoundedDataFileSet,
            > = std::collections::HashMap::new();


            let mut batch_description_frontier = Antichain::from_elem(Timestamp::minimum());
            let mut input_frontier = Antichain::from_elem(Timestamp::minimum());

            while !(batch_description_frontier.is_empty() && input_frontier.is_empty()) {
                tokio::select! {
                    _ = batch_desc_input.ready() => {},
                    _ = input.ready() => {}
                }

                while let Some(event) = batch_desc_input.next_sync() {
                    match event {
                        Event::Data(_cap, data) => {
                            for batch_desc in data {
                                let prev = batch_descriptions.insert(batch_desc, BoundedDataFileSet { data_files: vec![] });
                                if let Some(prev) = prev {
                                    anyhow::bail!(
                                        "Duplicate batch description received in commit operator: {:?}",
                                        prev
                                    );
                                }
                            }
                        }
                        Event::Progress(frontier) => {
                            batch_description_frontier = frontier;
                        }
                    }
                }

                let ready_events = std::iter::from_fn(|| input.next_sync()).collect_vec();
                for event in ready_events {
                    match event {
                        Event::Data(_cap, data) => {
                            for bounded_data_file in data {
                                let entry = batch_descriptions.entry(bounded_data_file.batch_desc().clone()).or_default();
                                entry.data_files.push(bounded_data_file);
                            }
                        }
                        Event::Progress(frontier) => {
                            input_frontier = frontier;
                        }
                    }
                }

                let mut done_batches: Vec<_> = batch_descriptions
                    .keys()
                    .filter(|(lower, _upper)| {
                        PartialOrder::less_than(lower, &input_frontier)
                    })
                    .cloned()
                    .collect();

                // Commit batches in timestamp order to maintain consistency
                done_batches.sort_by(|a, b| {
                    if PartialOrder::less_than(a, b) {
                        Ordering::Less
                    } else if PartialOrder::less_than(b, a) {
                        Ordering::Greater
                    } else {
                        Ordering::Equal
                    }
                });

                for batch in done_batches {
                    let file_set = batch_descriptions.remove(&batch).unwrap();

                    let mut data_files = vec![];
                    let mut delete_files = vec![];
                    for file in file_set.data_files {
                        match file.data_file().content_type() {
                            iceberg::spec::DataContentType::Data => {
                                data_files.push(file.into_data_file());
                            }
                            iceberg::spec::DataContentType::PositionDeletes |
                            iceberg::spec::DataContentType::EqualityDeletes => {
                                delete_files.push(file.into_data_file());
                            }
                        }
                    }

                    let frontier = batch.1.clone();
                    let tx = Transaction::new(&table);

                    let frontier_json = serde_json::to_string(&frontier.elements())
                        .context("Failed to serialize frontier to JSON")?;

                    // Store the frontier in snapshot metadata so we can resume from this point
                    let mut action = tx.row_delta().set_snapshot_properties(
                        vec![
                            ("mz-sink-id".to_string(), sink_id.to_string()),
                            ("mz-frontier".to_string(), frontier_json),
                            ("mz-sink-version".to_string(), sink_version.to_string()),
                        ].into_iter().collect()
                    );

                    if !data_files.is_empty() || !delete_files.is_empty() {
                        action = action.add_data_files(data_files).add_delete_files(delete_files);
                    }

                    let tx = action.apply(tx).context(
                        "Failed to apply data file addition to iceberg table transaction",
                    )?;

                    table = Retry::default().max_tries(5).retry_async(|_| async {
                        let new_table = tx.clone().commit(catalog.as_ref()).await;
                        match new_table {
                            Err(e) if matches!(e.kind(), ErrorKind::CatalogCommitConflicts) => {
                                let table = reload_table(
                                    catalog.as_ref(),
                                    connection.namespace.clone(),
                                    connection.table.clone(),
                                    table.clone(),
                                ).await;
                                let table = match table {
                                    Ok(table) => table,
                                    Err(e) => return RetryResult::RetryableErr(anyhow!(e)),
                                };

                                let mut snapshots: Vec<_> = table.metadata().snapshots().cloned().collect();
                                let last = retrieve_upper_from_snapshots(&mut snapshots);
                                let last = match last {
                                    Ok(val) => val,
                                    Err(e) => return RetryResult::RetryableErr(anyhow!(e)),
                                };

                                // Check if another writer has advanced the frontier beyond ours (fencing check)
                                if let Some((last_frontier, _last_version)) = last {
                                    if PartialOrder::less_equal(&frontier, &last_frontier) {
                                        return RetryResult::FatalErr(anyhow!(
                                            "Iceberg table '{}' has been modified by another writer. Current frontier: {:?}, last frontier: {:?}.",
                                            connection.table,
                                            frontier,
                                            last_frontier,
                                        ));
                                    }
                                }

                                RetryResult::Ok(table)
                            }
                            Err(e) => RetryResult::RetryableErr(anyhow!(e)),
                            Ok(table) => RetryResult::Ok(table)
                        }
                    }).await.context("failed to commit to iceberg")?;

                    let mut expect_upper = write_handle.shared_upper();
                    loop {
                        if PartialOrder::less_equal(&frontier, &expect_upper) {
                            // The frontier has already been advanced as far as necessary.
                            break;
                        }

                        const EMPTY: &[((SourceData, ()), Timestamp, StorageDiff)] = &[];
                        match write_handle
                            .compare_and_append(EMPTY, expect_upper, frontier.clone())
                            .await
                            .expect("valid usage")
                        {
                            Ok(()) => break,
                            Err(mismatch) => {
                                expect_upper = mismatch.current;
                            }
                        }
                    }
                    write_frontier.borrow_mut().clone_from(&frontier);

                }
            }


            Ok(())
        })
    });

    let statuses = errors.map(|error| HealthStatusMessage {
        id: None,
        update: HealthStatusUpdate::halting(format!("{}", error.display_with_causes()), None),
        namespace: StatusNamespace::Iceberg,
    });

    (statuses, button.press_on_drop())
}

impl<G: Scope<Timestamp = Timestamp>> SinkRender<G> for IcebergSinkConnection {
    fn get_key_indices(&self) -> Option<&[usize]> {
        self.key_desc_and_indices
            .as_ref()
            .map(|(_, indices)| indices.as_slice())
    }

    fn get_relation_key_indices(&self) -> Option<&[usize]> {
        self.relation_key_indices.as_deref()
    }

    fn render_sink(
        &self,
        storage_state: &mut StorageState,
        sink: &StorageSinkDesc<CollectionMetadata, Timestamp>,
        sink_id: GlobalId,
        input: VecCollection<G, (Option<Row>, DiffPair<Row>), Diff>,
        _err_collection: VecCollection<G, DataflowError, Diff>,
    ) -> (Stream<G, HealthStatusMessage>, Vec<PressOnDropButton>) {
        let mut scope = input.scope();

        let write_handle = {
            let persist = Arc::clone(&storage_state.persist_clients);
            let shard_meta = sink.to_storage_metadata.clone();
            async move {
                let client = persist.open(shard_meta.persist_location).await?;
                let handle = client
                    .open_writer(
                        shard_meta.data_shard,
                        Arc::new(shard_meta.relation_desc),
                        Arc::new(UnitSchema),
                        Diagnostics::from_purpose("sink handle"),
                    )
                    .await?;
                Ok(handle)
            }
        };

        let write_frontier = Rc::new(RefCell::new(Antichain::from_elem(Timestamp::minimum())));
        storage_state
            .sink_write_frontiers
            .insert(sink_id, Rc::clone(&write_frontier));

        let (arrow_schema_with_ids, iceberg_schema) =
            match relation_desc_to_iceberg_schema(&sink.from_desc) {
                Ok(schemas) => schemas,
                Err(err) => {
                    let error_stream = std::iter::once(HealthStatusMessage {
                        id: None,
                        update: HealthStatusUpdate::halting(
                            format!("{}", err.display_with_causes()),
                            None,
                        ),
                        namespace: StatusNamespace::Iceberg,
                    })
                    .to_stream(&mut scope);
                    return (error_stream, vec![]);
                }
            };

        let connection_for_minter = self.clone();
        let (minted_input, batch_descriptions, mint_status, mint_button) = mint_batch_descriptions(
            format!("{sink_id}-iceberg-mint"),
            sink_id,
            &input,
            sink,
            connection_for_minter,
            storage_state.storage_configuration.clone(),
            Arc::clone(&iceberg_schema),
        );

        let connection_for_writer = self.clone();
        let (datafiles, write_status, write_button) = write_data_files(
            format!("{sink_id}-write-data-files"),
            minted_input,
            &batch_descriptions,
            connection_for_writer,
            storage_state.storage_configuration.clone(),
            Arc::new(arrow_schema_with_ids.clone()),
        );

        let connection_for_committer = self.clone();
        let (commit_status, commit_button) = commit_to_iceberg(
            format!("{sink_id}-commit-to-iceberg"),
            sink_id,
            sink.version,
            &datafiles,
            &batch_descriptions,
            Rc::clone(&write_frontier),
            connection_for_committer,
            storage_state.storage_configuration.clone(),
            write_handle,
        );

        let running_status = Some(HealthStatusMessage {
            id: None,
            update: HealthStatusUpdate::running(),
            namespace: StatusNamespace::Iceberg,
        })
        .to_stream(&mut scope);

        let statuses =
            scope.concatenate([running_status, mint_status, write_status, commit_status]);

        (statuses, vec![mint_button, write_button, commit_button])
    }
}
