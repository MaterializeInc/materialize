// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Built-in catalog items for the `mz_introspection` schema.

use std::collections::BTreeMap;
use std::sync::LazyLock;

use mz_compute_client::logging::{ComputeLog, DifferentialLog, LogVariant, TimelyLog};
use mz_pgrepr::oid;
use mz_repr::adt::numeric::NumericMaxScale;
use mz_repr::namespaces::MZ_INTROSPECTION_SCHEMA;
use mz_repr::{RelationDesc, SemanticType, SqlScalarType};

use super::{
    BuiltinLog, BuiltinView, Cardinality, LinkProperties, Ontology, OntologyLink, PUBLIC_SELECT,
};

pub static MZ_DATAFLOW_OPERATORS_PER_WORKER: LazyLock<BuiltinLog> = LazyLock::new(|| BuiltinLog {
    name: "mz_dataflow_operators_per_worker",
    schema: MZ_INTROSPECTION_SCHEMA,
    oid: oid::LOG_MZ_DATAFLOW_OPERATORS_PER_WORKER_OID,
    variant: LogVariant::Timely(TimelyLog::Operates),
    access: vec![PUBLIC_SELECT],
    ontology: Some(Ontology {
        entity_name: "dataflow_operator_per_worker",
        description: "Timely dataflow operators present on a specific worker.",
        links: &const { [] },
        column_semantic_types: &[],
    }),
});

pub static MZ_DATAFLOW_ADDRESSES_PER_WORKER: LazyLock<BuiltinLog> = LazyLock::new(|| BuiltinLog {
    name: "mz_dataflow_addresses_per_worker",
    schema: MZ_INTROSPECTION_SCHEMA,
    oid: oid::LOG_MZ_DATAFLOW_ADDRESSES_PER_WORKER_OID,
    variant: LogVariant::Timely(TimelyLog::Addresses),
    access: vec![PUBLIC_SELECT],
    ontology: Some(Ontology {
        entity_name: "dataflow_address_per_worker",
        description: "Scope address of each Timely operator per worker.",
        links: &const {
            [OntologyLink {
                name: "address_of",
                target: "dataflow_operator_per_worker",
                properties: LinkProperties::fk_composite(
                    "id",
                    "id",
                    Cardinality::ManyToOne,
                    &[("worker_id", "worker_id")],
                ),
            }]
        },
        column_semantic_types: &[],
    }),
});

pub static MZ_DATAFLOW_CHANNELS_PER_WORKER: LazyLock<BuiltinLog> = LazyLock::new(|| BuiltinLog {
    name: "mz_dataflow_channels_per_worker",
    schema: MZ_INTROSPECTION_SCHEMA,
    oid: oid::LOG_MZ_DATAFLOW_CHANNELS_PER_WORKER_OID,
    variant: LogVariant::Timely(TimelyLog::Channels),
    access: vec![PUBLIC_SELECT],
    ontology: Some(Ontology {
        entity_name: "dataflow_channel_per_worker",
        description: "Timely dataflow communication channels per worker.",
        links: &const {
            [
                OntologyLink {
                    name: "source_operator",
                    target: "dataflow_operator_per_worker",
                    properties: LinkProperties::fk_composite(
                        "from_index",
                        "id",
                        Cardinality::ManyToOne,
                        &[("worker_id", "worker_id")],
                    ),
                },
                OntologyLink {
                    name: "target_operator",
                    target: "dataflow_operator_per_worker",
                    properties: LinkProperties::fk_composite(
                        "to_index",
                        "id",
                        Cardinality::ManyToOne,
                        &[("worker_id", "worker_id")],
                    ),
                },
            ]
        },
        column_semantic_types: &[],
    }),
});

pub static MZ_SCHEDULING_ELAPSED_RAW: LazyLock<BuiltinLog> = LazyLock::new(|| BuiltinLog {
    name: "mz_scheduling_elapsed_raw",
    schema: MZ_INTROSPECTION_SCHEMA,
    oid: oid::LOG_MZ_SCHEDULING_ELAPSED_RAW_OID,
    variant: LogVariant::Timely(TimelyLog::Elapsed),
    access: vec![PUBLIC_SELECT],
    ontology: None,
});

pub static MZ_COMPUTE_OPERATOR_DURATIONS_HISTOGRAM_RAW: LazyLock<BuiltinLog> =
    LazyLock::new(|| BuiltinLog {
        name: "mz_compute_operator_durations_histogram_raw",
        schema: MZ_INTROSPECTION_SCHEMA,
        oid: oid::LOG_MZ_COMPUTE_OPERATOR_DURATIONS_HISTOGRAM_RAW_OID,
        variant: LogVariant::Timely(TimelyLog::Histogram),
        access: vec![PUBLIC_SELECT],
        ontology: None,
    });

pub static MZ_SCHEDULING_PARKS_HISTOGRAM_RAW: LazyLock<BuiltinLog> = LazyLock::new(|| BuiltinLog {
    name: "mz_scheduling_parks_histogram_raw",
    schema: MZ_INTROSPECTION_SCHEMA,
    oid: oid::LOG_MZ_SCHEDULING_PARKS_HISTOGRAM_RAW_OID,
    variant: LogVariant::Timely(TimelyLog::Parks),
    access: vec![PUBLIC_SELECT],
    ontology: None,
});

pub static MZ_ARRANGEMENT_RECORDS_RAW: LazyLock<BuiltinLog> = LazyLock::new(|| BuiltinLog {
    name: "mz_arrangement_records_raw",
    schema: MZ_INTROSPECTION_SCHEMA,
    oid: oid::LOG_MZ_ARRANGEMENT_RECORDS_RAW_OID,
    variant: LogVariant::Differential(DifferentialLog::ArrangementRecords),
    access: vec![PUBLIC_SELECT],
    ontology: None,
});

pub static MZ_ARRANGEMENT_BATCHES_RAW: LazyLock<BuiltinLog> = LazyLock::new(|| BuiltinLog {
    name: "mz_arrangement_batches_raw",
    schema: MZ_INTROSPECTION_SCHEMA,
    oid: oid::LOG_MZ_ARRANGEMENT_BATCHES_RAW_OID,
    variant: LogVariant::Differential(DifferentialLog::ArrangementBatches),
    access: vec![PUBLIC_SELECT],
    ontology: None,
});

pub static MZ_ARRANGEMENT_SHARING_RAW: LazyLock<BuiltinLog> = LazyLock::new(|| BuiltinLog {
    name: "mz_arrangement_sharing_raw",
    schema: MZ_INTROSPECTION_SCHEMA,
    oid: oid::LOG_MZ_ARRANGEMENT_SHARING_RAW_OID,
    variant: LogVariant::Differential(DifferentialLog::Sharing),
    access: vec![PUBLIC_SELECT],
    ontology: None,
});

pub static MZ_ARRANGEMENT_BATCHER_RECORDS_RAW: LazyLock<BuiltinLog> =
    LazyLock::new(|| BuiltinLog {
        name: "mz_arrangement_batcher_records_raw",
        schema: MZ_INTROSPECTION_SCHEMA,
        oid: oid::LOG_MZ_ARRANGEMENT_BATCHER_RECORDS_RAW_OID,
        variant: LogVariant::Differential(DifferentialLog::BatcherRecords),
        access: vec![PUBLIC_SELECT],
        ontology: None,
    });

pub static MZ_ARRANGEMENT_BATCHER_SIZE_RAW: LazyLock<BuiltinLog> = LazyLock::new(|| BuiltinLog {
    name: "mz_arrangement_batcher_size_raw",
    schema: MZ_INTROSPECTION_SCHEMA,
    oid: oid::LOG_MZ_ARRANGEMENT_BATCHER_SIZE_RAW_OID,
    variant: LogVariant::Differential(DifferentialLog::BatcherSize),
    access: vec![PUBLIC_SELECT],
    ontology: None,
});

pub static MZ_ARRANGEMENT_BATCHER_CAPACITY_RAW: LazyLock<BuiltinLog> =
    LazyLock::new(|| BuiltinLog {
        name: "mz_arrangement_batcher_capacity_raw",
        schema: MZ_INTROSPECTION_SCHEMA,
        oid: oid::LOG_MZ_ARRANGEMENT_BATCHER_CAPACITY_RAW_OID,
        variant: LogVariant::Differential(DifferentialLog::BatcherCapacity),
        access: vec![PUBLIC_SELECT],
        ontology: None,
    });

pub static MZ_ARRANGEMENT_BATCHER_ALLOCATIONS_RAW: LazyLock<BuiltinLog> =
    LazyLock::new(|| BuiltinLog {
        name: "mz_arrangement_batcher_allocations_raw",
        schema: MZ_INTROSPECTION_SCHEMA,
        oid: oid::LOG_MZ_ARRANGEMENT_BATCHER_ALLOCATIONS_RAW_OID,
        variant: LogVariant::Differential(DifferentialLog::BatcherAllocations),
        access: vec![PUBLIC_SELECT],
        ontology: None,
    });

pub static MZ_COMPUTE_EXPORTS_PER_WORKER: LazyLock<BuiltinLog> = LazyLock::new(|| BuiltinLog {
    name: "mz_compute_exports_per_worker",
    schema: MZ_INTROSPECTION_SCHEMA,
    oid: oid::LOG_MZ_COMPUTE_EXPORTS_PER_WORKER_OID,
    variant: LogVariant::Compute(ComputeLog::DataflowCurrent),
    access: vec![PUBLIC_SELECT],
    ontology: Some(Ontology {
        entity_name: "compute_export_per_worker",
        description: "Active compute exports (dataflows) present per worker.",
        links: &const { [] },
        column_semantic_types: &[("export_id", SemanticType::GlobalId)],
    }),
});

pub static MZ_COMPUTE_DATAFLOW_GLOBAL_IDS_PER_WORKER: LazyLock<BuiltinLog> =
    LazyLock::new(|| BuiltinLog {
        name: "mz_compute_dataflow_global_ids_per_worker",
        schema: MZ_INTROSPECTION_SCHEMA,
        oid: oid::LOG_MZ_COMPUTE_DATAFLOW_GLOBAL_IDS_PER_WORKER_OID,
        variant: LogVariant::Compute(ComputeLog::DataflowGlobal),
        access: vec![PUBLIC_SELECT],
        ontology: Some(Ontology {
            entity_name: "dataflow_global_id_per_worker",
            description: "Mapping from internal dataflow IDs to GlobalIds per worker.",
            links: &const {
                [OntologyLink {
                    name: "global_id_of",
                    target: "compute_export_per_worker",
                    properties: LinkProperties::MapsTo {
                        source_column: "global_id",
                        target_column: "export_id",
                        via: None,
                        from_type: Some(SemanticType::GlobalId),
                        to_type: Some(SemanticType::GlobalId),
                        note: None,
                    },
                }]
            },
            column_semantic_types: &[("global_id", SemanticType::GlobalId)],
        }),
    });

pub static MZ_COMPUTE_DATAFLOW_AS_OF_PER_WORKER: LazyLock<BuiltinLog> =
    LazyLock::new(|| BuiltinLog {
        name: "mz_compute_dataflow_as_of_per_worker",
        schema: MZ_INTROSPECTION_SCHEMA,
        oid: oid::LOG_MZ_COMPUTE_DATAFLOW_AS_OF_PER_WORKER_OID,
        variant: LogVariant::Compute(ComputeLog::DataflowAsOf),
        access: vec![PUBLIC_SELECT],
        ontology: Some(Ontology {
            entity_name: "compute_dataflow_as_of_per_worker",
            description: "Initial as-of frontier of each dataflow, per worker.",
            links: &const {
                [OntologyLink {
                    name: "as_of_of",
                    target: "compute_export_per_worker",
                    properties: LinkProperties::fk_composite(
                        "dataflow_id",
                        "dataflow_id",
                        Cardinality::ManyToOne,
                        &[("worker_id", "worker_id")],
                    ),
                }]
            },
            column_semantic_types: &[("time", SemanticType::MzTimestamp)],
        }),
    });

pub static MZ_CLUSTER_PROMETHEUS_METRICS: LazyLock<BuiltinLog> = LazyLock::new(|| BuiltinLog {
    name: "mz_cluster_prometheus_metrics",
    schema: MZ_INTROSPECTION_SCHEMA,
    oid: oid::LOG_MZ_CLUSTER_PROMETHEUS_METRICS_OID,
    variant: LogVariant::Compute(ComputeLog::PrometheusMetrics),
    access: vec![PUBLIC_SELECT],
    ontology: Some(Ontology {
        entity_name: "cluster_prometheus_metric",
        description: "Prometheus metrics gathered from the cluster's metrics registry.",
        links: &const { [] },
        column_semantic_types: &[],
    }),
});

pub static MZ_COMPUTE_FRONTIERS_PER_WORKER: LazyLock<BuiltinLog> = LazyLock::new(|| BuiltinLog {
    name: "mz_compute_frontiers_per_worker",
    schema: MZ_INTROSPECTION_SCHEMA,
    oid: oid::LOG_MZ_COMPUTE_FRONTIERS_PER_WORKER_OID,
    variant: LogVariant::Compute(ComputeLog::FrontierCurrent),
    access: vec![PUBLIC_SELECT],
    ontology: Some(Ontology {
        entity_name: "compute_frontier_per_worker",
        description: "Per-worker output frontier timestamps for each compute export.",
        links: &const {
            [OntologyLink {
                name: "frontier_of",
                target: "compute_export_per_worker",
                properties: LinkProperties::measures_composite(
                    "export_id",
                    "export_id",
                    "time",
                    &[("worker_id", "worker_id")],
                ),
            }]
        },
        column_semantic_types: &[
            ("export_id", SemanticType::GlobalId),
            ("time", SemanticType::MzTimestamp),
        ],
    }),
});

pub static MZ_COMPUTE_IMPORT_FRONTIERS_PER_WORKER: LazyLock<BuiltinLog> =
    LazyLock::new(|| BuiltinLog {
        name: "mz_compute_import_frontiers_per_worker",
        schema: MZ_INTROSPECTION_SCHEMA,
        oid: oid::LOG_MZ_COMPUTE_IMPORT_FRONTIERS_PER_WORKER_OID,
        variant: LogVariant::Compute(ComputeLog::ImportFrontierCurrent),
        access: vec![PUBLIC_SELECT],
        ontology: Some(Ontology {
            entity_name: "compute_import_frontier_per_worker",
            description: "Per-worker input frontier timestamps for each compute dataflow.",
            links: &const {
                [OntologyLink {
                    name: "import_frontier_of",
                    target: "compute_export_per_worker",
                    properties: LinkProperties::fk_composite(
                        "export_id",
                        "export_id",
                        Cardinality::ManyToOne,
                        &[("worker_id", "worker_id")],
                    ),
                }]
            },
            column_semantic_types: &[
                ("export_id", SemanticType::GlobalId),
                ("import_id", SemanticType::GlobalId),
                ("time", SemanticType::MzTimestamp),
            ],
        }),
    });

pub static MZ_COMPUTE_ERROR_COUNTS_RAW: LazyLock<BuiltinLog> = LazyLock::new(|| BuiltinLog {
    name: "mz_compute_error_counts_raw",
    schema: MZ_INTROSPECTION_SCHEMA,
    oid: oid::LOG_MZ_COMPUTE_ERROR_COUNTS_RAW_OID,
    variant: LogVariant::Compute(ComputeLog::ErrorCount),
    access: vec![PUBLIC_SELECT],
    ontology: None,
});

pub static MZ_COMPUTE_HYDRATION_TIMES_PER_WORKER: LazyLock<BuiltinLog> =
    LazyLock::new(|| BuiltinLog {
        name: "mz_compute_hydration_times_per_worker",
        schema: MZ_INTROSPECTION_SCHEMA,
        oid: oid::LOG_MZ_COMPUTE_HYDRATION_TIMES_PER_WORKER_OID,
        variant: LogVariant::Compute(ComputeLog::HydrationTime),
        access: vec![PUBLIC_SELECT],
        ontology: Some(Ontology {
            entity_name: "hydration_time_per_worker",
            description: "Time in nanoseconds for each compute export to hydrate per worker.",
            links: &const {
                [OntologyLink {
                    name: "hydration_time_of",
                    target: "compute_export_per_worker",
                    properties: LinkProperties::measures_composite(
                        "export_id",
                        "export_id",
                        "time_ns",
                        &[("worker_id", "worker_id")],
                    ),
                }]
            },
            column_semantic_types: &[("export_id", SemanticType::GlobalId)],
        }),
    });

pub static MZ_COMPUTE_OPERATOR_HYDRATION_STATUSES_PER_WORKER: LazyLock<BuiltinLog> =
    LazyLock::new(|| BuiltinLog {
        name: "mz_compute_operator_hydration_statuses_per_worker",
        schema: MZ_INTROSPECTION_SCHEMA,
        oid: oid::LOG_MZ_COMPUTE_OPERATOR_HYDRATION_STATUSES_PER_WORKER_OID,
        variant: LogVariant::Compute(ComputeLog::OperatorHydrationStatus),
        access: vec![PUBLIC_SELECT],
        ontology: Some(Ontology {
            entity_name: "hydration_status_per_worker",
            description: "Hydration state for each LIR node per worker.",
            links: &const {
                [OntologyLink {
                    name: "hydration_of",
                    target: "compute_export_per_worker",
                    properties: LinkProperties::fk_composite(
                        "export_id",
                        "export_id",
                        Cardinality::ManyToOne,
                        &[("worker_id", "worker_id")],
                    ),
                }]
            },
            column_semantic_types: &[("export_id", SemanticType::GlobalId)],
        }),
    });

pub static MZ_ACTIVE_PEEKS_PER_WORKER: LazyLock<BuiltinLog> = LazyLock::new(|| BuiltinLog {
    name: "mz_active_peeks_per_worker",
    schema: MZ_INTROSPECTION_SCHEMA,
    oid: oid::LOG_MZ_ACTIVE_PEEKS_PER_WORKER_OID,
    variant: LogVariant::Compute(ComputeLog::PeekCurrent),
    access: vec![PUBLIC_SELECT],
    ontology: Some(Ontology {
        entity_name: "active_peek_per_worker",
        description: "In-flight peek requests currently executing per worker.",
        links: &const { [] },
        column_semantic_types: &[
            ("object_id", SemanticType::GlobalId),
            ("time", SemanticType::MzTimestamp),
        ],
    }),
});

pub static MZ_COMPUTE_LIR_MAPPING_PER_WORKER: LazyLock<BuiltinLog> = LazyLock::new(|| BuiltinLog {
    name: "mz_compute_lir_mapping_per_worker",
    schema: MZ_INTROSPECTION_SCHEMA,
    oid: oid::LOG_MZ_COMPUTE_LIR_MAPPING_PER_WORKER_OID,
    variant: LogVariant::Compute(ComputeLog::LirMapping),
    access: vec![PUBLIC_SELECT],
    ontology: Some(Ontology {
        entity_name: "lir_mapping_per_worker",
        description: "Mapping from LIR node IDs to dataflow operator address ranges per worker.",
        links: &const {
            [OntologyLink {
                name: "export_of",
                target: "compute_export_per_worker",
                properties: LinkProperties::fk_composite(
                    "global_id",
                    "export_id",
                    Cardinality::ManyToOne,
                    &[("worker_id", "worker_id")],
                ),
            }]
        },
        column_semantic_types: &[("global_id", SemanticType::GlobalId)],
    }),
});

pub static MZ_PEEK_DURATIONS_HISTOGRAM_RAW: LazyLock<BuiltinLog> = LazyLock::new(|| BuiltinLog {
    name: "mz_peek_durations_histogram_raw",
    schema: MZ_INTROSPECTION_SCHEMA,
    oid: oid::LOG_MZ_PEEK_DURATIONS_HISTOGRAM_RAW_OID,
    variant: LogVariant::Compute(ComputeLog::PeekDuration),
    access: vec![PUBLIC_SELECT],
    ontology: None,
});

pub static MZ_ARRANGEMENT_HEAP_SIZE_RAW: LazyLock<BuiltinLog> = LazyLock::new(|| BuiltinLog {
    name: "mz_arrangement_heap_size_raw",
    schema: MZ_INTROSPECTION_SCHEMA,
    oid: oid::LOG_MZ_ARRANGEMENT_HEAP_SIZE_RAW_OID,
    variant: LogVariant::Compute(ComputeLog::ArrangementHeapSize),
    access: vec![PUBLIC_SELECT],
    ontology: None,
});

pub static MZ_ARRANGEMENT_HEAP_CAPACITY_RAW: LazyLock<BuiltinLog> = LazyLock::new(|| BuiltinLog {
    name: "mz_arrangement_heap_capacity_raw",
    schema: MZ_INTROSPECTION_SCHEMA,
    oid: oid::LOG_MZ_ARRANGEMENT_HEAP_CAPACITY_RAW_OID,
    variant: LogVariant::Compute(ComputeLog::ArrangementHeapCapacity),
    access: vec![PUBLIC_SELECT],
    ontology: None,
});

pub static MZ_ARRANGEMENT_HEAP_ALLOCATIONS_RAW: LazyLock<BuiltinLog> =
    LazyLock::new(|| BuiltinLog {
        name: "mz_arrangement_heap_allocations_raw",
        schema: MZ_INTROSPECTION_SCHEMA,
        oid: oid::LOG_MZ_ARRANGEMENT_HEAP_ALLOCATIONS_RAW_OID,
        variant: LogVariant::Compute(ComputeLog::ArrangementHeapAllocations),
        access: vec![PUBLIC_SELECT],
        ontology: None,
    });

pub static MZ_MESSAGE_BATCH_COUNTS_RECEIVED_RAW: LazyLock<BuiltinLog> =
    LazyLock::new(|| BuiltinLog {
        name: "mz_message_batch_counts_received_raw",
        schema: MZ_INTROSPECTION_SCHEMA,
        oid: oid::LOG_MZ_MESSAGE_BATCH_COUNTS_RECEIVED_RAW_OID,
        variant: LogVariant::Timely(TimelyLog::BatchesReceived),
        access: vec![PUBLIC_SELECT],
        ontology: None,
    });

pub static MZ_MESSAGE_BATCH_COUNTS_SENT_RAW: LazyLock<BuiltinLog> = LazyLock::new(|| BuiltinLog {
    name: "mz_message_batch_counts_sent_raw",
    schema: MZ_INTROSPECTION_SCHEMA,
    oid: oid::LOG_MZ_MESSAGE_BATCH_COUNTS_SENT_RAW_OID,
    variant: LogVariant::Timely(TimelyLog::BatchesSent),
    access: vec![PUBLIC_SELECT],
    ontology: None,
});

pub static MZ_MESSAGE_COUNTS_RECEIVED_RAW: LazyLock<BuiltinLog> = LazyLock::new(|| BuiltinLog {
    name: "mz_message_counts_received_raw",
    schema: MZ_INTROSPECTION_SCHEMA,
    oid: oid::LOG_MZ_MESSAGE_COUNTS_RECEIVED_RAW_OID,
    variant: LogVariant::Timely(TimelyLog::MessagesReceived),
    access: vec![PUBLIC_SELECT],
    ontology: None,
});

pub static MZ_MESSAGE_COUNTS_SENT_RAW: LazyLock<BuiltinLog> = LazyLock::new(|| BuiltinLog {
    name: "mz_message_counts_sent_raw",
    schema: MZ_INTROSPECTION_SCHEMA,
    oid: oid::LOG_MZ_MESSAGE_COUNTS_SENT_RAW_OID,
    variant: LogVariant::Timely(TimelyLog::MessagesSent),
    access: vec![PUBLIC_SELECT],
    ontology: None,
});

pub static MZ_DATAFLOW_OPERATOR_REACHABILITY_RAW: LazyLock<BuiltinLog> =
    LazyLock::new(|| BuiltinLog {
        name: "mz_dataflow_operator_reachability_raw",
        schema: MZ_INTROSPECTION_SCHEMA,
        oid: oid::LOG_MZ_DATAFLOW_OPERATOR_REACHABILITY_RAW_OID,
        variant: LogVariant::Timely(TimelyLog::Reachability),
        access: vec![PUBLIC_SELECT],
        ontology: None,
    });

pub static MZ_DATAFLOWS_PER_WORKER: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "mz_dataflows_per_worker",
    schema: MZ_INTROSPECTION_SCHEMA,
    oid: oid::VIEW_MZ_DATAFLOWS_PER_WORKER_OID,
    desc: RelationDesc::builder()
        .with_column("id", SqlScalarType::UInt64.nullable(true))
        .with_column("worker_id", SqlScalarType::UInt64.nullable(false))
        .with_column("name", SqlScalarType::String.nullable(false))
        .finish(),
    column_comments: BTreeMap::new(),
    sql: "SELECT
    addrs.address[1] AS id,
    ops.worker_id,
    ops.name
FROM
    mz_introspection.mz_dataflow_addresses_per_worker addrs,
    mz_introspection.mz_dataflow_operators_per_worker ops
WHERE
    addrs.id = ops.id AND
    addrs.worker_id = ops.worker_id AND
    mz_catalog.list_length(addrs.address) = 1",
    access: vec![PUBLIC_SELECT],
    ontology: None,
});

pub static MZ_DATAFLOWS: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "mz_dataflows",
    schema: MZ_INTROSPECTION_SCHEMA,
    oid: oid::VIEW_MZ_DATAFLOWS_OID,
    desc: RelationDesc::builder()
        .with_column("id", SqlScalarType::UInt64.nullable(true))
        .with_column("name", SqlScalarType::String.nullable(false))
        .finish(),
    column_comments: BTreeMap::from_iter([
        ("id", "The ID of the dataflow."),
        ("name", "The internal name of the dataflow."),
    ]),
    sql: "
SELECT id, name
FROM mz_introspection.mz_dataflows_per_worker
WHERE worker_id = 0::uint8",
    access: vec![PUBLIC_SELECT],
    ontology: Some(Ontology {
        entity_name: "dataflow",
        description: "Dataflow instances",
        links: &const { [] },
        column_semantic_types: &[],
    }),
});

pub static MZ_DATAFLOW_ADDRESSES: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "mz_dataflow_addresses",
    schema: MZ_INTROSPECTION_SCHEMA,
    oid: oid::VIEW_MZ_DATAFLOW_ADDRESSES_OID,
    desc: RelationDesc::builder()
        .with_column("id", SqlScalarType::UInt64.nullable(false))
        .with_column(
            "address",
            SqlScalarType::List {
                element_type: Box::new(SqlScalarType::UInt64),
                custom_id: None,
            }
            .nullable(false),
        )
        .with_key(vec![0])
        .finish(),
    column_comments: BTreeMap::from_iter([
        (
            "id",
            "The ID of the channel or operator. Corresponds to `mz_dataflow_channels.id` or `mz_dataflow_operators.id`.",
        ),
        (
            "address",
            "A list of scope-local indexes indicating the path from the root to this channel or operator.",
        ),
    ]),
    sql: "
SELECT id, address
FROM mz_introspection.mz_dataflow_addresses_per_worker
WHERE worker_id = 0::uint8",
    access: vec![PUBLIC_SELECT],
    ontology: Some(Ontology {
        entity_name: "dataflow_address",
        description: "Address (scope path) of dataflow operators",
        links: &const {
            [OntologyLink {
                name: "address_of_operator",
                target: "dataflow_operator",
                properties: LinkProperties::fk("id", "id", Cardinality::OneToOne),
            }]
        },
        column_semantic_types: &[],
    }),
});

pub static MZ_DATAFLOW_CHANNELS: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "mz_dataflow_channels",
    schema: MZ_INTROSPECTION_SCHEMA,
    oid: oid::VIEW_MZ_DATAFLOW_CHANNELS_OID,
    desc: RelationDesc::builder()
        .with_column("id", SqlScalarType::UInt64.nullable(false))
        .with_column("from_index", SqlScalarType::UInt64.nullable(false))
        .with_column("from_port", SqlScalarType::UInt64.nullable(false))
        .with_column("to_index", SqlScalarType::UInt64.nullable(false))
        .with_column("to_port", SqlScalarType::UInt64.nullable(false))
        .with_column("type", SqlScalarType::String.nullable(false))
        .with_key(vec![0])
        .finish(),
    column_comments: BTreeMap::from_iter([
        ("id", "The ID of the channel."),
        (
            "from_index",
            "The scope-local index of the source operator. Corresponds to `mz_dataflow_addresses.address`.",
        ),
        ("from_port", "The source operator's output port."),
        (
            "to_index",
            "The scope-local index of the target operator. Corresponds to `mz_dataflow_addresses.address`.",
        ),
        ("to_port", "The target operator's input port."),
        ("type", "The container type of the channel."),
    ]),
    sql: "
SELECT id, from_index, from_port, to_index, to_port, type
FROM mz_introspection.mz_dataflow_channels_per_worker
WHERE worker_id = 0::uint8",
    access: vec![PUBLIC_SELECT],
    ontology: Some(Ontology {
        entity_name: "dataflow_channel",
        description: "Communication channels between operators",
        links: &const {
            [OntologyLink {
                name: "channel_in_dataflow",
                target: "dataflow",
                properties: LinkProperties::MapsTo {
                    source_column: "from_index",
                    target_column: "id",
                    via: Some("mz_introspection.mz_dataflow_operator_dataflows"),
                    from_type: None,
                    to_type: None,
                    note: Some(
                        "Channels do not have a direct dataflow_id. Use mz_dataflow_addresses to find the parent scope, then correlate with mz_dataflow_operator_dataflows.",
                    ),
                },
            }]
        },
        column_semantic_types: &[],
    }),
});

pub static MZ_DATAFLOW_OPERATORS: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "mz_dataflow_operators",
    schema: MZ_INTROSPECTION_SCHEMA,
    oid: oid::VIEW_MZ_DATAFLOW_OPERATORS_OID,
    desc: RelationDesc::builder()
        .with_column("id", SqlScalarType::UInt64.nullable(false))
        .with_column("name", SqlScalarType::String.nullable(false))
        .with_key(vec![0])
        .finish(),
    column_comments: BTreeMap::from_iter([
        ("id", "The ID of the operator."),
        ("name", "The internal name of the operator."),
    ]),
    sql: "
SELECT id, name
FROM mz_introspection.mz_dataflow_operators_per_worker
WHERE worker_id = 0::uint8",
    access: vec![PUBLIC_SELECT],
    ontology: Some(Ontology {
        entity_name: "dataflow_operator",
        description: "Operators within dataflows",
        links: &const { [] },
        column_semantic_types: &[],
    }),
});

pub static MZ_DATAFLOW_GLOBAL_IDS: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "mz_dataflow_global_ids",
    schema: MZ_INTROSPECTION_SCHEMA,
    oid: oid::VIEW_MZ_DATAFLOW_GLOBAL_IDS_OID,
    desc: RelationDesc::builder()
        .with_column("id", SqlScalarType::UInt64.nullable(false))
        .with_column("global_id", SqlScalarType::String.nullable(false))
        .with_key(vec![0, 1])
        .finish(),
    column_comments: BTreeMap::from_iter([
        ("id", "The dataflow ID."),
        ("global_id", "A global ID associated with that dataflow."),
    ]),
    sql: "
SELECT id, global_id
FROM mz_introspection.mz_compute_dataflow_global_ids_per_worker
WHERE worker_id = 0::uint8",
    access: vec![PUBLIC_SELECT],
    ontology: None,
});

pub static MZ_COMPUTE_DATAFLOW_AS_OF: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "mz_compute_dataflow_as_of",
    schema: MZ_INTROSPECTION_SCHEMA,
    oid: oid::VIEW_MZ_COMPUTE_DATAFLOW_AS_OF_OID,
    desc: RelationDesc::builder()
        .with_column("dataflow_id", SqlScalarType::UInt64.nullable(false))
        .with_column("time", SqlScalarType::MzTimestamp.nullable(true))
        .with_key(vec![0])
        .finish(),
    column_comments: BTreeMap::from_iter([
        (
            "dataflow_id",
            "The ID of the dataflow. Corresponds to `mz_dataflows.id`.",
        ),
        (
            "time",
            "The initial as-of frontier of the dataflow, or `NULL` if its as-of frontier is empty.",
        ),
    ]),
    // The as-of is identical across workers; take the minimum to be robust to a
    // worker that has not reported yet.
    sql: "
SELECT dataflow_id, pg_catalog.min(time) AS time
FROM mz_introspection.mz_compute_dataflow_as_of_per_worker
GROUP BY dataflow_id",
    access: vec![PUBLIC_SELECT],
    ontology: Some(Ontology {
        entity_name: "compute_dataflow_as_of",
        description: "Initial as-of frontier of each dataflow.",
        links: &const {
            [OntologyLink {
                name: "as_of_of",
                target: "dataflow",
                properties: LinkProperties::fk("dataflow_id", "id", Cardinality::ManyToOne),
            }]
        },
        column_semantic_types: &[("time", SemanticType::MzTimestamp)],
    }),
});

pub static MZ_MAPPABLE_OBJECTS: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "mz_mappable_objects",
    schema: MZ_INTROSPECTION_SCHEMA,
    oid: oid::VIEW_MZ_MAPPABLE_OBJECTS_OID,
    desc: RelationDesc::builder()
        .with_column("name", SqlScalarType::String.nullable(false))
        .with_column("global_id", SqlScalarType::String.nullable(false))
        .finish(),
    column_comments: BTreeMap::from_iter([
        (
            "name",
            "The name of the object. This name is unquoted, and you might need to call `quote_ident` if you want to reference the name shown here.",
        ),
        ("global_id", "The global ID of the object."),
    ]),
    sql: "
SELECT COALESCE(md.name || '.', '') || ms.name || '.' || mo.name AS name, mgi.global_id AS global_id
FROM      mz_catalog.mz_objects mo
          JOIN mz_introspection.mz_compute_exports mce ON (mo.id = mce.export_id)
          JOIN mz_catalog.mz_schemas ms ON (mo.schema_id = ms.id)
          JOIN mz_introspection.mz_dataflow_global_ids mgi ON (mce.dataflow_id = mgi.id)
     LEFT JOIN mz_catalog.mz_databases md ON (ms.database_id = md.id);",
    access: vec![PUBLIC_SELECT],
    ontology: Some(Ontology {
        entity_name: "mappable_object",
        description: "Objects that can be mapped to dataflow operators",
        links: &const { [] },
        column_semantic_types: &[("global_id", SemanticType::GlobalId)],
    }),
});

pub static MZ_LIR_MAPPING: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "mz_lir_mapping",
    schema: MZ_INTROSPECTION_SCHEMA,
    oid: oid::VIEW_MZ_LIR_MAPPING_OID,
    desc: RelationDesc::builder()
        .with_column("global_id", SqlScalarType::String.nullable(false))
        .with_column("lir_id", SqlScalarType::UInt64.nullable(false))
        .with_column("operator", SqlScalarType::String.nullable(false))
        .with_column("parent_lir_id", SqlScalarType::UInt64.nullable(true))
        .with_column("nesting", SqlScalarType::UInt16.nullable(false))
        .with_column("operator_id_start", SqlScalarType::UInt64.nullable(false))
        .with_column("operator_id_end", SqlScalarType::UInt64.nullable(false))
        .with_key(vec![0, 1])
        .finish(),
    column_comments: BTreeMap::from_iter([
        ("global_id", "The global ID."),
        ("lir_id", "The LIR node ID."),
        (
            "operator",
            "The LIR operator, in the format `OperatorName INPUTS [OPTIONS]`.",
        ),
        (
            "parent_lir_id",
            "The parent of this LIR node. May be `NULL`.",
        ),
        ("nesting", "The nesting level of this LIR node."),
        (
            "operator_id_start",
            "The first dataflow operator ID implementing this LIR operator (inclusive).",
        ),
        (
            "operator_id_end",
            "The first dataflow operator ID _after_ this LIR operator (exclusive).",
        ),
    ]),
    sql: "
SELECT global_id, lir_id, operator, parent_lir_id, nesting, operator_id_start, operator_id_end
FROM mz_introspection.mz_compute_lir_mapping_per_worker
WHERE worker_id = 0::uint8",
    access: vec![PUBLIC_SELECT],
    ontology: Some(Ontology {
        entity_name: "lir_mapping",
        description: "LIR (low-level IR) to dataflow operator mapping",
        links: &const { [] },
        column_semantic_types: &[("global_id", SemanticType::GlobalId)],
    }),
});

pub static MZ_DATAFLOW_OPERATOR_DATAFLOWS_PER_WORKER: LazyLock<BuiltinView> =
    LazyLock::new(|| BuiltinView {
        name: "mz_dataflow_operator_dataflows_per_worker",
        schema: MZ_INTROSPECTION_SCHEMA,
        oid: oid::VIEW_MZ_DATAFLOW_OPERATOR_DATAFLOWS_PER_WORKER_OID,
        desc: RelationDesc::builder()
            .with_column("id", SqlScalarType::UInt64.nullable(false))
            .with_column("name", SqlScalarType::String.nullable(false))
            .with_column("worker_id", SqlScalarType::UInt64.nullable(false))
            .with_column("dataflow_id", SqlScalarType::UInt64.nullable(false))
            .with_column("dataflow_name", SqlScalarType::String.nullable(false))
            .finish(),
        column_comments: BTreeMap::new(),
        sql: "SELECT
    ops.id,
    ops.name,
    ops.worker_id,
    dfs.id as dataflow_id,
    dfs.name as dataflow_name
FROM
    mz_introspection.mz_dataflow_operators_per_worker ops,
    mz_introspection.mz_dataflow_addresses_per_worker addrs,
    mz_introspection.mz_dataflows_per_worker dfs
WHERE
    ops.id = addrs.id AND
    ops.worker_id = addrs.worker_id AND
    dfs.id = addrs.address[1] AND
    dfs.worker_id = addrs.worker_id",
        access: vec![PUBLIC_SELECT],
        ontology: None,
    });

pub static MZ_DATAFLOW_OPERATOR_DATAFLOWS: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "mz_dataflow_operator_dataflows",
    schema: MZ_INTROSPECTION_SCHEMA,
    oid: oid::VIEW_MZ_DATAFLOW_OPERATOR_DATAFLOWS_OID,
    desc: RelationDesc::builder()
        .with_column("id", SqlScalarType::UInt64.nullable(false))
        .with_column("name", SqlScalarType::String.nullable(false))
        .with_column("dataflow_id", SqlScalarType::UInt64.nullable(false))
        .with_column("dataflow_name", SqlScalarType::String.nullable(false))
        .finish(),
    column_comments: BTreeMap::from_iter([
        (
            "id",
            "The ID of the operator. Corresponds to `mz_dataflow_operators.id`.",
        ),
        ("name", "The internal name of the operator."),
        (
            "dataflow_id",
            "The ID of the dataflow hosting the operator. Corresponds to `mz_dataflows.id`.",
        ),
        (
            "dataflow_name",
            "The internal name of the dataflow hosting the operator.",
        ),
    ]),
    sql: "
SELECT id, name, dataflow_id, dataflow_name
FROM mz_introspection.mz_dataflow_operator_dataflows_per_worker
WHERE worker_id = 0::uint8",
    access: vec![PUBLIC_SELECT],
    ontology: Some(Ontology {
        entity_name: "dataflow_operator_dataflow",
        description: "Mapping of operators to their parent dataflow",
        links: &const {
            [OntologyLink {
                name: "operator_in_dataflow",
                target: "dataflow",
                properties: LinkProperties::fk("dataflow_id", "id", Cardinality::ManyToOne),
            }]
        },
        column_semantic_types: &[],
    }),
});

pub static MZ_COMPUTE_EXPORTS: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "mz_compute_exports",
    schema: MZ_INTROSPECTION_SCHEMA,
    oid: oid::VIEW_MZ_COMPUTE_EXPORTS_OID,
    desc: RelationDesc::builder()
        .with_column("export_id", SqlScalarType::String.nullable(false))
        .with_column("dataflow_id", SqlScalarType::UInt64.nullable(false))
        .with_key(vec![0])
        .finish(),
    column_comments: BTreeMap::from_iter([
        (
            "export_id",
            "The ID of the index, materialized view, or subscription exported by the dataflow. Corresponds to `mz_catalog.mz_indexes.id`, `mz_catalog.mz_materialized_views.id`, or `mz_internal.mz_subscriptions.id`.",
        ),
        (
            "dataflow_id",
            "The ID of the dataflow. Corresponds to `mz_dataflows.id`.",
        ),
    ]),
    sql: "
SELECT export_id, dataflow_id
FROM mz_introspection.mz_compute_exports_per_worker
WHERE worker_id = 0::uint8",
    access: vec![PUBLIC_SELECT],
    ontology: Some(Ontology {
        entity_name: "compute_export",
        description: "Compute exports (maintained collections)",
        links: &const {
            [
                OntologyLink {
                    name: "export_of",
                    target: "object",
                    properties: LinkProperties::fk_mapped(
                        "export_id",
                        "id",
                        Cardinality::ManyToOne,
                        mz_repr::SemanticType::GlobalId,
                        "mz_internal.mz_object_global_ids",
                    ),
                },
                OntologyLink {
                    name: "introspection_uses_global_id",
                    target: "object_global_id",
                    properties: LinkProperties::MapsTo {
                        source_column: "export_id",
                        target_column: "global_id",
                        via: None,
                        from_type: None,
                        to_type: None,
                        note: Some(
                            "mz_introspection tables use GlobalId. To join with mz_catalog tables (which use CatalogItemId), go through mz_internal.mz_object_global_ids.",
                        ),
                    },
                },
            ]
        },
        column_semantic_types: &[("export_id", SemanticType::GlobalId)],
    }),
});

pub static MZ_COMPUTE_FRONTIERS: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "mz_compute_frontiers",
    schema: MZ_INTROSPECTION_SCHEMA,
    oid: oid::VIEW_MZ_COMPUTE_FRONTIERS_OID,
    desc: RelationDesc::builder()
        .with_column("export_id", SqlScalarType::String.nullable(false))
        .with_column("time", SqlScalarType::MzTimestamp.nullable(false))
        .with_key(vec![0])
        .finish(),
    column_comments: BTreeMap::from_iter([
        (
            "export_id",
            "The ID of the dataflow export. Corresponds to `mz_compute_exports.export_id`.",
        ),
        (
            "time",
            "The next timestamp at which the dataflow output may change.",
        ),
    ]),
    sql: "SELECT
    export_id, pg_catalog.min(time) AS time
FROM mz_introspection.mz_compute_frontiers_per_worker
GROUP BY export_id",
    access: vec![PUBLIC_SELECT],
    ontology: Some(Ontology {
        entity_name: "compute_frontier",
        description: "Per-replica compute frontiers",
        links: &const {
            [OntologyLink {
                name: "compute_frontier_of",
                target: "object",
                properties: LinkProperties::fk_mapped(
                    "export_id",
                    "id",
                    Cardinality::ManyToOne,
                    mz_repr::SemanticType::GlobalId,
                    "mz_internal.mz_object_global_ids",
                ),
            }]
        },
        column_semantic_types: &const {
            [
                ("export_id", SemanticType::GlobalId),
                ("time", SemanticType::MzTimestamp),
            ]
        },
    }),
});

pub static MZ_DATAFLOW_CHANNEL_OPERATORS_PER_WORKER: LazyLock<BuiltinView> =
    LazyLock::new(|| BuiltinView {
        name: "mz_dataflow_channel_operators_per_worker",
        schema: MZ_INTROSPECTION_SCHEMA,
        oid: oid::VIEW_MZ_DATAFLOW_CHANNEL_OPERATORS_PER_WORKER_OID,
        desc: RelationDesc::builder()
            .with_column("id", SqlScalarType::UInt64.nullable(false))
            .with_column("worker_id", SqlScalarType::UInt64.nullable(false))
            .with_column("from_operator_id", SqlScalarType::UInt64.nullable(true))
            .with_column(
                "from_operator_address",
                SqlScalarType::List {
                    element_type: Box::new(SqlScalarType::UInt64),
                    custom_id: None,
                }
                .nullable(false),
            )
            .with_column("to_operator_id", SqlScalarType::UInt64.nullable(true))
            .with_column(
                "to_operator_address",
                SqlScalarType::List {
                    element_type: Box::new(SqlScalarType::UInt64),
                    custom_id: None,
                }
                .nullable(false),
            )
            .with_column("type", SqlScalarType::String.nullable(false))
            .finish(),
        column_comments: BTreeMap::new(),
        sql: "
WITH
channel_addresses(id, worker_id, address, from_index, to_index, type) AS (
     SELECT id, worker_id, address, from_index, to_index, type
     FROM mz_introspection.mz_dataflow_channels_per_worker mdc
     INNER JOIN mz_introspection.mz_dataflow_addresses_per_worker mda
     USING (id, worker_id)
),
channel_operator_addresses(id, worker_id, from_address, to_address, type) AS (
     SELECT id, worker_id,
            address || from_index AS from_address,
            address || to_index AS to_address,
            type
     FROM channel_addresses
),
operator_addresses(id, worker_id, address) AS (
     SELECT id, worker_id, address
     FROM mz_introspection.mz_dataflow_addresses_per_worker mda
     INNER JOIN mz_introspection.mz_dataflow_operators_per_worker mdo
     USING (id, worker_id)
)
SELECT coa.id,
       coa.worker_id,
       from_ops.id AS from_operator_id,
       coa.from_address AS from_operator_address,
       to_ops.id AS to_operator_id,
       coa.to_address AS to_operator_address,
       coa.type
FROM channel_operator_addresses coa
     LEFT OUTER JOIN operator_addresses from_ops
          ON coa.from_address = from_ops.address AND
             coa.worker_id = from_ops.worker_id
     LEFT OUTER JOIN operator_addresses to_ops
          ON coa.to_address = to_ops.address AND
             coa.worker_id = to_ops.worker_id
",
        access: vec![PUBLIC_SELECT],
        ontology: None,
    });

pub static MZ_DATAFLOW_CHANNEL_OPERATORS: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "mz_dataflow_channel_operators",
    schema: MZ_INTROSPECTION_SCHEMA,
    oid: oid::VIEW_MZ_DATAFLOW_CHANNEL_OPERATORS_OID,
    desc: RelationDesc::builder()
        .with_column("id", SqlScalarType::UInt64.nullable(false))
        .with_column("from_operator_id", SqlScalarType::UInt64.nullable(true))
        .with_column(
            "from_operator_address",
            SqlScalarType::List {
                element_type: Box::new(SqlScalarType::UInt64),
                custom_id: None,
            }
            .nullable(false),
        )
        .with_column("to_operator_id", SqlScalarType::UInt64.nullable(true))
        .with_column(
            "to_operator_address",
            SqlScalarType::List {
                element_type: Box::new(SqlScalarType::UInt64),
                custom_id: None,
            }
            .nullable(false),
        )
        .with_column("type", SqlScalarType::String.nullable(false))
        .finish(),
    column_comments: BTreeMap::from_iter([
        (
            "id",
            "The ID of the channel. Corresponds to `mz_dataflow_channels.id`.",
        ),
        (
            "from_operator_id",
            "The ID of the source of the channel. Corresponds to `mz_dataflow_operators.id`.",
        ),
        (
            "from_operator_address",
            "The address of the source of the channel. Corresponds to `mz_dataflow_addresses.address`.",
        ),
        (
            "to_operator_id",
            "The ID of the target of the channel. Corresponds to `mz_dataflow_operators.id`.",
        ),
        (
            "to_operator_address",
            "The address of the target of the channel. Corresponds to `mz_dataflow_addresses.address`.",
        ),
        ("type", "The container type of the channel."),
    ]),
    sql: "
SELECT id, from_operator_id, from_operator_address, to_operator_id, to_operator_address, type
FROM mz_introspection.mz_dataflow_channel_operators_per_worker
WHERE worker_id = 0::uint8",
    access: vec![PUBLIC_SELECT],
    ontology: None,
});

pub static MZ_COMPUTE_IMPORT_FRONTIERS: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "mz_compute_import_frontiers",
    schema: MZ_INTROSPECTION_SCHEMA,
    oid: oid::VIEW_MZ_COMPUTE_IMPORT_FRONTIERS_OID,
    desc: RelationDesc::builder()
        .with_column("export_id", SqlScalarType::String.nullable(false))
        .with_column("import_id", SqlScalarType::String.nullable(false))
        .with_column("time", SqlScalarType::MzTimestamp.nullable(false))
        .with_key(vec![0, 1])
        .finish(),
    column_comments: BTreeMap::from_iter([
        (
            "export_id",
            "The ID of the dataflow export. Corresponds to `mz_compute_exports.export_id`.",
        ),
        (
            "import_id",
            "The ID of the dataflow import. Corresponds to `mz_catalog.mz_sources.id` or `mz_catalog.mz_tables.id` or `mz_compute_exports.export_id`.",
        ),
        (
            "time",
            "The next timestamp at which the dataflow input may change.",
        ),
    ]),
    sql: "SELECT
    export_id, import_id, pg_catalog.min(time) AS time
FROM mz_introspection.mz_compute_import_frontiers_per_worker
GROUP BY export_id, import_id",
    access: vec![PUBLIC_SELECT],
    ontology: Some(Ontology {
        entity_name: "compute_import_frontier",
        description: "Import frontiers for compute dependencies",
        links: &const {
            [OntologyLink {
                name: "compute_import_frontier_of",
                target: "object",
                properties: LinkProperties::fk_mapped(
                    "export_id",
                    "id",
                    Cardinality::ManyToOne,
                    mz_repr::SemanticType::GlobalId,
                    "mz_internal.mz_object_global_ids",
                ),
            }]
        },
        column_semantic_types: &const {
            [
                ("export_id", SemanticType::GlobalId),
                ("import_id", SemanticType::GlobalId),
                ("time", SemanticType::MzTimestamp),
            ]
        },
    }),
});

pub static MZ_RECORDS_PER_DATAFLOW_OPERATOR_PER_WORKER: LazyLock<BuiltinView> =
    LazyLock::new(|| BuiltinView {
        name: "mz_records_per_dataflow_operator_per_worker",
        schema: MZ_INTROSPECTION_SCHEMA,
        oid: oid::VIEW_MZ_RECORDS_PER_DATAFLOW_OPERATOR_PER_WORKER_OID,
        desc: RelationDesc::builder()
            .with_column("id", SqlScalarType::UInt64.nullable(false))
            .with_column("name", SqlScalarType::String.nullable(false))
            .with_column("worker_id", SqlScalarType::UInt64.nullable(false))
            .with_column("dataflow_id", SqlScalarType::UInt64.nullable(false))
            .with_column("records", SqlScalarType::Int64.nullable(true))
            .with_column("batches", SqlScalarType::Int64.nullable(true))
            .with_column("size", SqlScalarType::Int64.nullable(true))
            .with_column("capacity", SqlScalarType::Int64.nullable(true))
            .with_column("allocations", SqlScalarType::Int64.nullable(true))
            .finish(),
        column_comments: BTreeMap::new(),
        sql: "
SELECT
    dod.id,
    dod.name,
    dod.worker_id,
    dod.dataflow_id,
    ar_size.records AS records,
    ar_size.batches AS batches,
    ar_size.size AS size,
    ar_size.capacity AS capacity,
    ar_size.allocations AS allocations
FROM
    mz_introspection.mz_dataflow_operator_dataflows_per_worker dod
    LEFT OUTER JOIN mz_introspection.mz_arrangement_sizes_per_worker ar_size ON
        dod.id = ar_size.operator_id AND
        dod.worker_id = ar_size.worker_id",
        access: vec![PUBLIC_SELECT],
        ontology: None,
    });

pub static MZ_RECORDS_PER_DATAFLOW_OPERATOR: LazyLock<BuiltinView> =
    LazyLock::new(|| BuiltinView {
        name: "mz_records_per_dataflow_operator",
        schema: MZ_INTROSPECTION_SCHEMA,
        oid: oid::VIEW_MZ_RECORDS_PER_DATAFLOW_OPERATOR_OID,
        desc: RelationDesc::builder()
            .with_column("id", SqlScalarType::UInt64.nullable(false))
            .with_column("name", SqlScalarType::String.nullable(false))
            .with_column("dataflow_id", SqlScalarType::UInt64.nullable(false))
            .with_column("records", SqlScalarType::Int64.nullable(true))
            .with_column("batches", SqlScalarType::Int64.nullable(true))
            .with_column("size", SqlScalarType::Int64.nullable(true))
            .with_column("capacity", SqlScalarType::Int64.nullable(true))
            .with_column("allocations", SqlScalarType::Int64.nullable(true))
            .with_key(vec![0, 1, 2])
            .finish(),
        column_comments: BTreeMap::from_iter([
            (
                "id",
                "The ID of the operator. Corresponds to `mz_dataflow_operators.id`.",
            ),
            ("name", "The internal name of the operator."),
            (
                "dataflow_id",
                "The ID of the dataflow. Corresponds to `mz_dataflows.id`.",
            ),
            ("records", "The number of records in the operator."),
            ("batches", "The number of batches in the dataflow."),
            ("size", "The utilized size in bytes of the arrangement."),
            (
                "capacity",
                "The capacity in bytes of the arrangement. Can be larger than the size.",
            ),
            (
                "allocations",
                "The number of separate memory allocations backing the arrangement.",
            ),
        ]),
        sql: "
SELECT
    id,
    name,
    dataflow_id,
    SUM(records)::int8 AS records,
    SUM(batches)::int8 AS batches,
    SUM(size)::int8 AS size,
    SUM(capacity)::int8 AS capacity,
    SUM(allocations)::int8 AS allocations
FROM mz_introspection.mz_records_per_dataflow_operator_per_worker
GROUP BY id, name, dataflow_id",
        access: vec![PUBLIC_SELECT],
        ontology: None,
    });

pub static MZ_RECORDS_PER_DATAFLOW_PER_WORKER: LazyLock<BuiltinView> =
    LazyLock::new(|| BuiltinView {
        name: "mz_records_per_dataflow_per_worker",
        schema: MZ_INTROSPECTION_SCHEMA,
        oid: oid::VIEW_MZ_RECORDS_PER_DATAFLOW_PER_WORKER_OID,
        desc: RelationDesc::builder()
            .with_column("id", SqlScalarType::UInt64.nullable(false))
            .with_column("name", SqlScalarType::String.nullable(false))
            .with_column("worker_id", SqlScalarType::UInt64.nullable(false))
            .with_column("records", SqlScalarType::Int64.nullable(true))
            .with_column("batches", SqlScalarType::Int64.nullable(true))
            .with_column("size", SqlScalarType::Int64.nullable(true))
            .with_column("capacity", SqlScalarType::Int64.nullable(true))
            .with_column("allocations", SqlScalarType::Int64.nullable(true))
            .with_key(vec![0, 1, 2])
            .finish(),
        column_comments: BTreeMap::new(),
        sql: "
SELECT
    rdo.dataflow_id as id,
    dfs.name,
    rdo.worker_id,
    SUM(rdo.records)::int8 as records,
    SUM(rdo.batches)::int8 as batches,
    SUM(rdo.size)::int8 as size,
    SUM(rdo.capacity)::int8 as capacity,
    SUM(rdo.allocations)::int8 as allocations
FROM
    mz_introspection.mz_records_per_dataflow_operator_per_worker rdo,
    mz_introspection.mz_dataflows_per_worker dfs
WHERE
    rdo.dataflow_id = dfs.id AND
    rdo.worker_id = dfs.worker_id
GROUP BY
    rdo.dataflow_id,
    dfs.name,
    rdo.worker_id",
        access: vec![PUBLIC_SELECT],
        ontology: None,
    });

pub static MZ_RECORDS_PER_DATAFLOW: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "mz_records_per_dataflow",
    schema: MZ_INTROSPECTION_SCHEMA,
    oid: oid::VIEW_MZ_RECORDS_PER_DATAFLOW_OID,
    desc: RelationDesc::builder()
        .with_column("id", SqlScalarType::UInt64.nullable(false))
        .with_column("name", SqlScalarType::String.nullable(false))
        .with_column("records", SqlScalarType::Int64.nullable(true))
        .with_column("batches", SqlScalarType::Int64.nullable(true))
        .with_column("size", SqlScalarType::Int64.nullable(true))
        .with_column("capacity", SqlScalarType::Int64.nullable(true))
        .with_column("allocations", SqlScalarType::Int64.nullable(true))
        .with_key(vec![0, 1])
        .finish(),
    column_comments: BTreeMap::from_iter([
        (
            "id",
            "The ID of the dataflow. Corresponds to `mz_dataflows.id`.",
        ),
        ("name", "The internal name of the dataflow."),
        ("records", "The number of records in the dataflow."),
        ("batches", "The number of batches in the dataflow."),
        ("size", "The utilized size in bytes of the arrangements."),
        (
            "capacity",
            "The capacity in bytes of the arrangements. Can be larger than the size.",
        ),
        (
            "allocations",
            "The number of separate memory allocations backing the arrangements.",
        ),
    ]),
    sql: "
SELECT
    id,
    name,
    SUM(records)::int8 as records,
    SUM(batches)::int8 as batches,
    SUM(size)::int8 as size,
    SUM(capacity)::int8 as capacity,
    SUM(allocations)::int8 as allocations
FROM
    mz_introspection.mz_records_per_dataflow_per_worker
GROUP BY
    id,
    name",
    access: vec![PUBLIC_SELECT],
    ontology: Some(Ontology {
        entity_name: "records_per_dataflow",
        description: "Record counts aggregated per dataflow",
        links: &const {
            [OntologyLink {
                name: "details_of",
                target: "dataflow",
                properties: LinkProperties::fk("id", "id", Cardinality::OneToOne),
            }]
        },
        column_semantic_types: &[],
    }),
});

pub static MZ_PEEK_DURATIONS_HISTOGRAM_PER_WORKER: LazyLock<BuiltinView> =
    LazyLock::new(|| BuiltinView {
        name: "mz_peek_durations_histogram_per_worker",
        schema: MZ_INTROSPECTION_SCHEMA,
        oid: oid::VIEW_MZ_PEEK_DURATIONS_HISTOGRAM_PER_WORKER_OID,
        desc: RelationDesc::builder()
            .with_column("worker_id", SqlScalarType::UInt64.nullable(false))
            .with_column("type", SqlScalarType::String.nullable(false))
            .with_column("duration_ns", SqlScalarType::UInt64.nullable(false))
            .with_column("count", SqlScalarType::Int64.nullable(false))
            .with_key(vec![0, 1, 2])
            .finish(),
        column_comments: BTreeMap::new(),
        sql: "SELECT
    worker_id, type, duration_ns, pg_catalog.count(*) AS count
FROM
    mz_introspection.mz_peek_durations_histogram_raw
GROUP BY
    worker_id, type, duration_ns",
        access: vec![PUBLIC_SELECT],
        ontology: None,
    });

pub static MZ_PEEK_DURATIONS_HISTOGRAM: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "mz_peek_durations_histogram",
    schema: MZ_INTROSPECTION_SCHEMA,
    oid: oid::VIEW_MZ_PEEK_DURATIONS_HISTOGRAM_OID,
    desc: RelationDesc::builder()
        .with_column("type", SqlScalarType::String.nullable(false))
        .with_column("duration_ns", SqlScalarType::UInt64.nullable(false))
        .with_column(
            "count",
            SqlScalarType::Numeric {
                max_scale: Some(NumericMaxScale::ZERO),
            }
            .nullable(false),
        )
        .with_key(vec![0, 1])
        .finish(),
    column_comments: BTreeMap::from_iter([
        ("type", "The peek variant: `index` or `persist`."),
        (
            "duration_ns",
            "The upper bound of the bucket in nanoseconds.",
        ),
        (
            "count",
            "The (noncumulative) count of peeks in this bucket.",
        ),
    ]),
    sql: "
SELECT
    type, duration_ns,
    pg_catalog.sum(count) AS count
FROM mz_introspection.mz_peek_durations_histogram_per_worker
GROUP BY type, duration_ns",
    access: vec![PUBLIC_SELECT],
    ontology: Some(Ontology {
        entity_name: "peek_duration",
        description: "Histogram of SELECT query durations",
        links: &const { [] },
        column_semantic_types: &[],
    }),
});

pub static MZ_SCHEDULING_ELAPSED_PER_WORKER: LazyLock<BuiltinView> =
    LazyLock::new(|| BuiltinView {
        name: "mz_scheduling_elapsed_per_worker",
        schema: MZ_INTROSPECTION_SCHEMA,
        oid: oid::VIEW_MZ_SCHEDULING_ELAPSED_PER_WORKER_OID,
        desc: RelationDesc::builder()
            .with_column("id", SqlScalarType::UInt64.nullable(false))
            .with_column("worker_id", SqlScalarType::UInt64.nullable(false))
            .with_column("elapsed_ns", SqlScalarType::Int64.nullable(false))
            .with_key(vec![0, 1])
            .finish(),
        column_comments: BTreeMap::new(),
        sql: "SELECT
    id, worker_id, pg_catalog.count(*) AS elapsed_ns
FROM
    mz_introspection.mz_scheduling_elapsed_raw
GROUP BY
    id, worker_id",
        access: vec![PUBLIC_SELECT],
        ontology: None,
    });

pub static MZ_SCHEDULING_ELAPSED: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "mz_scheduling_elapsed",
    schema: MZ_INTROSPECTION_SCHEMA,
    oid: oid::VIEW_MZ_SCHEDULING_ELAPSED_OID,
    desc: RelationDesc::builder()
        .with_column("id", SqlScalarType::UInt64.nullable(false))
        .with_column(
            "elapsed_ns",
            SqlScalarType::Numeric {
                max_scale: Some(NumericMaxScale::ZERO),
            }
            .nullable(false),
        )
        .with_key(vec![0])
        .finish(),
    column_comments: BTreeMap::from_iter([
        (
            "id",
            "The ID of the operator. Corresponds to `mz_dataflow_operators.id`.",
        ),
        (
            "elapsed_ns",
            "The total elapsed time spent in the operator in nanoseconds.",
        ),
    ]),
    sql: "
SELECT
    id,
    pg_catalog.sum(elapsed_ns) AS elapsed_ns
FROM mz_introspection.mz_scheduling_elapsed_per_worker
GROUP BY id",
    access: vec![PUBLIC_SELECT],
    ontology: Some(Ontology {
        entity_name: "scheduling_elapsed",
        description: "CPU time spent per operator",
        links: &const {
            [OntologyLink {
                name: "elapsed_for_operator",
                target: "dataflow_operator",
                properties: LinkProperties::measures("id", "id", "cpu_time_ns"),
            }]
        },
        column_semantic_types: &[],
    }),
});

pub static MZ_COMPUTE_OPERATOR_DURATIONS_HISTOGRAM_PER_WORKER: LazyLock<BuiltinView> =
    LazyLock::new(|| BuiltinView {
        name: "mz_compute_operator_durations_histogram_per_worker",
        schema: MZ_INTROSPECTION_SCHEMA,
        oid: oid::VIEW_MZ_COMPUTE_OPERATOR_DURATIONS_HISTOGRAM_PER_WORKER_OID,
        desc: RelationDesc::builder()
            .with_column("id", SqlScalarType::UInt64.nullable(false))
            .with_column("worker_id", SqlScalarType::UInt64.nullable(false))
            .with_column("duration_ns", SqlScalarType::UInt64.nullable(false))
            .with_column("count", SqlScalarType::Int64.nullable(false))
            .with_key(vec![0, 1, 2])
            .finish(),
        column_comments: BTreeMap::new(),
        sql: "SELECT
    id, worker_id, duration_ns, pg_catalog.count(*) AS count
FROM
    mz_introspection.mz_compute_operator_durations_histogram_raw
GROUP BY
    id, worker_id, duration_ns",
        access: vec![PUBLIC_SELECT],
        ontology: None,
    });

pub static MZ_COMPUTE_OPERATOR_DURATIONS_HISTOGRAM: LazyLock<BuiltinView> =
    LazyLock::new(|| BuiltinView {
        name: "mz_compute_operator_durations_histogram",
        schema: MZ_INTROSPECTION_SCHEMA,
        oid: oid::VIEW_MZ_COMPUTE_OPERATOR_DURATIONS_HISTOGRAM_OID,
        desc: RelationDesc::builder()
            .with_column("id", SqlScalarType::UInt64.nullable(false))
            .with_column("duration_ns", SqlScalarType::UInt64.nullable(false))
            .with_column(
                "count",
                SqlScalarType::Numeric {
                    max_scale: Some(NumericMaxScale::ZERO),
                }
                .nullable(false),
            )
            .with_key(vec![0, 1])
            .finish(),
        column_comments: BTreeMap::from_iter([
            (
                "id",
                "The ID of the operator. Corresponds to `mz_dataflow_operators.id`.",
            ),
            (
                "duration_ns",
                "The upper bound of the duration bucket in nanoseconds.",
            ),
            (
                "count",
                "The (noncumulative) count of invocations in the bucket.",
            ),
        ]),
        sql: "
SELECT
    id,
    duration_ns,
    pg_catalog.sum(count) AS count
FROM mz_introspection.mz_compute_operator_durations_histogram_per_worker
GROUP BY id, duration_ns",
        access: vec![PUBLIC_SELECT],
        ontology: None,
    });

pub static MZ_SCHEDULING_PARKS_HISTOGRAM_PER_WORKER: LazyLock<BuiltinView> =
    LazyLock::new(|| BuiltinView {
        name: "mz_scheduling_parks_histogram_per_worker",
        schema: MZ_INTROSPECTION_SCHEMA,
        oid: oid::VIEW_MZ_SCHEDULING_PARKS_HISTOGRAM_PER_WORKER_OID,
        desc: RelationDesc::builder()
            .with_column("worker_id", SqlScalarType::UInt64.nullable(false))
            .with_column("slept_for_ns", SqlScalarType::UInt64.nullable(false))
            .with_column("requested_ns", SqlScalarType::UInt64.nullable(false))
            .with_column("count", SqlScalarType::Int64.nullable(false))
            .with_key(vec![0, 1, 2])
            .finish(),
        column_comments: BTreeMap::new(),
        sql: "SELECT
    worker_id, slept_for_ns, requested_ns, pg_catalog.count(*) AS count
FROM
    mz_introspection.mz_scheduling_parks_histogram_raw
GROUP BY
    worker_id, slept_for_ns, requested_ns",
        access: vec![PUBLIC_SELECT],
        ontology: None,
    });

pub static MZ_SCHEDULING_PARKS_HISTOGRAM: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "mz_scheduling_parks_histogram",
    schema: MZ_INTROSPECTION_SCHEMA,
    oid: oid::VIEW_MZ_SCHEDULING_PARKS_HISTOGRAM_OID,
    desc: RelationDesc::builder()
        .with_column("slept_for_ns", SqlScalarType::UInt64.nullable(false))
        .with_column("requested_ns", SqlScalarType::UInt64.nullable(false))
        .with_column(
            "count",
            SqlScalarType::Numeric {
                max_scale: Some(NumericMaxScale::ZERO),
            }
            .nullable(false),
        )
        .with_key(vec![0, 1])
        .finish(),
    column_comments: BTreeMap::from_iter([
        (
            "slept_for_ns",
            "The actual length of the park event in nanoseconds.",
        ),
        (
            "requested_ns",
            "The requested length of the park event in nanoseconds.",
        ),
        (
            "count",
            "The (noncumulative) count of park events in this bucket.",
        ),
    ]),
    sql: "
SELECT
    slept_for_ns,
    requested_ns,
    pg_catalog.sum(count) AS count
FROM mz_introspection.mz_scheduling_parks_histogram_per_worker
GROUP BY slept_for_ns, requested_ns",
    access: vec![PUBLIC_SELECT],
    ontology: Some(Ontology {
        entity_name: "scheduling_parks",
        description: "Histogram of operator park durations",
        links: &const { [] },
        column_semantic_types: &[],
    }),
});

pub static MZ_COMPUTE_ERROR_COUNTS_PER_WORKER: LazyLock<BuiltinView> =
    LazyLock::new(|| BuiltinView {
        name: "mz_compute_error_counts_per_worker",
        schema: MZ_INTROSPECTION_SCHEMA,
        oid: oid::VIEW_MZ_COMPUTE_ERROR_COUNTS_PER_WORKER_OID,
        desc: RelationDesc::builder()
            .with_column("export_id", SqlScalarType::String.nullable(false))
            .with_column("worker_id", SqlScalarType::UInt64.nullable(false))
            .with_column("count", SqlScalarType::Int64.nullable(false))
            .with_key(vec![0, 1, 2])
            .finish(),
        column_comments: BTreeMap::new(),
        sql: "
WITH MUTUALLY RECURSIVE
    -- Indexes that reuse existing indexes rather than maintaining separate dataflows.
    -- For these we don't log error counts separately, so we need to forward the error counts from
    -- their dependencies instead.
    index_reuses(reuse_id text, index_id text) AS (
        SELECT d.object_id, d.dependency_id
        FROM mz_internal.mz_compute_dependencies d
        JOIN mz_introspection.mz_compute_exports e ON (e.export_id = d.object_id)
        WHERE NOT EXISTS (
            SELECT 1 FROM mz_introspection.mz_dataflows
            WHERE id = e.dataflow_id
        )
    ),
    -- Error counts that were directly logged on compute exports.
    direct_errors(export_id text, worker_id uint8, count int8) AS (
        SELECT export_id, worker_id, count
        FROM mz_introspection.mz_compute_error_counts_raw
    ),
    -- Error counts propagated to index reused.
    all_errors(export_id text, worker_id uint8, count int8) AS (
        SELECT * FROM direct_errors
        UNION
        SELECT r.reuse_id, e.worker_id, e.count
        FROM all_errors e
        JOIN index_reuses r ON (r.index_id = e.export_id)
    )
SELECT * FROM all_errors",
        access: vec![PUBLIC_SELECT],
        ontology: Some(Ontology {
            entity_name: "compute_error_per_worker",
            description: "Error counts per compute collection per worker.",
            links: &const {
                [OntologyLink {
                    name: "errors_in",
                    target: "compute_export_per_worker",
                    properties: LinkProperties::measures_composite(
                        "export_id",
                        "export_id",
                        "count",
                        &[("worker_id", "worker_id")],
                    ),
                }]
            },
            column_semantic_types: &[("export_id", SemanticType::GlobalId)],
        }),
    });

pub static MZ_COMPUTE_ERROR_COUNTS: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "mz_compute_error_counts",
    schema: MZ_INTROSPECTION_SCHEMA,
    oid: oid::VIEW_MZ_COMPUTE_ERROR_COUNTS_OID,
    desc: RelationDesc::builder()
        .with_column("export_id", SqlScalarType::String.nullable(false))
        .with_column(
            "count",
            SqlScalarType::Numeric {
                max_scale: Some(NumericMaxScale::ZERO),
            }
            .nullable(false),
        )
        .with_key(vec![0])
        .finish(),
    column_comments: BTreeMap::from_iter([
        (
            "export_id",
            "The ID of the dataflow export. Corresponds to `mz_compute_exports.export_id`.",
        ),
        (
            "count",
            "The count of errors present in this dataflow export.",
        ),
    ]),
    sql: "
SELECT
    export_id,
    pg_catalog.sum(count) AS count
FROM mz_introspection.mz_compute_error_counts_per_worker
GROUP BY export_id
HAVING pg_catalog.sum(count) != 0",
    access: vec![PUBLIC_SELECT],
    ontology: Some(Ontology {
        entity_name: "compute_error_count",
        description: "Error counts per compute collection",
        links: &const {
            [OntologyLink {
                name: "errors_in",
                target: "compute_export",
                properties: LinkProperties::fk("export_id", "export_id", Cardinality::OneToOne),
            }]
        },
        column_semantic_types: &[("export_id", SemanticType::GlobalId)],
    }),
});

pub static MZ_MESSAGE_COUNTS_PER_WORKER: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "mz_message_counts_per_worker",
    schema: MZ_INTROSPECTION_SCHEMA,
    oid: oid::VIEW_MZ_MESSAGE_COUNTS_PER_WORKER_OID,
    desc: RelationDesc::builder()
        .with_column("channel_id", SqlScalarType::UInt64.nullable(false))
        .with_column("from_worker_id", SqlScalarType::UInt64.nullable(false))
        .with_column("to_worker_id", SqlScalarType::UInt64.nullable(false))
        .with_column("sent", SqlScalarType::Int64.nullable(false))
        .with_column("received", SqlScalarType::Int64.nullable(false))
        .with_column("batch_sent", SqlScalarType::Int64.nullable(false))
        .with_column("batch_received", SqlScalarType::Int64.nullable(false))
        .with_key(vec![0, 1, 2])
        .finish(),
    column_comments: BTreeMap::new(),
    sql: "
WITH batch_sent_cte AS (
    SELECT
        channel_id,
        from_worker_id,
        to_worker_id,
        pg_catalog.count(*) AS sent
    FROM
        mz_introspection.mz_message_batch_counts_sent_raw
    GROUP BY
        channel_id, from_worker_id, to_worker_id
),
batch_received_cte AS (
    SELECT
        channel_id,
        from_worker_id,
        to_worker_id,
        pg_catalog.count(*) AS received
    FROM
        mz_introspection.mz_message_batch_counts_received_raw
    GROUP BY
        channel_id, from_worker_id, to_worker_id
),
sent_cte AS (
    SELECT
        channel_id,
        from_worker_id,
        to_worker_id,
        pg_catalog.count(*) AS sent
    FROM
        mz_introspection.mz_message_counts_sent_raw
    GROUP BY
        channel_id, from_worker_id, to_worker_id
),
received_cte AS (
    SELECT
        channel_id,
        from_worker_id,
        to_worker_id,
        pg_catalog.count(*) AS received
    FROM
        mz_introspection.mz_message_counts_received_raw
    GROUP BY
        channel_id, from_worker_id, to_worker_id
)
SELECT
    sent_cte.channel_id,
    sent_cte.from_worker_id,
    sent_cte.to_worker_id,
    sent_cte.sent,
    received_cte.received,
    batch_sent_cte.sent AS batch_sent,
    batch_received_cte.received AS batch_received
FROM sent_cte
JOIN received_cte USING (channel_id, from_worker_id, to_worker_id)
JOIN batch_sent_cte USING (channel_id, from_worker_id, to_worker_id)
JOIN batch_received_cte USING (channel_id, from_worker_id, to_worker_id)",
    access: vec![PUBLIC_SELECT],
    ontology: None,
});

pub static MZ_MESSAGE_COUNTS: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "mz_message_counts",
    schema: MZ_INTROSPECTION_SCHEMA,
    oid: oid::VIEW_MZ_MESSAGE_COUNTS_OID,
    desc: RelationDesc::builder()
        .with_column("channel_id", SqlScalarType::UInt64.nullable(false))
        .with_column(
            "sent",
            SqlScalarType::Numeric {
                max_scale: Some(NumericMaxScale::ZERO),
            }
            .nullable(false),
        )
        .with_column(
            "received",
            SqlScalarType::Numeric {
                max_scale: Some(NumericMaxScale::ZERO),
            }
            .nullable(false),
        )
        .with_column(
            "batch_sent",
            SqlScalarType::Numeric {
                max_scale: Some(NumericMaxScale::ZERO),
            }
            .nullable(false),
        )
        .with_column(
            "batch_received",
            SqlScalarType::Numeric {
                max_scale: Some(NumericMaxScale::ZERO),
            }
            .nullable(false),
        )
        .with_key(vec![0])
        .finish(),
    column_comments: BTreeMap::from_iter([
        (
            "channel_id",
            "The ID of the channel. Corresponds to `mz_dataflow_channels.id`.",
        ),
        ("sent", "The number of messages sent."),
        ("received", "The number of messages received."),
        ("batch_sent", "The number of batches sent."),
        ("batch_received", "The number of batches received."),
    ]),
    sql: "
SELECT
    channel_id,
    pg_catalog.sum(sent) AS sent,
    pg_catalog.sum(received) AS received,
    pg_catalog.sum(batch_sent) AS batch_sent,
    pg_catalog.sum(batch_received) AS batch_received
FROM mz_introspection.mz_message_counts_per_worker
GROUP BY channel_id",
    access: vec![PUBLIC_SELECT],
    ontology: Some(Ontology {
        entity_name: "message_count",
        description: "Inter-worker message counts",
        links: &const {
            [OntologyLink {
                name: "counts_for",
                target: "dataflow_channel",
                properties: LinkProperties::fk("channel_id", "id", Cardinality::OneToOne),
            }]
        },
        column_semantic_types: &[],
    }),
});

pub static MZ_ACTIVE_PEEKS: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "mz_active_peeks",
    schema: MZ_INTROSPECTION_SCHEMA,
    oid: oid::VIEW_MZ_ACTIVE_PEEKS_OID,
    desc: RelationDesc::builder()
        .with_column("id", SqlScalarType::Uuid.nullable(false))
        .with_column("object_id", SqlScalarType::String.nullable(false))
        .with_column("type", SqlScalarType::String.nullable(false))
        .with_column("time", SqlScalarType::MzTimestamp.nullable(false))
        .with_key(vec![0])
        .finish(),
    column_comments: BTreeMap::from_iter([
        ("id", "The ID of the peek request."),
        (
            "object_id",
            "The ID of the collection the peek is targeting. Corresponds to `mz_catalog.mz_indexes.id`, `mz_catalog.mz_materialized_views.id`, `mz_catalog.mz_sources.id`, or `mz_catalog.mz_tables.id`.",
        ),
        (
            "type",
            "The type of the corresponding peek: `index` if targeting an index or temporary dataflow; `persist` for a source, materialized view, or table.",
        ),
        ("time", "The timestamp the peek has requested."),
    ]),
    sql: "
SELECT id, object_id, type, time
FROM mz_introspection.mz_active_peeks_per_worker
WHERE worker_id = 0::uint8",
    access: vec![PUBLIC_SELECT],
    ontology: Some(Ontology {
        entity_name: "active_peek",
        description: "Currently executing SELECT queries",
        links: &const { [] },
        column_semantic_types: &const {
            [
                ("object_id", SemanticType::GlobalId),
                ("time", SemanticType::MzTimestamp),
            ]
        },
    }),
});

pub static MZ_DATAFLOW_OPERATOR_REACHABILITY_PER_WORKER: LazyLock<BuiltinView> =
    LazyLock::new(|| BuiltinView {
        name: "mz_dataflow_operator_reachability_per_worker",
        schema: MZ_INTROSPECTION_SCHEMA,
        oid: oid::VIEW_MZ_DATAFLOW_OPERATOR_REACHABILITY_PER_WORKER_OID,
        desc: RelationDesc::builder()
            .with_column("id", SqlScalarType::UInt64.nullable(false))
            .with_column("worker_id", SqlScalarType::UInt64.nullable(false))
            .with_column("port", SqlScalarType::UInt64.nullable(false))
            .with_column("update_type", SqlScalarType::String.nullable(false))
            .with_column("time", SqlScalarType::MzTimestamp.nullable(true))
            .with_column("count", SqlScalarType::Int64.nullable(false))
            .with_key(vec![0, 1, 2, 3, 4])
            .finish(),
        column_comments: BTreeMap::new(),
        sql: "SELECT
    addr2.id,
    reachability.worker_id,
    port,
    update_type,
    time,
    pg_catalog.count(*) as count
FROM
    mz_introspection.mz_dataflow_operator_reachability_raw reachability,
    mz_introspection.mz_dataflow_addresses_per_worker addr1,
    mz_introspection.mz_dataflow_addresses_per_worker addr2
WHERE
    addr2.address =
    CASE
        WHEN source = 0 THEN addr1.address
        ELSE addr1.address || reachability.source
    END
    AND addr1.id = reachability.id
    AND addr1.worker_id = reachability.worker_id
    AND addr2.worker_id = reachability.worker_id
GROUP BY addr2.id, reachability.worker_id, port, update_type, time",
        access: vec![PUBLIC_SELECT],
        ontology: None,
    });

pub static MZ_DATAFLOW_OPERATOR_REACHABILITY: LazyLock<BuiltinView> =
    LazyLock::new(|| BuiltinView {
        name: "mz_dataflow_operator_reachability",
        schema: MZ_INTROSPECTION_SCHEMA,
        oid: oid::VIEW_MZ_DATAFLOW_OPERATOR_REACHABILITY_OID,
        desc: RelationDesc::builder()
            .with_column("id", SqlScalarType::UInt64.nullable(false))
            .with_column("port", SqlScalarType::UInt64.nullable(false))
            .with_column("update_type", SqlScalarType::String.nullable(false))
            .with_column("time", SqlScalarType::MzTimestamp.nullable(true))
            .with_column(
                "count",
                SqlScalarType::Numeric {
                    max_scale: Some(NumericMaxScale::ZERO),
                }
                .nullable(false),
            )
            .with_key(vec![0, 1, 2, 3])
            .finish(),
        column_comments: BTreeMap::new(),
        sql: "
SELECT
    id,
    port,
    update_type,
    time,
    pg_catalog.sum(count) as count
FROM mz_introspection.mz_dataflow_operator_reachability_per_worker
GROUP BY id, port, update_type, time",
        access: vec![PUBLIC_SELECT],
        ontology: None,
    });

pub static MZ_ARRANGEMENT_SIZES_PER_WORKER: LazyLock<BuiltinView> = LazyLock::new(|| {
    BuiltinView {
        name: "mz_arrangement_sizes_per_worker",
        schema: MZ_INTROSPECTION_SCHEMA,
        oid: oid::VIEW_MZ_ARRANGEMENT_SIZES_PER_WORKER_OID,
        desc: RelationDesc::builder()
            .with_column("operator_id", SqlScalarType::UInt64.nullable(false))
            .with_column("worker_id", SqlScalarType::UInt64.nullable(false))
            .with_column("records", SqlScalarType::Int64.nullable(true))
            .with_column("batches", SqlScalarType::Int64.nullable(true))
            .with_column("size", SqlScalarType::Int64.nullable(true))
            .with_column("capacity", SqlScalarType::Int64.nullable(true))
            .with_column("allocations", SqlScalarType::Int64.nullable(true))
            .finish(),
        column_comments: BTreeMap::new(),
        sql: "
WITH operators_per_worker_cte AS (
    SELECT
        id AS operator_id,
        worker_id
    FROM
        mz_introspection.mz_dataflow_operators_per_worker
),
batches_cte AS (
    SELECT
        operator_id,
        worker_id,
        COUNT(*) AS batches
    FROM
        mz_introspection.mz_arrangement_batches_raw
    GROUP BY
        operator_id, worker_id
),
records_cte AS (
    SELECT
        operator_id,
        worker_id,
        COUNT(*) AS records
    FROM
        mz_introspection.mz_arrangement_records_raw
    GROUP BY
        operator_id, worker_id
),
heap_size_cte AS (
    SELECT
        operator_id,
        worker_id,
        COUNT(*) AS size
    FROM
        mz_introspection.mz_arrangement_heap_size_raw
    GROUP BY
        operator_id, worker_id
),
heap_capacity_cte AS (
    SELECT
        operator_id,
        worker_id,
        COUNT(*) AS capacity
    FROM
        mz_introspection.mz_arrangement_heap_capacity_raw
    GROUP BY
        operator_id, worker_id
),
heap_allocations_cte AS (
    SELECT
        operator_id,
        worker_id,
        COUNT(*) AS allocations
    FROM
        mz_introspection.mz_arrangement_heap_allocations_raw
    GROUP BY
        operator_id, worker_id
),
batcher_records_cte AS (
    SELECT
        operator_id,
        worker_id,
        COUNT(*) AS records
    FROM
        mz_introspection.mz_arrangement_batcher_records_raw
    GROUP BY
        operator_id, worker_id
),
batcher_size_cte AS (
    SELECT
        operator_id,
        worker_id,
        COUNT(*) AS size
    FROM
        mz_introspection.mz_arrangement_batcher_size_raw
    GROUP BY
        operator_id, worker_id
),
batcher_capacity_cte AS (
    SELECT
        operator_id,
        worker_id,
        COUNT(*) AS capacity
    FROM
        mz_introspection.mz_arrangement_batcher_capacity_raw
    GROUP BY
        operator_id, worker_id
),
batcher_allocations_cte AS (
    SELECT
        operator_id,
        worker_id,
        COUNT(*) AS allocations
    FROM
        mz_introspection.mz_arrangement_batcher_allocations_raw
    GROUP BY
        operator_id, worker_id
),
combined AS (
    SELECT
        opw.operator_id,
        opw.worker_id,
        CASE
            WHEN records_cte.records IS NULL AND batcher_records_cte.records IS NULL THEN NULL
            ELSE COALESCE(records_cte.records, 0) + COALESCE(batcher_records_cte.records, 0)
        END AS records,
        batches_cte.batches AS batches,
        CASE
            WHEN heap_size_cte.size IS NULL AND batcher_size_cte.size IS NULL THEN NULL
            ELSE COALESCE(heap_size_cte.size, 0) + COALESCE(batcher_size_cte.size, 0)
        END AS size,
        CASE
            WHEN heap_capacity_cte.capacity IS NULL AND batcher_capacity_cte.capacity IS NULL THEN NULL
            ELSE COALESCE(heap_capacity_cte.capacity, 0) + COALESCE(batcher_capacity_cte.capacity, 0)
        END AS capacity,
        CASE
            WHEN heap_allocations_cte.allocations IS NULL AND batcher_allocations_cte.allocations IS NULL THEN NULL
            ELSE COALESCE(heap_allocations_cte.allocations, 0) + COALESCE(batcher_allocations_cte.allocations, 0)
        END AS allocations
    FROM
                    operators_per_worker_cte opw
    LEFT OUTER JOIN batches_cte USING (operator_id, worker_id)
    LEFT OUTER JOIN records_cte USING (operator_id, worker_id)
    LEFT OUTER JOIN heap_size_cte USING (operator_id, worker_id)
    LEFT OUTER JOIN heap_capacity_cte USING (operator_id, worker_id)
    LEFT OUTER JOIN heap_allocations_cte USING (operator_id, worker_id)
    LEFT OUTER JOIN batcher_records_cte USING (operator_id, worker_id)
    LEFT OUTER JOIN batcher_size_cte USING (operator_id, worker_id)
    LEFT OUTER JOIN batcher_capacity_cte USING (operator_id, worker_id)
    LEFT OUTER JOIN batcher_allocations_cte USING (operator_id, worker_id)
)
SELECT
    operator_id, worker_id, records, batches, size, capacity, allocations
FROM combined
WHERE
       records     IS NOT NULL
    OR batches     IS NOT NULL
    OR size        IS NOT NULL
    OR capacity    IS NOT NULL
    OR allocations IS NOT NULL
",
        access: vec![PUBLIC_SELECT],
        ontology: None,
    }
});

pub static MZ_ARRANGEMENT_SIZES: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "mz_arrangement_sizes",
    schema: MZ_INTROSPECTION_SCHEMA,
    oid: oid::VIEW_MZ_ARRANGEMENT_SIZES_OID,
    desc: RelationDesc::builder()
        .with_column("operator_id", SqlScalarType::UInt64.nullable(false))
        .with_column("records", SqlScalarType::Int64.nullable(true))
        .with_column("batches", SqlScalarType::Int64.nullable(true))
        .with_column("size", SqlScalarType::Int64.nullable(true))
        .with_column("capacity", SqlScalarType::Int64.nullable(true))
        .with_column("allocations", SqlScalarType::Int64.nullable(true))
        .with_key(vec![0])
        .finish(),
    column_comments: BTreeMap::from_iter([
        (
            "operator_id",
            "The ID of the operator that created the arrangement. Corresponds to `mz_dataflow_operators.id`.",
        ),
        ("records", "The number of records in the arrangement."),
        ("batches", "The number of batches in the arrangement."),
        ("size", "The utilized size in bytes of the arrangement."),
        (
            "capacity",
            "The capacity in bytes of the arrangement. Can be larger than the size.",
        ),
        (
            "allocations",
            "The number of separate memory allocations backing the arrangement.",
        ),
    ]),
    sql: "
SELECT
    operator_id,
    SUM(records)::int8 AS records,
    SUM(batches)::int8 AS batches,
    SUM(size)::int8 AS size,
    SUM(capacity)::int8 AS capacity,
    SUM(allocations)::int8 AS allocations
FROM mz_introspection.mz_arrangement_sizes_per_worker
GROUP BY operator_id",
    access: vec![PUBLIC_SELECT],
    ontology: Some(Ontology {
        entity_name: "arrangement_size",
        description: "Aggregated arrangement sizes (records, batches, bytes)",
        links: &const {
            [OntologyLink {
                name: "arrangement_of_operator",
                target: "dataflow_operator",
                properties: LinkProperties::Measures {
                    source_column: "operator_id",
                    target_column: "id",
                    metric: "arrangement_size",
                    source_id_type: None,
                    requires_mapping: None,
                    note: Some(
                        "Both IDs are local uint64 operator IDs within a dataflow, not GlobalIds.",
                    ),
                    extra_key_columns: None,
                },
            }]
        },
        column_semantic_types: &[],
    }),
});

pub static MZ_ARRANGEMENT_SHARING_PER_WORKER: LazyLock<BuiltinView> =
    LazyLock::new(|| BuiltinView {
        name: "mz_arrangement_sharing_per_worker",
        schema: MZ_INTROSPECTION_SCHEMA,
        oid: oid::VIEW_MZ_ARRANGEMENT_SHARING_PER_WORKER_OID,
        desc: RelationDesc::builder()
            .with_column("operator_id", SqlScalarType::UInt64.nullable(false))
            .with_column("worker_id", SqlScalarType::UInt64.nullable(false))
            .with_column("count", SqlScalarType::Int64.nullable(false))
            .with_key(vec![0, 1])
            .finish(),
        column_comments: BTreeMap::new(),
        sql: "
SELECT
    operator_id,
    worker_id,
    pg_catalog.count(*) AS count
FROM mz_introspection.mz_arrangement_sharing_raw
GROUP BY operator_id, worker_id",
        access: vec![PUBLIC_SELECT],
        ontology: None,
    });

pub static MZ_ARRANGEMENT_SHARING: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "mz_arrangement_sharing",
    schema: MZ_INTROSPECTION_SCHEMA,
    oid: oid::VIEW_MZ_ARRANGEMENT_SHARING_OID,
    desc: RelationDesc::builder()
        .with_column("operator_id", SqlScalarType::UInt64.nullable(false))
        .with_column("count", SqlScalarType::Int64.nullable(false))
        .with_key(vec![0])
        .finish(),
    column_comments: BTreeMap::from_iter([
        (
            "operator_id",
            "The ID of the operator that created the arrangement. Corresponds to `mz_dataflow_operators.id`.",
        ),
        (
            "count",
            "The number of operators that share the arrangement.",
        ),
    ]),
    sql: "
SELECT operator_id, count
FROM mz_introspection.mz_arrangement_sharing_per_worker
WHERE worker_id = 0::uint8",
    access: vec![PUBLIC_SELECT],
    ontology: Some(Ontology {
        entity_name: "arrangement_sharing",
        description: "Arrangement sharing between operators",
        links: &const {
            [OntologyLink {
                name: "shared_by",
                target: "dataflow_operator",
                properties: LinkProperties::fk("operator_id", "id", Cardinality::OneToOne),
            }]
        },
        column_semantic_types: &[],
    }),
});

pub static MZ_DATAFLOW_OPERATOR_PARENTS_PER_WORKER: LazyLock<BuiltinView> =
    LazyLock::new(|| BuiltinView {
        name: "mz_dataflow_operator_parents_per_worker",
        schema: MZ_INTROSPECTION_SCHEMA,
        oid: oid::VIEW_MZ_DATAFLOW_OPERATOR_PARENTS_PER_WORKER_OID,
        desc: RelationDesc::builder()
            .with_column("id", SqlScalarType::UInt64.nullable(false))
            .with_column("parent_id", SqlScalarType::UInt64.nullable(false))
            .with_column("worker_id", SqlScalarType::UInt64.nullable(false))
            .finish(),
        column_comments: BTreeMap::new(),
        sql: "
WITH operator_addrs AS(
    SELECT
        id, address, worker_id
    FROM mz_introspection.mz_dataflow_addresses_per_worker
        INNER JOIN mz_introspection.mz_dataflow_operators_per_worker
            USING (id, worker_id)
),
parent_addrs AS (
    SELECT
        id,
        address[1:list_length(address) - 1] AS parent_address,
        worker_id
    FROM operator_addrs
)
SELECT pa.id, oa.id AS parent_id, pa.worker_id
FROM parent_addrs AS pa
    INNER JOIN operator_addrs AS oa
        ON pa.parent_address = oa.address
        AND pa.worker_id = oa.worker_id",
        access: vec![PUBLIC_SELECT],
        ontology: None,
    });

pub static MZ_DATAFLOW_OPERATOR_PARENTS: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "mz_dataflow_operator_parents",
    schema: MZ_INTROSPECTION_SCHEMA,
    oid: oid::VIEW_MZ_DATAFLOW_OPERATOR_PARENTS_OID,
    desc: RelationDesc::builder()
        .with_column("id", SqlScalarType::UInt64.nullable(false))
        .with_column("parent_id", SqlScalarType::UInt64.nullable(false))
        .finish(),
    column_comments: BTreeMap::from_iter([
        (
            "id",
            "The ID of the operator. Corresponds to `mz_dataflow_operators.id`.",
        ),
        (
            "parent_id",
            "The ID of the operator's parent operator. Corresponds to `mz_dataflow_operators.id`.",
        ),
    ]),
    sql: "
SELECT id, parent_id
FROM mz_introspection.mz_dataflow_operator_parents_per_worker
WHERE worker_id = 0::uint8",
    access: vec![PUBLIC_SELECT],
    ontology: None,
});

pub static MZ_DATAFLOW_ARRANGEMENT_SIZES: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "mz_dataflow_arrangement_sizes",
    schema: MZ_INTROSPECTION_SCHEMA,
    oid: oid::VIEW_MZ_DATAFLOW_ARRANGEMENT_SIZES_OID,
    desc: RelationDesc::builder()
        .with_column("id", SqlScalarType::UInt64.nullable(false))
        .with_column("name", SqlScalarType::String.nullable(false))
        .with_column("records", SqlScalarType::Int64.nullable(true))
        .with_column("batches", SqlScalarType::Int64.nullable(true))
        .with_column("size", SqlScalarType::Int64.nullable(true))
        .with_column("capacity", SqlScalarType::Int64.nullable(true))
        .with_column("allocations", SqlScalarType::Int64.nullable(true))
        .with_key(vec![0, 1])
        .finish(),
    column_comments: BTreeMap::from_iter([
        (
            "id",
            "The ID of the [dataflow]. Corresponds to `mz_dataflows.id`.",
        ),
        ("name", "The name of the [dataflow]."),
        (
            "records",
            "The number of records in all arrangements in the dataflow.",
        ),
        (
            "batches",
            "The number of batches in all arrangements in the dataflow.",
        ),
        ("size", "The utilized size in bytes of the arrangements."),
        (
            "capacity",
            "The capacity in bytes of the arrangements. Can be larger than the size.",
        ),
        (
            "allocations",
            "The number of separate memory allocations backing the arrangements.",
        ),
    ]),
    sql: "
SELECT
    mdod.dataflow_id AS id,
    mdod.dataflow_name AS name,
    SUM(mas.records)::int8 AS records,
    SUM(mas.batches)::int8 AS batches,
    SUM(mas.size)::int8 AS size,
    SUM(mas.capacity)::int8 AS capacity,
    SUM(mas.allocations)::int8 AS allocations
FROM mz_introspection.mz_dataflow_operator_dataflows AS mdod
LEFT JOIN mz_introspection.mz_arrangement_sizes AS mas
    ON mdod.id = mas.operator_id
GROUP BY mdod.dataflow_id, mdod.dataflow_name",
    access: vec![PUBLIC_SELECT],
    ontology: None,
});

pub static MZ_EXPECTED_GROUP_SIZE_ADVICE: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "mz_expected_group_size_advice",
    schema: MZ_INTROSPECTION_SCHEMA,
    oid: oid::VIEW_MZ_EXPECTED_GROUP_SIZE_ADVICE_OID,
    desc: RelationDesc::builder()
        .with_column("dataflow_id", SqlScalarType::UInt64.nullable(false))
        .with_column("dataflow_name", SqlScalarType::String.nullable(false))
        .with_column("region_id", SqlScalarType::UInt64.nullable(false))
        .with_column("region_name", SqlScalarType::String.nullable(false))
        .with_column("levels", SqlScalarType::Int64.nullable(false))
        .with_column("to_cut", SqlScalarType::Int64.nullable(false))
        .with_column(
            "savings",
            SqlScalarType::Numeric {
                max_scale: Some(NumericMaxScale::ZERO),
            }
            .nullable(true),
        )
        .with_column("hint", SqlScalarType::Float64.nullable(false))
        .finish(),
    column_comments: BTreeMap::from_iter([
        (
            "dataflow_id",
            "The ID of the [dataflow]. Corresponds to `mz_dataflows.id`.",
        ),
        (
            "dataflow_name",
            "The internal name of the dataflow hosting the min/max aggregation or Top K.",
        ),
        (
            "region_id",
            "The ID of the root operator scope. Corresponds to `mz_dataflow_operators.id`.",
        ),
        (
            "region_name",
            "The internal name of the root operator scope for the min/max aggregation or Top K.",
        ),
        (
            "levels",
            "The number of levels in the hierarchical scheme implemented by the region.",
        ),
        (
            "to_cut",
            "The number of levels that can be eliminated (cut) from the region's hierarchy.",
        ),
        (
            "savings",
            "A conservative estimate of the amount of memory in bytes to be saved by applying the hint.",
        ),
        (
            "hint",
            "The hint value that will eliminate `to_cut` levels from the region's hierarchy.",
        ),
    ]),
    sql: "
        -- The mz_expected_group_size_advice view provides tuning suggestions for the GROUP SIZE
        -- query hints. This tuning hint is effective for min/max/top-k patterns, where a stack
        -- of arrangements must be built. For each dataflow and region corresponding to one
        -- such pattern, we look for how many levels can be eliminated without hitting a level
        -- that actually substantially filters the input. The advice is constructed so that
        -- setting the hint for the affected region will eliminate these redundant levels of
        -- the hierarchical rendering.
        --
        -- A number of helper CTEs are used for the view definition. The first one, operators,
        -- looks for operator names that comprise arrangements of inputs to each level of a
        -- min/max/top-k hierarchy.
        WITH operators AS (
            SELECT
                dod.dataflow_id,
                dor.id AS region_id,
                dod.id,
                ars.records,
                ars.size
            FROM
                mz_introspection.mz_dataflow_operator_dataflows dod
                JOIN mz_introspection.mz_dataflow_addresses doa
                    ON dod.id = doa.id
                JOIN mz_introspection.mz_dataflow_addresses dra
                    ON dra.address = doa.address[:list_length(doa.address) - 1]
                JOIN mz_introspection.mz_dataflow_operators dor
                    ON dor.id = dra.id
                JOIN mz_introspection.mz_arrangement_sizes ars
                    ON ars.operator_id = dod.id
            WHERE
                dod.name = 'Arranged TopK input'
                OR dod.name = 'Arranged MinsMaxesHierarchical input'
                OR dod.name = 'Arrange ReduceMinsMaxes'
            ),
        -- The second CTE, levels, simply computes the heights of the min/max/top-k hierarchies
        -- identified in operators above.
        levels AS (
            SELECT o.dataflow_id, o.region_id, COUNT(*) AS levels
            FROM operators o
            GROUP BY o.dataflow_id, o.region_id
        ),
        -- The third CTE, pivot, determines for each min/max/top-k hierarchy, the first input
        -- operator. This operator is crucially important, as it records the number of records
        -- that was given as input to the gadget as a whole.
        pivot AS (
            SELECT
                o1.dataflow_id,
                o1.region_id,
                o1.id,
                o1.records
            FROM operators o1
            WHERE
                o1.id = (
                    SELECT MIN(o2.id)
                    FROM operators o2
                    WHERE
                        o2.dataflow_id = o1.dataflow_id
                        AND o2.region_id = o1.region_id
                    OPTIONS (AGGREGATE INPUT GROUP SIZE = 8)
                )
        ),
        -- The fourth CTE, candidates, will look for operators where the number of records
        -- maintained is not significantly different from the number at the pivot (excluding
        -- the pivot itself). These are the candidates for being cut from the dataflow region
        -- by adjusting the hint. The query includes a constant, heuristically tuned on TPC-H
        -- load generator data, to give some room for small deviations in number of records.
        -- The intuition for allowing for this deviation is that we are looking for a strongly
        -- reducing point in the hierarchy. To see why one such operator ought to exist in an
        -- untuned hierarchy, consider that at each level, we use hashing to distribute rows
        -- among groups where the min/max/top-k computation is (partially) applied. If the
        -- hierarchy has too many levels, the first-level (pivot) groups will be such that many
        -- groups might be empty or contain only one row. Each subsequent level will have a number
        -- of groups that is reduced exponentially. So at some point, we will find the level where
        -- we actually start having a few rows per group. That's where we will see the row counts
        -- significantly drop off.
        candidates AS (
            SELECT
                o.dataflow_id,
                o.region_id,
                o.id,
                o.records,
                o.size
            FROM
                operators o
                JOIN pivot p
                    ON o.dataflow_id = p.dataflow_id
                        AND o.region_id = p.region_id
                        AND o.id <> p.id
            WHERE o.records >= p.records * (1 - 0.15)
        ),
        -- The fifth CTE, cuts, computes for each relevant dataflow region, the number of
        -- candidate levels that should be cut. We only return here dataflow regions where at
        -- least one level must be cut. Note that once we hit a point where the hierarchy starts
        -- to have a filtering effect, i.e., after the last candidate, it is dangerous to suggest
        -- cutting the height of the hierarchy further. This is because we will have way less
        -- groups in the next level, so there should be even further reduction happening or there
        -- is some substantial skew in the data. But if the latter is the case, then we should not
        -- tune the GROUP SIZE hints down anyway to avoid hurting latency upon updates directed
        -- at these unusually large groups. In addition to selecting the levels to cut, we also
        -- compute a conservative estimate of the memory savings in bytes that will result from
        -- cutting these levels from the hierarchy. The estimate is based on the sizes of the
        -- input arrangements for each level to be cut. These arrangements should dominate the
        -- size of each level that can be cut, since the reduction gadget internal to the level
        -- does not remove much data at these levels.
        cuts AS (
            SELECT c.dataflow_id, c.region_id, COUNT(*) AS to_cut, SUM(c.size) AS savings
            FROM candidates c
            GROUP BY c.dataflow_id, c.region_id
            HAVING COUNT(*) > 0
        )
        -- Finally, we compute the hint suggestion for each dataflow region based on the number of
        -- levels and the number of candidates to be cut. The hint is computed taking into account
        -- the fan-in used in rendering for the hash partitioning and reduction of the groups,
        -- currently equal to 16.
        SELECT
            dod.dataflow_id,
            dod.dataflow_name,
            dod.id AS region_id,
            dod.name AS region_name,
            l.levels,
            c.to_cut,
            c.savings,
            pow(16, l.levels - c.to_cut) - 1 AS hint
        FROM cuts c
            JOIN levels l
                ON c.dataflow_id = l.dataflow_id AND c.region_id = l.region_id
            JOIN mz_introspection.mz_dataflow_operator_dataflows dod
                ON dod.dataflow_id = c.dataflow_id AND dod.id = c.region_id",
    access: vec![PUBLIC_SELECT],
    ontology: Some(Ontology {
        entity_name: "group_size_advice",
        description: "Advice on expected group sizes for reduce operators",
        links: &const { [] },
        column_semantic_types: &[],
    }),
});
