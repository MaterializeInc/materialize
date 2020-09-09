// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Built-in catalog items.
//!
//! Builtins exist in the `mz_catalog` ambient schema. They are automatically
//! installed into the catalog when it is opened. Their definitions are not
//! persisted in the catalog, but hardcoded in this module. This makes it easy
//! to add new builtins, or change the definition of existing builtins, in new
//! versions of Materialize.
//!
//! Builtin's names, columns, and types are part of the stable API of
//! Materialize. Be careful to maintain backwards compatibility when changing
//! definitions of existing builtins!
//!
//! More information about builtin system tables and types can be found in
//! https://materialize.io/docs/sql/system-tables/.

use std::collections::BTreeMap;

use lazy_static::lazy_static;

use dataflow_types::logging::{DifferentialLog, LogVariant, MaterializedLog, TimelyLog};
use expr::GlobalId;
use repr::{RelationDesc, ScalarType};

pub enum Builtin {
    Log(&'static BuiltinLog),
    Table(&'static BuiltinTable),
    View(&'static BuiltinView),
}

impl Builtin {
    pub fn name(&self) -> &'static str {
        match self {
            Builtin::Log(log) => log.name,
            Builtin::Table(table) => table.name,
            Builtin::View(view) => view.name,
        }
    }

    pub fn id(&self) -> GlobalId {
        match self {
            Builtin::Log(log) => log.id,
            Builtin::Table(table) => table.id,
            Builtin::View(view) => view.id,
        }
    }
}

pub struct BuiltinLog {
    pub variant: LogVariant,
    pub name: &'static str,
    pub id: GlobalId,
    pub index_id: GlobalId,
}

pub struct BuiltinTable {
    pub name: &'static str,
    pub desc: RelationDesc,
    pub id: GlobalId,
    pub index_id: GlobalId,
}

pub struct BuiltinView {
    pub name: &'static str,
    pub sql: &'static str,
    pub id: GlobalId,
}

// Builtin definitions below. Keep these sorted by global ID, and ensure you
// add new builtins to the `BUILTINS` map.
//
// Builtins are loaded in ID order, so sorting them by global ID makes the makes
// the source code definition order match the load order.
//
// A builtin must appear AFTER any items it depends upon. This means you may
// need to reorder IDs if you change the dependency set of an existing builtin.
// Unlike user IDs, system IDs are not persisted in the catalog, so it's safe to
// change a builtin's ID.
//
// Allocate IDs from the following ranges based on the item's type:
//
// | Item type | ID range  |
// | ----------|-----------|
// | Logs      | 1000-1999 |
// | Tables    | 2000-2999 |
// | Views     | 3000-3999 |
//
// WARNING: if you change the definition of an existing builtin item, you must
// be careful to maintain backwards compatibility! Adding new columns is safe.
// Removing a column, changing the name of a column, or changing the type of a
// column is not safe, as persisted user views may depend upon that column.

pub const MZ_DATAFLOW_OPERATORS: BuiltinLog = BuiltinLog {
    name: "mz_dataflow_operators",
    variant: LogVariant::Timely(TimelyLog::Operates),
    id: GlobalId::System(1000),
    index_id: GlobalId::System(1001),
};

pub const MZ_DATAFLOW_OPERATORS_ADDRESSES: BuiltinLog = BuiltinLog {
    name: "mz_dataflow_operator_addresses",
    variant: LogVariant::Timely(TimelyLog::Addresses),
    id: GlobalId::System(1002),
    index_id: GlobalId::System(1003),
};

pub const MZ_DATAFLOW_CHANNELS: BuiltinLog = BuiltinLog {
    name: "mz_dataflow_channels",
    variant: LogVariant::Timely(TimelyLog::Channels),
    id: GlobalId::System(1004),
    index_id: GlobalId::System(1005),
};

pub const MZ_SCHEDULING_ELAPSED: BuiltinLog = BuiltinLog {
    name: "mz_scheduling_elapsed",
    variant: LogVariant::Timely(TimelyLog::Elapsed),
    id: GlobalId::System(1006),
    index_id: GlobalId::System(1007),
};

pub const MZ_SCHEDULING_HISTOGRAM: BuiltinLog = BuiltinLog {
    name: "mz_scheduling_histogram",
    variant: LogVariant::Timely(TimelyLog::Histogram),
    id: GlobalId::System(1008),
    index_id: GlobalId::System(1009),
};

pub const MZ_SCHEDULING_PARKS: BuiltinLog = BuiltinLog {
    name: "mz_scheduling_parks",
    variant: LogVariant::Timely(TimelyLog::Parks),
    id: GlobalId::System(1010),
    index_id: GlobalId::System(1011),
};

pub const MZ_ARRANGEMENT_SIZES: BuiltinLog = BuiltinLog {
    name: "mz_arrangement_sizes",
    variant: LogVariant::Differential(DifferentialLog::Arrangement),
    id: GlobalId::System(1012),
    index_id: GlobalId::System(1013),
};

pub const MZ_ARRANGEMENT_SHARING: BuiltinLog = BuiltinLog {
    name: "mz_arrangement_sharing",
    variant: LogVariant::Differential(DifferentialLog::Sharing),
    id: GlobalId::System(1014),
    index_id: GlobalId::System(1015),
};

pub const MZ_MATERIALIZATIONS: BuiltinLog = BuiltinLog {
    name: "mz_materializations",
    variant: LogVariant::Materialized(MaterializedLog::DataflowCurrent),
    id: GlobalId::System(1016),
    index_id: GlobalId::System(1017),
};

pub const MZ_MATERIALIZATION_DEPENDENCIES: BuiltinLog = BuiltinLog {
    name: "mz_materialization_dependencies",
    variant: LogVariant::Materialized(MaterializedLog::DataflowDependency),
    id: GlobalId::System(1018),
    index_id: GlobalId::System(1019),
};

pub const MZ_WORKER_MATERIALIZATION_FRONTIERS: BuiltinLog = BuiltinLog {
    name: "mz_worker_materialization_frontiers",
    variant: LogVariant::Materialized(MaterializedLog::FrontierCurrent),
    id: GlobalId::System(1020),
    index_id: GlobalId::System(1021),
};

pub const MZ_PEEK_ACTIVE: BuiltinLog = BuiltinLog {
    name: "mz_peek_active",
    variant: LogVariant::Materialized(MaterializedLog::PeekCurrent),
    id: GlobalId::System(1022),
    index_id: GlobalId::System(1023),
};

pub const MZ_PEEK_DURATIONS: BuiltinLog = BuiltinLog {
    name: "mz_peek_durations",
    variant: LogVariant::Materialized(MaterializedLog::PeekDuration),
    id: GlobalId::System(1024),
    index_id: GlobalId::System(1025),
};

lazy_static! {
    pub static ref MZ_VIEW_KEYS: BuiltinTable = BuiltinTable {
        name: "mz_view_keys",
        desc: RelationDesc::empty()
            .with_column("global_id", ScalarType::String.nullable(false))
            .with_column("column", ScalarType::Int64.nullable(false))
            .with_column("key_group", ScalarType::Int64.nullable(false)),
        id: GlobalId::System(2001),
        index_id: GlobalId::System(2002),
    };
    pub static ref MZ_VIEW_FOREIGN_KEYS: BuiltinTable = BuiltinTable {
        name: "mz_view_foreign_keys",
        desc: RelationDesc::empty()
            .with_column("child_id", ScalarType::String.nullable(false))
            .with_column("child_column", ScalarType::Int64.nullable(false))
            .with_column("parent_id", ScalarType::String.nullable(false))
            .with_column("parent_column", ScalarType::Int64.nullable(false))
            .with_column("key_group", ScalarType::Int64.nullable(false))
            .with_key(vec![0, 1, 4]), // TODO: explain why this is a key.
        id: GlobalId::System(2003),
        index_id: GlobalId::System(2004),
    };
    pub static ref MZ_CATALOG_NAMES: BuiltinTable = BuiltinTable {
        name: "mz_catalog_names",
        desc: RelationDesc::empty()
            .with_column("global_id", ScalarType::String.nullable(false))
            .with_column("name", ScalarType::String.nullable(false))
            .with_key(vec![0]),
        id: GlobalId::System(2005),
        index_id: GlobalId::System(2006),
    };
    pub static ref MZ_KAFKA_SINKS: BuiltinTable = BuiltinTable {
        name: "mz_kafka_sinks",
        desc: RelationDesc::empty()
            .with_column("global_id", ScalarType::String.nullable(false))
            .with_column("topic", ScalarType::String.nullable(false))
            .with_key(vec![0]),
        id: GlobalId::System(2007),
        index_id: GlobalId::System(2008),
    };
    pub static ref MZ_AVRO_OCF_SINKS: BuiltinTable = BuiltinTable {
        name: "mz_avro_ocf_sinks",
        desc: RelationDesc::empty()
            .with_column("global_id", ScalarType::String.nullable(false))
            .with_column("path", ScalarType::Bytes.nullable(false))
            .with_key(vec![0]),
        id: GlobalId::System(2009),
        index_id: GlobalId::System(2010),
    };
    pub static ref MZ_DATABASES: BuiltinTable = BuiltinTable {
        name: "mz_databases",
        desc: RelationDesc::empty()
            .with_column("id", ScalarType::Int64.nullable(false))
            .with_column("database", ScalarType::String.nullable(false)),
        id: GlobalId::System(2011),
        index_id: GlobalId::System(2012),
    };
    pub static ref MZ_SCHEMAS: BuiltinTable = BuiltinTable {
        name: "mz_schemas",
        desc: RelationDesc::empty()
            .with_column("database_id", ScalarType::Int64.nullable(false))
            .with_column("schema_id", ScalarType::Int64.nullable(false))
            .with_column("schema", ScalarType::String.nullable(false))
            .with_column("type", ScalarType::String.nullable(false)),
        id: GlobalId::System(2013),
        index_id: GlobalId::System(2014),
    };
    pub static ref MZ_COLUMNS: BuiltinTable = BuiltinTable {
        name: "mz_columns",
        desc: RelationDesc::empty()
            .with_column("global_id", ScalarType::String.nullable(false))
            .with_column("field_number", ScalarType::Int64.nullable(false))
            .with_column("field", ScalarType::String.nullable(false))
            .with_column("nullable", ScalarType::Bool.nullable(false))
            .with_column("type", ScalarType::String.nullable(false)),
        id: GlobalId::System(2015),
        index_id: GlobalId::System(2016),
    };
}

pub const MZ_ADDRESSES_WITH_UNIT_LENGTHS: BuiltinView = BuiltinView {
    name: "mz_addresses_with_unit_length",
    sql: "CREATE VIEW mz_addresses_with_unit_length AS SELECT
    mz_dataflow_operator_addresses.id,
    mz_dataflow_operator_addresses.worker
FROM
    mz_catalog.mz_dataflow_operator_addresses
GROUP BY
    mz_dataflow_operator_addresses.id,
    mz_dataflow_operator_addresses.worker
HAVING count(*) = 1",
    id: GlobalId::System(3000),
};

pub const MZ_DATAFLOW_NAMES: BuiltinView = BuiltinView {
    name: "mz_dataflow_names",
    sql: "CREATE VIEW mz_dataflow_names AS SELECT
    mz_dataflow_operator_addresses.id,
    mz_dataflow_operator_addresses.worker,
    mz_dataflow_operator_addresses.value as local_id,
    mz_dataflow_operators.name
FROM
    mz_catalog.mz_dataflow_operator_addresses,
    mz_catalog.mz_dataflow_operators,
    mz_catalog.mz_addresses_with_unit_length
WHERE
    mz_dataflow_operator_addresses.id = mz_dataflow_operators.id AND
    mz_dataflow_operator_addresses.worker = mz_dataflow_operators.worker AND
    mz_dataflow_operator_addresses.id = mz_addresses_with_unit_length.id AND
    mz_dataflow_operator_addresses.worker = mz_addresses_with_unit_length.worker AND
    mz_dataflow_operator_addresses.slot = 0",
    id: GlobalId::System(3001),
};

pub const MZ_DATAFLOW_OPERATOR_DATAFLOWS: BuiltinView = BuiltinView {
    name: "mz_dataflow_operator_dataflows",
    sql: "CREATE VIEW mz_dataflow_operator_dataflows AS SELECT
    mz_dataflow_operators.id,
    mz_dataflow_operators.name,
    mz_dataflow_operators.worker,
    mz_dataflow_names.id as dataflow_id,
    mz_dataflow_names.name as dataflow_name
FROM
    mz_catalog.mz_dataflow_operators,
    mz_catalog.mz_dataflow_operator_addresses,
    mz_catalog.mz_dataflow_names
WHERE
    mz_dataflow_operators.id = mz_dataflow_operator_addresses.id AND
    mz_dataflow_operators.worker = mz_dataflow_operator_addresses.worker AND
    mz_dataflow_operator_addresses.slot = 0 AND
    mz_dataflow_names.local_id = mz_dataflow_operator_addresses.value AND
    mz_dataflow_names.worker = mz_dataflow_operator_addresses.worker",
    id: GlobalId::System(3002),
};

pub const MZ_MATERIALIZATION_FRONTIERS: BuiltinView = BuiltinView {
    name: "mz_materialization_frontiers",
    sql: "CREATE VIEW mz_materialization_frontiers AS SELECT
    global_id, min(time) AS time
FROM mz_catalog.mz_worker_materialization_frontiers
GROUP BY global_id",
    id: GlobalId::System(3003),
};

pub const MZ_RECORDS_PER_DATAFLOW_OPERATOR: BuiltinView = BuiltinView {
    name: "mz_records_per_dataflow_operator",
    sql: "CREATE VIEW mz_records_per_dataflow_operator AS SELECT
    mz_dataflow_operator_dataflows.id,
    mz_dataflow_operator_dataflows.name,
    mz_dataflow_operator_dataflows.worker,
    mz_dataflow_operator_dataflows.dataflow_id,
    mz_arrangement_sizes.records
FROM
    mz_catalog.mz_arrangement_sizes,
    mz_catalog.mz_dataflow_operator_dataflows
WHERE
    mz_dataflow_operator_dataflows.id = mz_arrangement_sizes.operator AND
    mz_dataflow_operator_dataflows.worker = mz_arrangement_sizes.worker",
    id: GlobalId::System(3004),
};

pub const MZ_RECORDS_PER_DATAFLOW: BuiltinView = BuiltinView {
    name: "mz_records_per_dataflow",
    sql: "CREATE VIEW mz_records_per_dataflow AS SELECT
    mz_records_per_dataflow_operator.dataflow_id as id,
    mz_dataflow_names.name,
    mz_records_per_dataflow_operator.worker,
    SUM(mz_records_per_dataflow_operator.records) as records
FROM
    mz_catalog.mz_records_per_dataflow_operator,
    mz_catalog.mz_dataflow_names
WHERE
    mz_records_per_dataflow_operator.dataflow_id = mz_dataflow_names.id AND
    mz_records_per_dataflow_operator.worker = mz_dataflow_names.worker
GROUP BY
    mz_records_per_dataflow_operator.dataflow_id,
    mz_dataflow_names.name,
    mz_records_per_dataflow_operator.worker",
    id: GlobalId::System(3005),
};

pub const MZ_RECORDS_PER_DATAFLOW_GLOBAL: BuiltinView = BuiltinView {
    name: "mz_records_per_dataflow_global",
    sql: "CREATE VIEW mz_records_per_dataflow_global AS SELECT
    mz_records_per_dataflow.id,
    mz_records_per_dataflow.name,
    SUM(mz_records_per_dataflow.records) as records
FROM
    mz_catalog.mz_records_per_dataflow
GROUP BY
    mz_records_per_dataflow.id,
    mz_records_per_dataflow.name",
    id: GlobalId::System(3006),
};

pub const MZ_PERF_ARRANGEMENT_RECORDS: BuiltinView = BuiltinView {
    name: "mz_perf_arrangement_records",
    sql:
        "CREATE VIEW mz_perf_arrangement_records AS SELECT mas.worker, name, records, operator
FROM mz_catalog.mz_arrangement_sizes mas
LEFT JOIN mz_catalog.mz_dataflow_operators mdo ON mdo.id = mas.operator AND mdo.worker = mas.worker",
    id: GlobalId::System(3007),
};

pub const MZ_PERF_PEEK_DURATIONS_CORE: BuiltinView = BuiltinView {
    name: "mz_perf_peek_durations_core",
    sql: "CREATE VIEW mz_perf_peek_durations_core AS SELECT
    d_upper.worker,
    CAST(d_upper.duration_ns AS TEXT) AS le,
    sum(d_summed.count) AS count
FROM
    mz_catalog.mz_peek_durations AS d_upper,
    mz_catalog.mz_peek_durations AS d_summed
WHERE
    d_upper.worker = d_summed.worker AND
    d_upper.duration_ns >= d_summed.duration_ns
GROUP BY d_upper.worker, d_upper.duration_ns",
    id: GlobalId::System(3008),
};

pub const MZ_PERF_PEEK_DURATIONS_BUCKET: BuiltinView = BuiltinView {
    name: "mz_perf_peek_durations_bucket",
    sql: "CREATE VIEW mz_perf_peek_durations_bucket AS
(
    SELECT * FROM mz_catalog.mz_perf_peek_durations_core
) UNION (
    SELECT worker, '+Inf', max(count) AS count FROM mz_catalog.mz_perf_peek_durations_core
    GROUP BY worker
)",
    id: GlobalId::System(3009),
};

pub const MZ_PERF_PEEK_DURATIONS_AGGREGATES: BuiltinView = BuiltinView {
    name: "mz_perf_peek_durations_aggregates",
    sql: "CREATE VIEW mz_perf_peek_durations_aggregates AS SELECT worker, sum(duration_ns * count) AS sum, sum(count) AS count
FROM mz_catalog.mz_peek_durations lpd
GROUP BY worker",
    id: GlobalId::System(3010),
};

pub const MZ_PERF_DEPENDENCY_FRONTIERS: BuiltinView = BuiltinView {
    name: "mz_perf_dependency_frontiers",
    sql: "CREATE VIEW mz_perf_dependency_frontiers AS SELECT DISTINCT
    mcn.name AS dataflow,
    mcn_source.name AS source,
    frontier_source.time - frontier_df.time as lag_ms
FROM mz_catalog.mz_materialization_dependencies index_deps
JOIN mz_catalog.mz_materialization_frontiers frontier_source ON index_deps.source = frontier_source.global_id
JOIN mz_catalog.mz_materialization_frontiers frontier_df ON index_deps.dataflow = frontier_df.global_id
JOIN mz_catalog.mz_catalog_names mcn ON mcn.global_id = index_deps.dataflow
JOIN mz_catalog.mz_catalog_names mcn_source ON mcn_source.global_id = frontier_source.global_id",
    id: GlobalId::System(3011),
};

lazy_static! {
    pub static ref BUILTINS: BTreeMap<GlobalId, Builtin> = {
        let builtins = vec![
            Builtin::Log(&MZ_DATAFLOW_OPERATORS),
            Builtin::Log(&MZ_DATAFLOW_OPERATORS_ADDRESSES),
            Builtin::Log(&MZ_DATAFLOW_CHANNELS),
            Builtin::Log(&MZ_SCHEDULING_ELAPSED),
            Builtin::Log(&MZ_SCHEDULING_HISTOGRAM),
            Builtin::Log(&MZ_SCHEDULING_PARKS),
            Builtin::Log(&MZ_ARRANGEMENT_SIZES),
            Builtin::Log(&MZ_ARRANGEMENT_SHARING),
            Builtin::Log(&MZ_MATERIALIZATIONS),
            Builtin::Log(&MZ_MATERIALIZATION_DEPENDENCIES),
            Builtin::Log(&MZ_WORKER_MATERIALIZATION_FRONTIERS),
            Builtin::Log(&MZ_PEEK_ACTIVE),
            Builtin::Log(&MZ_PEEK_DURATIONS),
            Builtin::Table(&MZ_VIEW_KEYS),
            Builtin::Table(&MZ_VIEW_FOREIGN_KEYS),
            Builtin::Table(&MZ_CATALOG_NAMES),
            Builtin::Table(&MZ_KAFKA_SINKS),
            Builtin::Table(&MZ_AVRO_OCF_SINKS),
            Builtin::Table(&MZ_DATABASES),
            Builtin::Table(&MZ_SCHEMAS),
            Builtin::Table(&MZ_COLUMNS),
            Builtin::View(&MZ_ADDRESSES_WITH_UNIT_LENGTHS),
            Builtin::View(&MZ_DATAFLOW_NAMES),
            Builtin::View(&MZ_DATAFLOW_OPERATOR_DATAFLOWS),
            Builtin::View(&MZ_RECORDS_PER_DATAFLOW_OPERATOR),
            Builtin::View(&MZ_RECORDS_PER_DATAFLOW),
            Builtin::View(&MZ_RECORDS_PER_DATAFLOW_GLOBAL),
            Builtin::View(&MZ_PERF_ARRANGEMENT_RECORDS),
            Builtin::View(&MZ_PERF_PEEK_DURATIONS_CORE),
            Builtin::View(&MZ_PERF_PEEK_DURATIONS_BUCKET),
            Builtin::View(&MZ_PERF_PEEK_DURATIONS_AGGREGATES),
            Builtin::View(&MZ_MATERIALIZATION_FRONTIERS),
            Builtin::View(&MZ_PERF_DEPENDENCY_FRONTIERS),
        ];
        builtins.into_iter().map(|b| (b.id(), b)).collect()
    };
}

impl BUILTINS {
    pub fn logs(&self) -> impl Iterator<Item = &'static BuiltinLog> + '_ {
        self.values().filter_map(|b| match b {
            Builtin::Log(log) => Some(*log),
            _ => None,
        })
    }
}
