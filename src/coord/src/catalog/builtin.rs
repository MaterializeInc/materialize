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
//! https://materialize.com/docs/sql/system-tables/.

use std::collections::{BTreeMap, BTreeSet};

use lazy_static::lazy_static;
use postgres_types::{Kind, Type};

use dataflow_types::logging::{DifferentialLog, LogVariant, MaterializedLog, TimelyLog};
use expr::GlobalId;
use repr::{RelationDesc, ScalarType};

pub const MZ_TEMP_SCHEMA: &str = "mz_temp";
pub const MZ_CATALOG_SCHEMA: &str = "mz_catalog";
pub const PG_CATALOG_SCHEMA: &str = "pg_catalog";
pub const MZ_INTERNAL_SCHEMA: &str = "mz_internal";

pub enum Builtin {
    Log(&'static BuiltinLog),
    Table(&'static BuiltinTable),
    View(&'static BuiltinView),
    Type(&'static BuiltinType),
    Func(BuiltinFunc),
}

impl Builtin {
    pub fn name(&self) -> &'static str {
        match self {
            Builtin::Log(log) => log.name,
            Builtin::Table(table) => table.name,
            Builtin::View(view) => view.name,
            Builtin::Type(typ) => typ.name(),
            Builtin::Func(func) => func.name,
        }
    }

    pub fn schema(&self) -> &'static str {
        match self {
            Builtin::Log(log) => log.schema,
            Builtin::Table(table) => table.schema,
            Builtin::View(view) => view.schema,
            Builtin::Type(typ) => typ.schema,
            Builtin::Func(func) => func.schema,
        }
    }

    pub fn id(&self) -> GlobalId {
        match self {
            Builtin::Log(log) => log.id,
            Builtin::Table(table) => table.id,
            Builtin::View(view) => view.id,
            Builtin::Type(typ) => typ.id,
            Builtin::Func(func) => func.id,
        }
    }
}

pub struct BuiltinLog {
    pub variant: LogVariant,
    pub name: &'static str,
    pub schema: &'static str,
    pub id: GlobalId,
    pub index_id: GlobalId,
}

pub struct BuiltinTable {
    pub name: &'static str,
    pub schema: &'static str,
    pub desc: RelationDesc,
    pub id: GlobalId,
    pub index_id: GlobalId,
}

pub struct BuiltinView {
    pub name: &'static str,
    pub schema: &'static str,
    pub sql: &'static str,
    pub id: GlobalId,
    // TODO(benesch): auto-derive the needs_logs property.
    pub needs_logs: bool,
}

pub struct BuiltinType {
    pub schema: &'static str,
    pub id: GlobalId,
    pgtype: &'static Type,
}

impl BuiltinType {
    pub fn name(&self) -> &str {
        self.pgtype.name()
    }

    pub fn oid(&self) -> u32 {
        self.pgtype.oid()
    }

    pub fn kind(&self) -> &Kind {
        self.pgtype.kind()
    }
}

pub struct BuiltinFunc {
    pub schema: &'static str,
    pub name: &'static str,
    pub id: GlobalId,
    pub inner: &'static sql::func::Func,
}

pub struct BuiltinRole {
    pub name: &'static str,
    pub id: i64,
}

// Builtin definitions below. Keep these sorted by global ID, and ensure you
// add new builtins to the `BUILTINS` map.
//
// Builtins are loaded in ID order, so sorting them by global ID makes the
// source code definition order match the load order.
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
// | Types     | 1000-1999 |
// | Funcs     | 2000-2999 |
// | Logs      | 3000-3999 |
// | Tables    | 4000-4999 |
// | Views     | 5000-5999 |
//
// WARNING: if you change the definition of an existing builtin item, you must
// be careful to maintain backwards compatibility! Adding new columns is safe.
// Removing a column, changing the name of a column, or changing the type of a
// column is not safe, as persisted user views may depend upon that column.
// The following types are the list of builtin data types available
// in Materialize. This list is derived from the Type variants supported
// in pgrepr.
//
// Builtin types cannot be created, updated, or deleted. Their OIDs
// are static, unlike other objects, to match the type OIDs defined by Postgres.
pub const TYPE_BOOL: BuiltinType = BuiltinType {
    schema: PG_CATALOG_SCHEMA,
    id: GlobalId::System(1000),
    pgtype: &postgres_types::Type::BOOL,
};

pub const TYPE_BYTEA: BuiltinType = BuiltinType {
    schema: PG_CATALOG_SCHEMA,
    id: GlobalId::System(1001),
    pgtype: &postgres_types::Type::BYTEA,
};

pub const TYPE_INT8: BuiltinType = BuiltinType {
    schema: PG_CATALOG_SCHEMA,
    id: GlobalId::System(1002),
    pgtype: &postgres_types::Type::INT8,
};

pub const TYPE_INT4: BuiltinType = BuiltinType {
    schema: PG_CATALOG_SCHEMA,
    id: GlobalId::System(1003),
    pgtype: &postgres_types::Type::INT4,
};

pub const TYPE_TEXT: BuiltinType = BuiltinType {
    schema: PG_CATALOG_SCHEMA,
    id: GlobalId::System(1004),
    pgtype: &postgres_types::Type::TEXT,
};

pub const TYPE_OID: BuiltinType = BuiltinType {
    schema: PG_CATALOG_SCHEMA,
    id: GlobalId::System(1005),
    pgtype: &postgres_types::Type::OID,
};

pub const TYPE_FLOAT4: BuiltinType = BuiltinType {
    schema: PG_CATALOG_SCHEMA,
    id: GlobalId::System(1006),
    pgtype: &postgres_types::Type::FLOAT4,
};

pub const TYPE_FLOAT8: BuiltinType = BuiltinType {
    schema: PG_CATALOG_SCHEMA,
    id: GlobalId::System(1007),
    pgtype: &postgres_types::Type::FLOAT8,
};

pub const TYPE_BOOL_ARRAY: BuiltinType = BuiltinType {
    schema: PG_CATALOG_SCHEMA,
    id: GlobalId::System(1008),
    pgtype: &postgres_types::Type::BOOL_ARRAY,
};

pub const TYPE_BYTEA_ARRAY: BuiltinType = BuiltinType {
    schema: PG_CATALOG_SCHEMA,
    id: GlobalId::System(1009),
    pgtype: &postgres_types::Type::BYTEA_ARRAY,
};

pub const TYPE_INT4_ARRAY: BuiltinType = BuiltinType {
    schema: PG_CATALOG_SCHEMA,
    id: GlobalId::System(1010),
    pgtype: &postgres_types::Type::INT4_ARRAY,
};

pub const TYPE_TEXT_ARRAY: BuiltinType = BuiltinType {
    schema: PG_CATALOG_SCHEMA,
    id: GlobalId::System(1011),
    pgtype: &postgres_types::Type::TEXT_ARRAY,
};

pub const TYPE_INT8_ARRAY: BuiltinType = BuiltinType {
    schema: PG_CATALOG_SCHEMA,
    id: GlobalId::System(1012),
    pgtype: &postgres_types::Type::INT8_ARRAY,
};

pub const TYPE_FLOAT4_ARRAY: BuiltinType = BuiltinType {
    schema: PG_CATALOG_SCHEMA,
    id: GlobalId::System(1013),
    pgtype: &postgres_types::Type::FLOAT4_ARRAY,
};

pub const TYPE_FLOAT8_ARRAY: BuiltinType = BuiltinType {
    schema: PG_CATALOG_SCHEMA,
    id: GlobalId::System(1014),
    pgtype: &postgres_types::Type::FLOAT8_ARRAY,
};

pub const TYPE_OID_ARRAY: BuiltinType = BuiltinType {
    schema: PG_CATALOG_SCHEMA,
    id: GlobalId::System(1015),
    pgtype: &postgres_types::Type::OID_ARRAY,
};

pub const TYPE_DATE: BuiltinType = BuiltinType {
    schema: PG_CATALOG_SCHEMA,
    id: GlobalId::System(1016),
    pgtype: &postgres_types::Type::DATE,
};

pub const TYPE_TIME: BuiltinType = BuiltinType {
    schema: PG_CATALOG_SCHEMA,
    id: GlobalId::System(1017),
    pgtype: &postgres_types::Type::TIME,
};

pub const TYPE_TIMESTAMP: BuiltinType = BuiltinType {
    schema: PG_CATALOG_SCHEMA,
    id: GlobalId::System(1018),
    pgtype: &postgres_types::Type::TIMESTAMP,
};

pub const TYPE_TIMESTAMP_ARRAY: BuiltinType = BuiltinType {
    schema: PG_CATALOG_SCHEMA,
    id: GlobalId::System(1019),
    pgtype: &postgres_types::Type::TIMESTAMP_ARRAY,
};

pub const TYPE_DATE_ARRAY: BuiltinType = BuiltinType {
    schema: PG_CATALOG_SCHEMA,
    id: GlobalId::System(1020),
    pgtype: &postgres_types::Type::DATE_ARRAY,
};

pub const TYPE_TIME_ARRAY: BuiltinType = BuiltinType {
    schema: PG_CATALOG_SCHEMA,
    id: GlobalId::System(1021),
    pgtype: &postgres_types::Type::TIME_ARRAY,
};

pub const TYPE_TIMESTAMPTZ: BuiltinType = BuiltinType {
    schema: PG_CATALOG_SCHEMA,
    id: GlobalId::System(1022),
    pgtype: &postgres_types::Type::TIMESTAMPTZ,
};

pub const TYPE_TIMESTAMPTZ_ARRAY: BuiltinType = BuiltinType {
    schema: PG_CATALOG_SCHEMA,
    id: GlobalId::System(1023),
    pgtype: &postgres_types::Type::TIMESTAMPTZ_ARRAY,
};

pub const TYPE_INTERVAL: BuiltinType = BuiltinType {
    schema: PG_CATALOG_SCHEMA,
    id: GlobalId::System(1024),
    pgtype: &postgres_types::Type::INTERVAL,
};

pub const TYPE_INTERVAL_ARRAY: BuiltinType = BuiltinType {
    schema: PG_CATALOG_SCHEMA,
    id: GlobalId::System(1025),
    pgtype: &postgres_types::Type::INTERVAL_ARRAY,
};

pub const TYPE_NUMERIC: BuiltinType = BuiltinType {
    schema: PG_CATALOG_SCHEMA,
    id: GlobalId::System(1026),
    pgtype: &postgres_types::Type::NUMERIC,
};

pub const TYPE_NUMERIC_ARRAY: BuiltinType = BuiltinType {
    schema: PG_CATALOG_SCHEMA,
    id: GlobalId::System(1027),
    pgtype: &postgres_types::Type::NUMERIC_ARRAY,
};

pub const TYPE_RECORD: BuiltinType = BuiltinType {
    schema: PG_CATALOG_SCHEMA,
    id: GlobalId::System(1028),
    pgtype: &postgres_types::Type::RECORD,
};

pub const TYPE_RECORD_ARRAY: BuiltinType = BuiltinType {
    schema: PG_CATALOG_SCHEMA,
    id: GlobalId::System(1029),
    pgtype: &postgres_types::Type::RECORD_ARRAY,
};

pub const TYPE_UUID: BuiltinType = BuiltinType {
    schema: PG_CATALOG_SCHEMA,
    id: GlobalId::System(1030),
    pgtype: &postgres_types::Type::UUID,
};

pub const TYPE_UUID_ARRAY: BuiltinType = BuiltinType {
    schema: PG_CATALOG_SCHEMA,
    id: GlobalId::System(1031),
    pgtype: &postgres_types::Type::UUID_ARRAY,
};

pub const TYPE_JSONB: BuiltinType = BuiltinType {
    schema: PG_CATALOG_SCHEMA,
    id: GlobalId::System(1032),
    pgtype: &postgres_types::Type::JSONB,
};

pub const TYPE_JSONB_ARRAY: BuiltinType = BuiltinType {
    schema: PG_CATALOG_SCHEMA,
    id: GlobalId::System(1033),
    pgtype: &postgres_types::Type::JSONB_ARRAY,
};

pub const TYPE_ANY: BuiltinType = BuiltinType {
    schema: PG_CATALOG_SCHEMA,
    id: GlobalId::System(1034),
    pgtype: &postgres_types::Type::ANY,
};

pub const TYPE_ANYARRAY: BuiltinType = BuiltinType {
    schema: PG_CATALOG_SCHEMA,
    id: GlobalId::System(1035),
    pgtype: &postgres_types::Type::ANYARRAY,
};

pub const TYPE_ANYELEMENT: BuiltinType = BuiltinType {
    schema: PG_CATALOG_SCHEMA,
    id: GlobalId::System(1036),
    pgtype: &postgres_types::Type::ANYELEMENT,
};

pub const TYPE_ANYNONARRAY: BuiltinType = BuiltinType {
    schema: PG_CATALOG_SCHEMA,
    id: GlobalId::System(1037),
    pgtype: &postgres_types::Type::ANYNONARRAY,
};

pub const TYPE_CHAR: BuiltinType = BuiltinType {
    schema: PG_CATALOG_SCHEMA,
    id: GlobalId::System(1038),
    pgtype: &postgres_types::Type::CHAR,
};

pub const TYPE_VARCHAR: BuiltinType = BuiltinType {
    schema: PG_CATALOG_SCHEMA,
    id: GlobalId::System(1039),
    pgtype: &postgres_types::Type::VARCHAR,
};

pub const TYPE_INT2: BuiltinType = BuiltinType {
    schema: PG_CATALOG_SCHEMA,
    id: GlobalId::System(1040),
    pgtype: &postgres_types::Type::INT2,
};

pub const TYPE_INT2_ARRAY: BuiltinType = BuiltinType {
    schema: PG_CATALOG_SCHEMA,
    id: GlobalId::System(1041),
    pgtype: &postgres_types::Type::INT2_ARRAY,
};

lazy_static! {
    pub static ref TYPE_RDN: BuiltinType = BuiltinType {
        schema: PG_CATALOG_SCHEMA,
        id: GlobalId::System(1997),
        pgtype: &pgrepr::RDNType,
    };
    pub static ref TYPE_LIST: BuiltinType = BuiltinType {
        schema: PG_CATALOG_SCHEMA,
        id: GlobalId::System(1998),
        pgtype: &pgrepr::LIST,
    };
    pub static ref TYPE_MAP: BuiltinType = BuiltinType {
        schema: PG_CATALOG_SCHEMA,
        id: GlobalId::System(1999),
        pgtype: &pgrepr::MAP,
    };
}

pub const MZ_DATAFLOW_OPERATORS: BuiltinLog = BuiltinLog {
    name: "mz_dataflow_operators",
    schema: MZ_CATALOG_SCHEMA,
    variant: LogVariant::Timely(TimelyLog::Operates),
    id: GlobalId::System(3000),
    index_id: GlobalId::System(3001),
};

pub const MZ_DATAFLOW_OPERATORS_ADDRESSES: BuiltinLog = BuiltinLog {
    name: "mz_dataflow_operator_addresses",
    schema: MZ_CATALOG_SCHEMA,
    variant: LogVariant::Timely(TimelyLog::Addresses),
    id: GlobalId::System(3002),
    index_id: GlobalId::System(3003),
};

pub const MZ_DATAFLOW_CHANNELS: BuiltinLog = BuiltinLog {
    name: "mz_dataflow_channels",
    schema: MZ_CATALOG_SCHEMA,
    variant: LogVariant::Timely(TimelyLog::Channels),
    id: GlobalId::System(3004),
    index_id: GlobalId::System(3005),
};

pub const MZ_SCHEDULING_ELAPSED: BuiltinLog = BuiltinLog {
    name: "mz_scheduling_elapsed",
    schema: MZ_CATALOG_SCHEMA,
    variant: LogVariant::Timely(TimelyLog::Elapsed),
    id: GlobalId::System(3006),
    index_id: GlobalId::System(3007),
};

pub const MZ_SCHEDULING_HISTOGRAM: BuiltinLog = BuiltinLog {
    name: "mz_scheduling_histogram",
    schema: MZ_CATALOG_SCHEMA,
    variant: LogVariant::Timely(TimelyLog::Histogram),
    id: GlobalId::System(3008),
    index_id: GlobalId::System(3009),
};

pub const MZ_SCHEDULING_PARKS: BuiltinLog = BuiltinLog {
    name: "mz_scheduling_parks",
    schema: MZ_CATALOG_SCHEMA,
    variant: LogVariant::Timely(TimelyLog::Parks),
    id: GlobalId::System(3010),
    index_id: GlobalId::System(3011),
};

pub const MZ_ARRANGEMENT_SIZES: BuiltinLog = BuiltinLog {
    name: "mz_arrangement_sizes",
    schema: MZ_CATALOG_SCHEMA,
    variant: LogVariant::Differential(DifferentialLog::Arrangement),
    id: GlobalId::System(3012),
    index_id: GlobalId::System(3013),
};

pub const MZ_ARRANGEMENT_SHARING: BuiltinLog = BuiltinLog {
    name: "mz_arrangement_sharing",
    schema: MZ_CATALOG_SCHEMA,
    variant: LogVariant::Differential(DifferentialLog::Sharing),
    id: GlobalId::System(3014),
    index_id: GlobalId::System(3015),
};

pub const MZ_MATERIALIZATIONS: BuiltinLog = BuiltinLog {
    name: "mz_materializations",
    schema: MZ_CATALOG_SCHEMA,
    variant: LogVariant::Materialized(MaterializedLog::DataflowCurrent),
    id: GlobalId::System(3016),
    index_id: GlobalId::System(3017),
};

pub const MZ_MATERIALIZATION_DEPENDENCIES: BuiltinLog = BuiltinLog {
    name: "mz_materialization_dependencies",
    schema: MZ_CATALOG_SCHEMA,
    variant: LogVariant::Materialized(MaterializedLog::DataflowDependency),
    id: GlobalId::System(3018),
    index_id: GlobalId::System(3019),
};

pub const MZ_WORKER_MATERIALIZATION_FRONTIERS: BuiltinLog = BuiltinLog {
    name: "mz_worker_materialization_frontiers",
    schema: MZ_CATALOG_SCHEMA,
    variant: LogVariant::Materialized(MaterializedLog::FrontierCurrent),
    id: GlobalId::System(3020),
    index_id: GlobalId::System(3021),
};

pub const MZ_PEEK_ACTIVE: BuiltinLog = BuiltinLog {
    name: "mz_peek_active",
    schema: MZ_CATALOG_SCHEMA,
    variant: LogVariant::Materialized(MaterializedLog::PeekCurrent),
    id: GlobalId::System(3022),
    index_id: GlobalId::System(3023),
};

pub const MZ_PEEK_DURATIONS: BuiltinLog = BuiltinLog {
    name: "mz_peek_durations",
    schema: MZ_CATALOG_SCHEMA,
    variant: LogVariant::Materialized(MaterializedLog::PeekDuration),
    id: GlobalId::System(3024),
    index_id: GlobalId::System(3025),
};

pub const MZ_SOURCE_INFO: BuiltinLog = BuiltinLog {
    name: "mz_source_info",
    schema: MZ_CATALOG_SCHEMA,
    variant: LogVariant::Materialized(MaterializedLog::SourceInfo),
    id: GlobalId::System(3026),
    index_id: GlobalId::System(3027),
};

pub const MZ_MESSAGE_COUNTS: BuiltinLog = BuiltinLog {
    name: "mz_message_counts",
    schema: MZ_CATALOG_SCHEMA,
    variant: LogVariant::Timely(TimelyLog::Messages),
    id: GlobalId::System(3028),
    index_id: GlobalId::System(3029),
};

pub const MZ_KAFKA_CONSUMER_PARTITIONS: BuiltinLog = BuiltinLog {
    name: "mz_kafka_consumer_partitions",
    schema: MZ_CATALOG_SCHEMA,
    variant: LogVariant::Materialized(MaterializedLog::KafkaConsumerInfo),
    id: GlobalId::System(3030),
    index_id: GlobalId::System(3031),
};

pub const MZ_KAFKA_BROKER_RTT: BuiltinLog = BuiltinLog {
    name: "mz_kafka_broker_rtt",
    schema: MZ_CATALOG_SCHEMA,
    variant: LogVariant::Materialized(MaterializedLog::KafkaBrokerRtt),
    id: GlobalId::System(3032),
    index_id: GlobalId::System(3033),
};

lazy_static! {
    pub static ref MZ_VIEW_KEYS: BuiltinTable = BuiltinTable {
        name: "mz_view_keys",
        schema: MZ_CATALOG_SCHEMA,
        desc: RelationDesc::empty()
            .with_column("global_id", ScalarType::String.nullable(false))
            .with_column("column", ScalarType::Int64.nullable(false))
            .with_column("key_group", ScalarType::Int64.nullable(false)),
        id: GlobalId::System(4001),
        index_id: GlobalId::System(4002),
    };
    pub static ref MZ_VIEW_FOREIGN_KEYS: BuiltinTable = BuiltinTable {
        name: "mz_view_foreign_keys",
        schema: MZ_CATALOG_SCHEMA,
        desc: RelationDesc::empty()
            .with_column("child_id", ScalarType::String.nullable(false))
            .with_column("child_column", ScalarType::Int64.nullable(false))
            .with_column("parent_id", ScalarType::String.nullable(false))
            .with_column("parent_column", ScalarType::Int64.nullable(false))
            .with_column("key_group", ScalarType::Int64.nullable(false))
            .with_key(vec![0, 1, 4]), // TODO: explain why this is a key.
        id: GlobalId::System(4003),
        index_id: GlobalId::System(4004),
    };
    pub static ref MZ_KAFKA_SINKS: BuiltinTable = BuiltinTable {
        name: "mz_kafka_sinks",
        schema: MZ_CATALOG_SCHEMA,
        desc: RelationDesc::empty()
            .with_column("sink_id", ScalarType::String.nullable(false))
            .with_column("topic", ScalarType::String.nullable(false))
            .with_key(vec![0]),
        id: GlobalId::System(4005),
        index_id: GlobalId::System(4006),
    };
    pub static ref MZ_AVRO_OCF_SINKS: BuiltinTable = BuiltinTable {
        name: "mz_avro_ocf_sinks",
        schema: MZ_CATALOG_SCHEMA,
        desc: RelationDesc::empty()
            .with_column("sink_id", ScalarType::String.nullable(false))
            .with_column("path", ScalarType::Bytes.nullable(false))
            .with_key(vec![0]),
        id: GlobalId::System(4007),
        index_id: GlobalId::System(4008),
    };
    pub static ref MZ_DATABASES: BuiltinTable = BuiltinTable {
        name: "mz_databases",
        schema: MZ_CATALOG_SCHEMA,
        desc: RelationDesc::empty()
            .with_column("id", ScalarType::Int64.nullable(false))
            .with_column("oid", ScalarType::Oid.nullable(false))
            .with_column("name", ScalarType::String.nullable(false)),
        id: GlobalId::System(4009),
        index_id: GlobalId::System(4010),
    };
    pub static ref MZ_SCHEMAS: BuiltinTable = BuiltinTable {
        name: "mz_schemas",
        schema: MZ_CATALOG_SCHEMA,
        desc: RelationDesc::empty()
            .with_column("id", ScalarType::Int64.nullable(false))
            .with_column("oid", ScalarType::Oid.nullable(false))
            .with_column("database_id", ScalarType::Int64.nullable(true))
            .with_column("name", ScalarType::String.nullable(false)),
        id: GlobalId::System(4011),
        index_id: GlobalId::System(4012),
    };
    pub static ref MZ_COLUMNS: BuiltinTable = BuiltinTable {
        name: "mz_columns",
        schema: MZ_CATALOG_SCHEMA,
        desc: RelationDesc::empty()
            .with_column("id", ScalarType::String.nullable(false))
            .with_column("name", ScalarType::String.nullable(false))
            .with_column("position", ScalarType::Int64.nullable(false))
            .with_column("nullable", ScalarType::Bool.nullable(false))
            .with_column("type", ScalarType::String.nullable(false)),
        id: GlobalId::System(4013),
        index_id: GlobalId::System(4014),
    };
    pub static ref MZ_INDEXES: BuiltinTable = BuiltinTable {
        name: "mz_indexes",
        schema: MZ_CATALOG_SCHEMA,
        desc: RelationDesc::empty()
            .with_column("id", ScalarType::String.nullable(false))
            .with_column("oid", ScalarType::Oid.nullable(false))
            .with_column("name", ScalarType::String.nullable(false))
            .with_column("on_id", ScalarType::String.nullable(false))
            .with_column("volatility", ScalarType::String.nullable(false)),
        id: GlobalId::System(4015),
        index_id: GlobalId::System(4016),
    };
    pub static ref MZ_INDEX_COLUMNS: BuiltinTable = BuiltinTable {
        name: "mz_index_columns",
        schema: MZ_CATALOG_SCHEMA,
        desc: RelationDesc::empty()
            .with_column("index_id", ScalarType::String.nullable(false))
            .with_column("index_position", ScalarType::Int64.nullable(false))
            .with_column("on_position", ScalarType::Int64.nullable(true))
            .with_column("on_expression", ScalarType::String.nullable(true))
            .with_column("nullable", ScalarType::Bool.nullable(false)),
        id: GlobalId::System(4017),
        index_id: GlobalId::System(4018),
    };
    pub static ref MZ_TABLES: BuiltinTable = BuiltinTable {
        name: "mz_tables",
        schema: MZ_CATALOG_SCHEMA,
        desc: RelationDesc::empty()
            .with_column("id", ScalarType::String.nullable(false))
            .with_column("oid", ScalarType::Oid.nullable(false))
            .with_column("schema_id", ScalarType::Int64.nullable(false))
            .with_column("name", ScalarType::String.nullable(false)),
        id: GlobalId::System(4019),
        index_id: GlobalId::System(4020),
    };
    pub static ref MZ_SOURCES: BuiltinTable = BuiltinTable {
        name: "mz_sources",
        schema: MZ_CATALOG_SCHEMA,
        desc: RelationDesc::empty()
            .with_column("id", ScalarType::String.nullable(false))
            .with_column("oid", ScalarType::Oid.nullable(false))
            .with_column("schema_id", ScalarType::Int64.nullable(false))
            .with_column("name", ScalarType::String.nullable(false))
            .with_column("volatility", ScalarType::String.nullable(false)),
        id: GlobalId::System(4021),
        index_id: GlobalId::System(4022),
    };
    pub static ref MZ_SINKS: BuiltinTable = BuiltinTable {
        name: "mz_sinks",
        schema: MZ_CATALOG_SCHEMA,
        desc: RelationDesc::empty()
            .with_column("id", ScalarType::String.nullable(false))
            .with_column("oid", ScalarType::Oid.nullable(false))
            .with_column("schema_id", ScalarType::Int64.nullable(false))
            .with_column("name", ScalarType::String.nullable(false))
            .with_column("volatility", ScalarType::String.nullable(false)),
        id: GlobalId::System(4023),
        index_id: GlobalId::System(4024),
    };
    pub static ref MZ_VIEWS: BuiltinTable = BuiltinTable {
        name: "mz_views",
        schema: MZ_CATALOG_SCHEMA,
        desc: RelationDesc::empty()
            .with_column("id", ScalarType::String.nullable(false))
            .with_column("oid", ScalarType::Oid.nullable(false))
            .with_column("schema_id", ScalarType::Int64.nullable(false))
            .with_column("name", ScalarType::String.nullable(false))
            .with_column("volatility", ScalarType::String.nullable(false)),
        id: GlobalId::System(4025),
        index_id: GlobalId::System(4026),
    };
    pub static ref MZ_TYPES: BuiltinTable = BuiltinTable {
        name: "mz_types",
        schema: MZ_CATALOG_SCHEMA,
        desc: RelationDesc::empty()
            .with_column("id", ScalarType::String.nullable(false))
            .with_column("oid", ScalarType::Oid.nullable(false))
            .with_column("schema_id", ScalarType::Int64.nullable(false))
            .with_column("name", ScalarType::String.nullable(false)),
        id: GlobalId::System(4027),
        index_id: GlobalId::System(4028),
    };
    pub static ref MZ_ARRAY_TYPES: BuiltinTable = BuiltinTable {
        name: "mz_array_types",
        schema: MZ_CATALOG_SCHEMA,
        desc: RelationDesc::empty()
            .with_column("type_id", ScalarType::String.nullable(false))
            .with_column("element_id", ScalarType::String.nullable(false)),
            id: GlobalId::System(4029),
            index_id: GlobalId::System(4030),
    };
    pub static ref MZ_BASE_TYPES: BuiltinTable = BuiltinTable {
        name: "mz_base_types",
        schema: MZ_CATALOG_SCHEMA,
        desc: RelationDesc::empty()
            .with_column("type_id", ScalarType::String.nullable(false)),
            id: GlobalId::System(4031),
            index_id: GlobalId::System(4032),
    };
    pub static ref MZ_LIST_TYPES: BuiltinTable = BuiltinTable {
        name: "mz_list_types",
        schema: MZ_CATALOG_SCHEMA,
        desc: RelationDesc::empty()
            .with_column("type_id", ScalarType::String.nullable(false))
            .with_column("element_id", ScalarType::String.nullable(false)),
            id: GlobalId::System(4033),
            index_id: GlobalId::System(4034),
    };
    pub static ref MZ_MAP_TYPES: BuiltinTable = BuiltinTable {
        name: "mz_map_types",
        schema: MZ_CATALOG_SCHEMA,
        desc: RelationDesc::empty()
            .with_column("type_id", ScalarType::String.nullable(false))
            .with_column("key_id", ScalarType::String.nullable(false))
            .with_column("value_id", ScalarType::String.nullable(false)),
            id: GlobalId::System(4035),
            index_id: GlobalId::System(4036),
    };
    pub static ref MZ_ROLES: BuiltinTable = BuiltinTable {
        name: "mz_roles",
        schema: MZ_CATALOG_SCHEMA,
        desc: RelationDesc::empty()
            .with_column("id", ScalarType::Int64.nullable(false))
            .with_column("oid", ScalarType::Oid.nullable(false))
            .with_column("name", ScalarType::String.nullable(false)),
        id: GlobalId::System(4037),
        index_id: GlobalId::System(4038),
    };
    pub static ref MZ_PSEUDO_TYPES: BuiltinTable = BuiltinTable {
        name: "mz_pseudo_types",
        schema: MZ_CATALOG_SCHEMA,
        desc: RelationDesc::empty()
            .with_column("type_id", ScalarType::String.nullable(false)),
        id: GlobalId::System(4039),
        index_id: GlobalId::System(4040),
    };
    pub static ref MZ_FUNCTIONS: BuiltinTable = BuiltinTable {
        name: "mz_functions",
        schema: MZ_CATALOG_SCHEMA,
        desc: RelationDesc::empty()
            .with_column("id", ScalarType::String.nullable(false))
            .with_column("oid", ScalarType::Oid.nullable(false))
            .with_column("schema_id", ScalarType::Int64.nullable(false))
            .with_column("name", ScalarType::String.nullable(false))
            .with_column("arg_ids", ScalarType::Array(Box::new(ScalarType::String)).nullable(false))
            .with_column("variadic_id", ScalarType::String.nullable(true)),
        id: GlobalId::System(4041),
        index_id: GlobalId::System(4042),
    };
    pub static ref MZ_PROMETHEUS_READINGS: BuiltinTable = BuiltinTable {
        name: "mz_metrics",
        schema: MZ_CATALOG_SCHEMA,
        desc: RelationDesc::empty()
                .with_column("metric", ScalarType::String.nullable(false))
                .with_column("time", ScalarType::Timestamp.nullable(false))
                .with_column("labels", ScalarType::Jsonb.nullable(false))
                .with_column("value", ScalarType::Float64.nullable(false))
                .with_key(vec![0, 1, 2]),
        id: GlobalId::System(4043),
        index_id: GlobalId::System(4044),
    };
    pub static ref MZ_PROMETHEUS_METRICS: BuiltinTable = BuiltinTable {
        name: "mz_metrics_meta",
        schema: MZ_CATALOG_SCHEMA,
        desc: RelationDesc::empty()
                .with_column("metric", ScalarType::String.nullable(false))
                .with_column("type", ScalarType::String.nullable(false))
                .with_column("help", ScalarType::String.nullable(false))
                .with_key(vec![0]),
        id: GlobalId::System(4045),
        index_id: GlobalId::System(4046),
    };
    pub static ref MZ_PROMETHEUS_HISTOGRAMS: BuiltinTable = BuiltinTable {
        name: "mz_metric_histograms",
        schema: MZ_CATALOG_SCHEMA,
        desc: RelationDesc::empty()
                .with_column("metric", ScalarType::String.nullable(false))
                .with_column("time", ScalarType::Timestamp.nullable(false))
                .with_column("labels", ScalarType::Jsonb.nullable(false))
                .with_column("bound", ScalarType::Float64.nullable(false))
                .with_column("count", ScalarType::Int64.nullable(false))
                .with_key(vec![0, 1, 2]),
        id: GlobalId::System(4047),
        index_id: GlobalId::System(4048),
    };
}

pub const MZ_RELATIONS: BuiltinView = BuiltinView {
    name: "mz_relations",
    schema: MZ_CATALOG_SCHEMA,
    sql: "CREATE VIEW mz_relations (id, oid, schema_id, name, type) AS
      SELECT id, oid, schema_id, name, 'table' FROM mz_catalog.mz_tables
UNION SELECT id, oid, schema_id, name, 'source' FROM mz_catalog.mz_sources
UNION SELECT id, oid, schema_id, name, 'view' FROM mz_catalog.mz_views",
    id: GlobalId::System(5000),
    needs_logs: false,
};

pub const MZ_OBJECTS: BuiltinView = BuiltinView {
    name: "mz_objects",
    schema: MZ_CATALOG_SCHEMA,
    sql: "CREATE VIEW mz_objects (id, oid, schema_id, name, type) AS
    SELECT id, oid, schema_id, name, type FROM mz_catalog.mz_relations
UNION
    SELECT id, oid, schema_id, name, 'sink' FROM mz_catalog.mz_sinks
UNION
    SELECT mz_indexes.id, mz_indexes.oid, schema_id, mz_indexes.name, 'index'
    FROM mz_catalog.mz_indexes
    JOIN mz_catalog.mz_relations ON mz_indexes.on_id = mz_relations.id",
    id: GlobalId::System(5001),
    needs_logs: false,
};

// For historical reasons, this view does not properly escape identifiers. For
// example, a table named 'cAp.S' in the default database and schema will be
// rendered as `materialize.public.cAp.S`. This is *not* a valid SQL identifier,
// as it has an extra dot, and the capitals will be folded to lowercase. This
// view is thus only fit for use in debugging queries where the names are for
// human consumption. Applications should instead pull the names of individual
// components out of mz_objects, mz_schemas, and mz_databases to avoid these
// escaping issues.
//
// TODO(benesch): deprecate this view.
pub const MZ_CATALOG_NAMES: BuiltinView = BuiltinView {
    name: "mz_catalog_names",
    schema: MZ_CATALOG_SCHEMA,
    sql: "CREATE VIEW mz_catalog_names AS SELECT
    o.id AS global_id,
    coalesce(d.name || '.', '') || s.name || '.' || o.name AS name
FROM mz_catalog.mz_objects o
JOIN mz_catalog.mz_schemas s ON s.id = o.schema_id
LEFT JOIN mz_catalog.mz_databases d ON d.id = s.database_id",
    id: GlobalId::System(5002),
    needs_logs: false,
};

pub const MZ_ADDRESSES_WITH_UNIT_LENGTHS: BuiltinView = BuiltinView {
    name: "mz_addresses_with_unit_length",
    schema: MZ_CATALOG_SCHEMA,
    sql: "CREATE VIEW mz_addresses_with_unit_length AS SELECT
    mz_dataflow_operator_addresses.id,
    mz_dataflow_operator_addresses.worker
FROM
    mz_catalog.mz_dataflow_operator_addresses
WHERE
    mz_catalog.list_length(mz_dataflow_operator_addresses.address) = 1
GROUP BY
    mz_dataflow_operator_addresses.id,
    mz_dataflow_operator_addresses.worker",
    id: GlobalId::System(5003),
    needs_logs: true,
};

pub const MZ_DATAFLOW_NAMES: BuiltinView = BuiltinView {
    name: "mz_dataflow_names",
    schema: MZ_CATALOG_SCHEMA,
    sql: "CREATE VIEW mz_dataflow_names AS SELECT
    mz_dataflow_operator_addresses.id,
    mz_dataflow_operator_addresses.worker,
    mz_dataflow_operator_addresses.address[1] as local_id,
    mz_dataflow_operators.name
FROM
    mz_catalog.mz_dataflow_operator_addresses,
    mz_catalog.mz_dataflow_operators,
    mz_catalog.mz_addresses_with_unit_length
WHERE
    mz_dataflow_operator_addresses.id = mz_dataflow_operators.id AND
    mz_dataflow_operator_addresses.worker = mz_dataflow_operators.worker AND
    mz_dataflow_operator_addresses.id = mz_addresses_with_unit_length.id AND
    mz_dataflow_operator_addresses.worker = mz_addresses_with_unit_length.worker",
    id: GlobalId::System(5004),
    needs_logs: true,
};

pub const MZ_DATAFLOW_OPERATOR_DATAFLOWS: BuiltinView = BuiltinView {
    name: "mz_dataflow_operator_dataflows",
    schema: MZ_CATALOG_SCHEMA,
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
    mz_dataflow_names.local_id = mz_dataflow_operator_addresses.address[1] AND
    mz_dataflow_names.worker = mz_dataflow_operator_addresses.worker",
    id: GlobalId::System(5005),
    needs_logs: true,
};

pub const MZ_MATERIALIZATION_FRONTIERS: BuiltinView = BuiltinView {
    name: "mz_materialization_frontiers",
    schema: MZ_CATALOG_SCHEMA,
    sql: "CREATE VIEW mz_materialization_frontiers AS SELECT
    global_id, pg_catalog.min(time) AS time
FROM mz_catalog.mz_worker_materialization_frontiers
GROUP BY global_id",
    id: GlobalId::System(5006),
    needs_logs: true,
};

pub const MZ_RECORDS_PER_DATAFLOW_OPERATOR: BuiltinView = BuiltinView {
    name: "mz_records_per_dataflow_operator",
    schema: MZ_CATALOG_SCHEMA,
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
    id: GlobalId::System(5007),
    needs_logs: true,
};

pub const MZ_RECORDS_PER_DATAFLOW: BuiltinView = BuiltinView {
    name: "mz_records_per_dataflow",
    schema: MZ_CATALOG_SCHEMA,
    sql: "CREATE VIEW mz_records_per_dataflow AS SELECT
    mz_records_per_dataflow_operator.dataflow_id as id,
    mz_dataflow_names.name,
    mz_records_per_dataflow_operator.worker,
    pg_catalog.SUM(mz_records_per_dataflow_operator.records) as records
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
    id: GlobalId::System(5008),
    needs_logs: true,
};

pub const MZ_RECORDS_PER_DATAFLOW_GLOBAL: BuiltinView = BuiltinView {
    name: "mz_records_per_dataflow_global",
    schema: MZ_CATALOG_SCHEMA,
    sql: "CREATE VIEW mz_records_per_dataflow_global AS SELECT
    mz_records_per_dataflow.id,
    mz_records_per_dataflow.name,
    pg_catalog.SUM(mz_records_per_dataflow.records) as records
FROM
    mz_catalog.mz_records_per_dataflow
GROUP BY
    mz_records_per_dataflow.id,
    mz_records_per_dataflow.name",
    id: GlobalId::System(5009),
    needs_logs: true,
};

pub const MZ_PERF_ARRANGEMENT_RECORDS: BuiltinView = BuiltinView {
    name: "mz_perf_arrangement_records",
    schema: MZ_CATALOG_SCHEMA,
    sql:
        "CREATE VIEW mz_perf_arrangement_records AS SELECT mas.worker, name, records, operator
FROM mz_catalog.mz_arrangement_sizes mas
LEFT JOIN mz_catalog.mz_dataflow_operators mdo ON mdo.id = mas.operator AND mdo.worker = mas.worker",
    id: GlobalId::System(5010),
    needs_logs: true,
};

pub const MZ_PERF_PEEK_DURATIONS_CORE: BuiltinView = BuiltinView {
    name: "mz_perf_peek_durations_core",
    schema: MZ_CATALOG_SCHEMA,
    sql: "CREATE VIEW mz_perf_peek_durations_core AS SELECT
    d_upper.worker,
    d_upper.duration_ns::pg_catalog.text AS le,
    pg_catalog.sum(d_summed.count) AS count
FROM
    mz_catalog.mz_peek_durations AS d_upper,
    mz_catalog.mz_peek_durations AS d_summed
WHERE
    d_upper.worker = d_summed.worker AND
    d_upper.duration_ns >= d_summed.duration_ns
GROUP BY d_upper.worker, d_upper.duration_ns",
    id: GlobalId::System(5011),
    needs_logs: true,
};

pub const MZ_PERF_PEEK_DURATIONS_BUCKET: BuiltinView = BuiltinView {
    name: "mz_perf_peek_durations_bucket",
    schema: MZ_CATALOG_SCHEMA,
    sql: "CREATE VIEW mz_perf_peek_durations_bucket AS
(
    SELECT * FROM mz_catalog.mz_perf_peek_durations_core
) UNION (
    SELECT worker, '+Inf', pg_catalog.max(count) AS count FROM mz_catalog.mz_perf_peek_durations_core
    GROUP BY worker
)",
    id: GlobalId::System(5012),
    needs_logs: true,
};

pub const MZ_PERF_PEEK_DURATIONS_AGGREGATES: BuiltinView = BuiltinView {
    name: "mz_perf_peek_durations_aggregates",
    schema: MZ_CATALOG_SCHEMA,
    sql: "CREATE VIEW mz_perf_peek_durations_aggregates AS SELECT worker, pg_catalog.sum(duration_ns * count) AS sum, pg_catalog.sum(count) AS count
FROM mz_catalog.mz_peek_durations lpd
GROUP BY worker",
    id: GlobalId::System(5013),
    needs_logs: true,
};

pub const MZ_PERF_DEPENDENCY_FRONTIERS: BuiltinView = BuiltinView {
    name: "mz_perf_dependency_frontiers",
    schema: MZ_CATALOG_SCHEMA,
    sql: "CREATE VIEW mz_perf_dependency_frontiers AS SELECT DISTINCT
    mcn.name AS dataflow,
    mcn_source.name AS source,
    frontier_source.time - frontier_df.time AS lag_ms
FROM mz_catalog.mz_materialization_dependencies index_deps
JOIN mz_catalog.mz_materialization_frontiers frontier_source ON index_deps.source = frontier_source.global_id
JOIN mz_catalog.mz_materialization_frontiers frontier_df ON index_deps.dataflow = frontier_df.global_id
JOIN mz_catalog.mz_catalog_names mcn ON mcn.global_id = index_deps.dataflow
JOIN mz_catalog.mz_catalog_names mcn_source ON mcn_source.global_id = frontier_source.global_id
UNION
SELECT DISTINCT
    mcn.name AS dataflow,
    mcn_source.name AS source,
    CASE WHEN source_info.time < frontier_df.time THEN 0 ELSE source_info.time - frontier_df.time END AS lag_ms
FROM mz_catalog.mz_materialization_dependencies index_deps
JOIN (SELECT source_id, pg_catalog.MAX(timestamp) time FROM mz_catalog.mz_source_info GROUP BY source_id) source_info ON index_deps.source = source_info.source_id
JOIN mz_catalog.mz_materialization_frontiers frontier_df ON index_deps.dataflow = frontier_df.global_id
JOIN mz_catalog.mz_catalog_names mcn ON mcn.global_id = index_deps.dataflow
JOIN mz_catalog.mz_catalog_names mcn_source ON mcn_source.global_id = source_info.source_id",
    id: GlobalId::System(5014),
    needs_logs: true,
};

pub const PG_NAMESPACE: BuiltinView = BuiltinView {
    name: "pg_namespace",
    schema: PG_CATALOG_SCHEMA,
    sql: "CREATE VIEW pg_namespace AS SELECT
oid,
name AS nspname,
NULL::pg_catalog.oid AS nspowner,
NULL::pg_catalog.text[] AS nspacl
FROM mz_catalog.mz_schemas",
    id: GlobalId::System(5015),
    needs_logs: false,
};

pub const PG_CLASS: BuiltinView = BuiltinView {
    name: "pg_class",
    schema: PG_CATALOG_SCHEMA,
    sql: "CREATE VIEW pg_class AS SELECT
    mz_objects.oid,
    mz_objects.name AS relname,
    mz_schemas.oid AS relnamespace,
    NULL::pg_catalog.oid AS relowner,
    CASE
        WHEN mz_objects.type = 'table' THEN 'r'
        WHEN mz_objects.type = 'source' THEN 'r'
        WHEN mz_objects.type = 'index' THEN 'i'
        WHEN mz_objects.type = 'view' THEN 'v'
    END relkind
FROM mz_catalog.mz_objects
JOIN mz_catalog.mz_schemas ON mz_schemas.id = mz_objects.schema_id",
    id: GlobalId::System(5016),
    needs_logs: false,
};

pub const PG_DATABASE: BuiltinView = BuiltinView {
    name: "pg_database",
    schema: PG_CATALOG_SCHEMA,
    sql: "CREATE VIEW pg_database AS SELECT
    oid,
    name as datname,
    NULL::pg_catalog.oid AS datdba,
    6 as encoding,
    'C' as datcollate,
    'C' as datctype,
    NULL::pg_catalog.text[] as datacl
FROM mz_catalog.mz_databases",
    id: GlobalId::System(5017),
    needs_logs: false,
};

pub const PG_INDEX: BuiltinView = BuiltinView {
    name: "pg_index",
    schema: PG_CATALOG_SCHEMA,
    sql: "CREATE VIEW pg_index AS SELECT
    mz_indexes.oid as indexrelid,
    mz_objects.oid as indrelid
FROM mz_catalog.mz_indexes
JOIN mz_catalog.mz_objects ON mz_indexes.on_id = mz_objects.id",
    id: GlobalId::System(5018),
    needs_logs: false,
};

pub const PG_DESCRIPTION: BuiltinView = BuiltinView {
    name: "pg_description",
    schema: PG_CATALOG_SCHEMA,
    sql: "CREATE VIEW pg_description AS SELECT
    oid as objoid,
    NULL::pg_catalog.oid as classoid,
    0::pg_catalog.int4 as objsubid,
    NULL::pg_catalog.text as description
FROM pg_catalog.pg_class",
    id: GlobalId::System(5019),
    needs_logs: false,
};

pub const PG_ATTRIBUTE: BuiltinView = BuiltinView {
    name: "pg_attribute",
    schema: PG_CATALOG_SCHEMA,
    sql: "CREATE VIEW pg_attribute AS SELECT
    mz_objects.oid as attrelid,
    mz_columns.name as attname,
    mz_types.oid AS atttypid,
    position as attnum,
    -1::pg_catalog.int4 as atttypmod,
    NOT nullable as attnotnull,
    FALSE as attisdropped
FROM mz_catalog.mz_objects
JOIN mz_catalog.mz_columns ON mz_objects.id = mz_columns.id
JOIN mz_catalog.mz_types ON mz_columns.type = mz_types.name",
    id: GlobalId::System(5020),
    needs_logs: false,
};

pub const PG_TYPE: BuiltinView = BuiltinView {
    name: "pg_type",
    schema: PG_CATALOG_SCHEMA,
    sql: "CREATE VIEW pg_type AS SELECT
    mz_types.oid,
    mz_types.name AS typname,
    mz_schemas.oid AS typnamespace,
    typtype,
    0::pg_catalog.oid AS typrelid,
    NULL::pg_catalog.oid AS typelem,
    coalesce(
        (
            SELECT
                t.oid
            FROM
                mz_catalog.mz_array_types AS a
                JOIN mz_catalog.mz_types AS t ON a.type_id = t.id
            WHERE
                a.element_id = mz_types.id
        ),
        0
    )
        AS typarray,
    NULL::pg_catalog.oid AS typreceive,
    false::pg_catalog.bool AS typnotnull,
    0::pg_catalog.oid AS typbasetype
FROM
    mz_catalog.mz_types
    JOIN mz_catalog.mz_schemas ON mz_schemas.id = mz_types.schema_id
    JOIN (
            SELECT type_id, 'a' AS typtype FROM mz_catalog.mz_array_types
            UNION ALL SELECT type_id, 'b' FROM mz_catalog.mz_base_types
            UNION ALL SELECT type_id, 'l' FROM mz_catalog.mz_list_types
            UNION ALL SELECT type_id, 'm' FROM mz_catalog.mz_map_types
            UNION ALL SELECT type_id, 'p' FROM mz_catalog.mz_pseudo_types
        )
            AS t ON mz_types.id = t.type_id",
    id: GlobalId::System(5021),
    needs_logs: false,
};

pub const PG_PROC: BuiltinView = BuiltinView {
    name: "pg_proc",
    schema: PG_CATALOG_SCHEMA,
    sql: "CREATE VIEW pg_proc AS SELECT
    NULL::pg_catalog.oid AS oid,
    NULL::pg_catalog.text AS proname
    WHERE false",
    id: GlobalId::System(5022),
    needs_logs: false,
};

pub const PG_RANGE: BuiltinView = BuiltinView {
    name: "pg_range",
    schema: PG_CATALOG_SCHEMA,
    sql: "CREATE VIEW pg_range AS SELECT
    NULL::pg_catalog.oid AS rngtypid,
    NULL::pg_catalog.oid AS rngsubtype
    WHERE false",
    id: GlobalId::System(5023),
    needs_logs: false,
};

pub const PG_ENUM: BuiltinView = BuiltinView {
    name: "pg_enum",
    schema: PG_CATALOG_SCHEMA,
    sql: "CREATE VIEW pg_enum AS SELECT
    NULL::pg_catalog.oid AS oid,
    NULL::pg_catalog.oid AS enumtypid,
    NULL::pg_catalog.float4 AS enumsortorder,
    NULL::pg_catalog.text AS enumlabel
    WHERE false",
    id: GlobalId::System(5024),
    needs_logs: false,
};

pub const MZ_SYSTEM: BuiltinRole = BuiltinRole {
    name: "mz_system",
    id: -1,
};

lazy_static! {
    pub static ref BUILTINS: BTreeMap<GlobalId, Builtin> = {
        let mut builtins = vec![
            Builtin::Type(&TYPE_ANY),
            Builtin::Type(&TYPE_ANYARRAY),
            Builtin::Type(&TYPE_ANYELEMENT),
            Builtin::Type(&TYPE_ANYNONARRAY),
            Builtin::Type(&TYPE_BOOL),
            Builtin::Type(&TYPE_BOOL_ARRAY),
            Builtin::Type(&TYPE_BYTEA),
            Builtin::Type(&TYPE_BYTEA_ARRAY),
            Builtin::Type(&TYPE_CHAR),
            Builtin::Type(&TYPE_DATE),
            Builtin::Type(&TYPE_DATE_ARRAY),
            Builtin::Type(&TYPE_FLOAT4),
            Builtin::Type(&TYPE_FLOAT4_ARRAY),
            Builtin::Type(&TYPE_FLOAT8),
            Builtin::Type(&TYPE_FLOAT8_ARRAY),
            Builtin::Type(&TYPE_INT4),
            Builtin::Type(&TYPE_INT4_ARRAY),
            Builtin::Type(&TYPE_INT8),
            Builtin::Type(&TYPE_INT8_ARRAY),
            Builtin::Type(&TYPE_INTERVAL),
            Builtin::Type(&TYPE_INTERVAL_ARRAY),
            Builtin::Type(&TYPE_JSONB),
            Builtin::Type(&TYPE_JSONB_ARRAY),
            Builtin::Type(&TYPE_RDN),
            Builtin::Type(&TYPE_LIST),
            Builtin::Type(&TYPE_MAP),
            Builtin::Type(&TYPE_NUMERIC),
            Builtin::Type(&TYPE_NUMERIC_ARRAY),
            Builtin::Type(&TYPE_OID),
            Builtin::Type(&TYPE_OID_ARRAY),
            Builtin::Type(&TYPE_RECORD),
            Builtin::Type(&TYPE_RECORD_ARRAY),
            Builtin::Type(&TYPE_INT2),
            Builtin::Type(&TYPE_INT2_ARRAY),
            Builtin::Type(&TYPE_TEXT),
            Builtin::Type(&TYPE_TEXT_ARRAY),
            Builtin::Type(&TYPE_TIME),
            Builtin::Type(&TYPE_TIME_ARRAY),
            Builtin::Type(&TYPE_TIMESTAMP),
            Builtin::Type(&TYPE_TIMESTAMP_ARRAY),
            Builtin::Type(&TYPE_TIMESTAMPTZ),
            Builtin::Type(&TYPE_TIMESTAMPTZ_ARRAY),
            Builtin::Type(&TYPE_UUID),
            Builtin::Type(&TYPE_UUID_ARRAY),
            Builtin::Type(&TYPE_VARCHAR),
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
            Builtin::Log(&MZ_SOURCE_INFO),
            Builtin::Log(&MZ_MESSAGE_COUNTS),
            Builtin::Log(&MZ_KAFKA_CONSUMER_PARTITIONS),
            Builtin::Log(&MZ_KAFKA_BROKER_RTT),
            Builtin::Table(&MZ_VIEW_KEYS),
            Builtin::Table(&MZ_VIEW_FOREIGN_KEYS),
            Builtin::Table(&MZ_KAFKA_SINKS),
            Builtin::Table(&MZ_AVRO_OCF_SINKS),
            Builtin::Table(&MZ_DATABASES),
            Builtin::Table(&MZ_SCHEMAS),
            Builtin::Table(&MZ_COLUMNS),
            Builtin::Table(&MZ_INDEXES),
            Builtin::Table(&MZ_INDEX_COLUMNS),
            Builtin::Table(&MZ_TABLES),
            Builtin::Table(&MZ_SOURCES),
            Builtin::Table(&MZ_SINKS),
            Builtin::Table(&MZ_VIEWS),
            Builtin::Table(&MZ_TYPES),
            Builtin::Table(&MZ_ARRAY_TYPES),
            Builtin::Table(&MZ_BASE_TYPES),
            Builtin::Table(&MZ_LIST_TYPES),
            Builtin::Table(&MZ_MAP_TYPES),
            Builtin::Table(&MZ_ROLES),
            Builtin::Table(&MZ_PSEUDO_TYPES),
            Builtin::Table(&MZ_FUNCTIONS),
            Builtin::Table(&MZ_PROMETHEUS_READINGS),
            Builtin::Table(&MZ_PROMETHEUS_HISTOGRAMS),
            Builtin::Table(&MZ_PROMETHEUS_METRICS),
            Builtin::View(&MZ_RELATIONS),
            Builtin::View(&MZ_OBJECTS),
            Builtin::View(&MZ_CATALOG_NAMES),
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
            Builtin::View(&PG_NAMESPACE),
            Builtin::View(&PG_CLASS),
            Builtin::View(&PG_DATABASE),
            Builtin::View(&PG_INDEX),
            Builtin::View(&PG_DESCRIPTION),
            Builtin::View(&PG_ATTRIBUTE),
            Builtin::View(&PG_TYPE),
            Builtin::View(&PG_PROC),
            Builtin::View(&PG_RANGE),
            Builtin::View(&PG_ENUM),
        ];

        // TODO(sploiselle): assign static global IDs to functions
        let mut func_global_id_counter = 2000;

        for (schema, funcs) in &[
            (PG_CATALOG_SCHEMA, &*sql::func::PG_CATALOG_BUILTINS),
            (MZ_CATALOG_SCHEMA, &*sql::func::MZ_CATALOG_BUILTINS),
            (MZ_INTERNAL_SCHEMA, &*sql::func::MZ_INTERNAL_BUILTINS),
        ] {
            for (name, func) in funcs.iter() {
                builtins.push(Builtin::Func(BuiltinFunc {
                    name,
                    schema,
                    id: GlobalId::System(func_global_id_counter),
                    inner: func,
                }));
                func_global_id_counter += 1;
            }
        }

        assert!(func_global_id_counter < 3000, "exhausted func global IDs");

        // Ensure the IDs we assign are all unique:
        let mut encountered = BTreeSet::<GlobalId>::new();
        let mut encounter =
            move |kind_name: &str, field_name: &str, identifier: &str, id: &GlobalId| {
                assert!(
                    encountered.insert(id.to_owned()),
                    "{} for {} {:?} is already used as a global ID: {:?}",
                    field_name,
                    kind_name,
                    identifier,
                    id,
                );
            };
        use Builtin::*;
        for b in builtins.iter() {
            match b {
                Type(BuiltinType{id, ..}) => {
                    let name = format!("with ID {:?}", id);
                    encounter("type", "id", &name, id);
                }
                Log(BuiltinLog { id, name, index_id, .. }) => {
                    encounter("type", "id", name, id);
                    encounter("type", "index_id", name, index_id);
                }
                Table(BuiltinTable { id, index_id, name, .. }) => {
                    encounter("builtin table", "id", name, id);
                    encounter("builtin table", "index_id", name, index_id);
                }
                View(BuiltinView { id, name, .. }) => {
                    encounter("view", "id", name, id);
                }
                Func(BuiltinFunc { id, name, .. }) => {
                    encounter("function", "id", name, id);
                }
            }
        }

        builtins.into_iter().map(|b| (b.id(), b)).collect()
    };

    pub static ref BUILTIN_ROLES: Vec<BuiltinRole> = vec![MZ_SYSTEM];
}

impl BUILTINS {
    pub fn logs(&self) -> impl Iterator<Item = &'static BuiltinLog> + '_ {
        self.values().filter_map(|b| match b {
            Builtin::Log(log) => Some(*log),
            _ => None,
        })
    }
}
