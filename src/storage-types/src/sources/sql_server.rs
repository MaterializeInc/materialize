// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Types related to SQL Server sources

use std::sync::{Arc, LazyLock};
use std::time::Duration;

use mz_dyncfg::Config;
use mz_ore::future::InTask;
use mz_proto::{IntoRustIfSome, RustType};
use mz_repr::{CatalogItemId, Datum, GlobalId, RelationDesc, Row, ScalarType};
use mz_sql_server_util::cdc::Lsn;
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};
use timely::progress::Antichain;

use crate::AlterCompatible;
use crate::connections::inline::{
    ConnectionAccess, ConnectionResolver, InlinedConnection, IntoInlineConnection,
    ReferencedConnection,
};
use crate::controller::AlterError;
use crate::sources::{SourceConnection, SourceExportDetails, SourceTimestamp};

include!(concat!(
    env!("OUT_DIR"),
    "/mz_storage_types.sources.sql_server.rs"
));

pub const SNAPSHOT_MAX_LSN_WAIT: Config<Duration> = Config::new(
    "sql_server_snapshot_max_lsn_wait",
    Duration::from_secs(30),
    "Maximum amount of time we'll wait for SQL Server to report an LSN (in other words for \
    CDC to be fully enabled) before taking an initial snapshot.",
);

pub const SNAPSHOT_PROGRESS_REPORT_INTERVAL: Config<Duration> = Config::new(
    "sql_server_snapshot_progress_report_interval",
    Duration::from_secs(2),
    "Interval at which we'll report progress for currently running snapshots.",
);

pub const CDC_POLL_INTERVAL: Config<Duration> = Config::new(
    "sql_server_cdc_poll_interval",
    Duration::from_millis(500),
    "Interval at which we'll poll the upstream SQL Server instance to discover new changes.",
);

pub const CDC_CLEANUP_CHANGE_TABLE: Config<bool> = Config::new(
    "sql_server_cdc_cleanup_change_table",
    false,
    "When enabled we'll notify SQL Server that it can cleanup the change tables \
    as the source makes progress and commits data.",
);

/// Maximum number of deletes that we'll make from a single SQL Server change table.
///
/// See: <https://learn.microsoft.com/en-us/sql/relational-databases/system-stored-procedures/sys-sp-cdc-cleanup-change-table-transact-sql?view=sql-server-ver16>.
pub const CDC_CLEANUP_CHANGE_TABLE_MAX_DELETES: Config<u32> = Config::new(
    "sql_server_cdc_cleanup_change_table_max_deletes",
    // The default in SQL Server is 5,000 but until we change the cleanup
    // function to call it iteratively we set a large value here.
    //
    // TODO(sql_server2): Call the cleanup function iteratively.
    1_000_000,
    "Maximum number of entries that can be deleted by using a single statement.",
);

pub const OFFSET_KNOWN_INTERVAL: Config<Duration> = Config::new(
    "sql_server_offset_known_interval",
    Duration::from_secs(10),
    "Interval to fetch `offset_known`, from `sys.fn_cdc_get_max_lsn()`",
);

pub static SQL_SERVER_PROGRESS_DESC: LazyLock<RelationDesc> = LazyLock::new(|| {
    RelationDesc::builder()
        .with_column("lsn", ScalarType::Bytes.nullable(true))
        .finish()
});

/// Details about how to create a Materialize Source that reads from Microsoft SQL Server.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Arbitrary)]
pub struct SqlServerSource<C: ConnectionAccess = InlinedConnection> {
    /// ID of this SQL `SOURCE` object in the Catalog.
    pub catalog_id: CatalogItemId,
    /// Configuration for connecting to SQL Server.
    pub connection: C::SqlServer,
    /// SQL Server specific information that is relevant to creating a source.
    pub extras: SqlServerSourceExtras,
}

impl SqlServerSource<InlinedConnection> {
    pub async fn fetch_write_frontier(
        self,
        storage_configuration: &crate::configuration::StorageConfiguration,
    ) -> Result<Antichain<Lsn>, anyhow::Error> {
        let config = self
            .connection
            .resolve_config(
                &storage_configuration.connection_context.secrets_reader,
                storage_configuration,
                InTask::No,
            )
            .await?;
        let mut client = mz_sql_server_util::Client::connect(config).await?;

        let max_lsn = mz_sql_server_util::inspect::get_max_lsn(&mut client).await?;
        Ok(Antichain::from_elem(max_lsn))
    }
}

impl<R: ConnectionResolver> IntoInlineConnection<SqlServerSource, R>
    for SqlServerSource<ReferencedConnection>
{
    fn into_inline_connection(self, r: R) -> SqlServerSource {
        let SqlServerSource {
            catalog_id,
            connection,
            extras,
        } = self;

        SqlServerSource {
            catalog_id,
            connection: r.resolve_connection(connection).unwrap_sql_server(),
            extras,
        }
    }
}

impl<C: ConnectionAccess> SourceConnection for SqlServerSource<C> {
    fn name(&self) -> &'static str {
        "sql-server"
    }

    fn external_reference(&self) -> Option<&str> {
        None
    }

    fn default_key_desc(&self) -> RelationDesc {
        RelationDesc::empty()
    }

    fn default_value_desc(&self) -> RelationDesc {
        // The SQL Server source only outputs data to its subsources. The catalog object
        // representing the source itself is just an empty relation with no columns
        RelationDesc::empty()
    }

    fn timestamp_desc(&self) -> RelationDesc {
        SQL_SERVER_PROGRESS_DESC.clone()
    }

    fn connection_id(&self) -> Option<CatalogItemId> {
        Some(self.catalog_id)
    }

    fn primary_export_details(&self) -> super::SourceExportDetails {
        SourceExportDetails::None
    }

    fn supports_read_only(&self) -> bool {
        false
    }

    fn prefers_single_replica(&self) -> bool {
        true
    }
}

impl<C: ConnectionAccess> AlterCompatible for SqlServerSource<C> {
    fn alter_compatible(&self, id: GlobalId, other: &Self) -> Result<(), AlterError> {
        if self == other {
            return Ok(());
        }

        let SqlServerSource {
            catalog_id,
            connection,
            extras,
        } = self;

        let compatibility_checks = [
            (catalog_id == &other.catalog_id, "catalog_id"),
            (
                connection.alter_compatible(id, &other.connection).is_ok(),
                "connection",
            ),
            (extras.alter_compatible(id, &other.extras).is_ok(), "extras"),
        ];

        for (compatible, field) in compatibility_checks {
            if !compatible {
                tracing::warn!(
                    "SqlServerSourceConnection incompatible at {field}:\nself:\n{:#?}\n\nother\n{:#?}",
                    self,
                    other
                );

                return Err(AlterError { id });
            }
        }

        Ok(())
    }
}

impl RustType<ProtoSqlServerSource> for SqlServerSource {
    fn into_proto(&self) -> ProtoSqlServerSource {
        ProtoSqlServerSource {
            catalog_id: Some(self.catalog_id.into_proto()),
            connection: Some(self.connection.into_proto()),
            extras: Some(self.extras.into_proto()),
        }
    }

    fn from_proto(proto: ProtoSqlServerSource) -> Result<Self, mz_proto::TryFromProtoError> {
        Ok(SqlServerSource {
            catalog_id: proto
                .catalog_id
                .into_rust_if_some("ProtoSqlServerSource::catalog_id")?,
            connection: proto
                .connection
                .into_rust_if_some("ProtoSqlServerSource::connection")?,
            extras: proto
                .extras
                .into_rust_if_some("ProtoSqlServerSource::extras")?,
        })
    }
}

/// Extra information that is pertinent to creating a SQL Server specific
/// Materialize source.
///
/// The information in this struct is durably recorded by serializing it as an
/// option in the `CREATE SOURCE` SQL statement, thus backward compatibility is
/// important!
///
/// It's currently unused but we keep the struct around to maintain conformity
/// with other sources.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Arbitrary)]
pub struct SqlServerSourceExtras {}

impl AlterCompatible for SqlServerSourceExtras {
    fn alter_compatible(&self, _id: GlobalId, _other: &Self) -> Result<(), AlterError> {
        Ok(())
    }
}

impl RustType<ProtoSqlServerSourceExtras> for SqlServerSourceExtras {
    fn into_proto(&self) -> ProtoSqlServerSourceExtras {
        ProtoSqlServerSourceExtras {}
    }

    fn from_proto(_proto: ProtoSqlServerSourceExtras) -> Result<Self, mz_proto::TryFromProtoError> {
        Ok(SqlServerSourceExtras {})
    }
}

/// Specifies the details of a SQL Server source export.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Arbitrary)]
pub struct SqlServerSourceExportDetails {
    /// Name of the SQL Server capture instance we replicate changes from.
    pub capture_instance: Arc<str>,
    /// Description of the upstream table and how it maps to Materialize.
    pub table: mz_sql_server_util::desc::SqlServerTableDesc,
    /// Column names that we want to parse as text.
    pub text_columns: Vec<String>,
    /// Columns from the upstream source that should be excluded.
    pub exclude_columns: Vec<String>,
}

impl RustType<ProtoSqlServerSourceExportDetails> for SqlServerSourceExportDetails {
    fn into_proto(&self) -> ProtoSqlServerSourceExportDetails {
        ProtoSqlServerSourceExportDetails {
            capture_instance: self.capture_instance.to_string(),
            table: Some(self.table.into_proto()),
            text_columns: self.text_columns.clone(),
            exclude_columns: self.exclude_columns.clone(),
        }
    }

    fn from_proto(
        proto: ProtoSqlServerSourceExportDetails,
    ) -> Result<Self, mz_proto::TryFromProtoError> {
        Ok(SqlServerSourceExportDetails {
            capture_instance: proto.capture_instance.into(),
            table: proto
                .table
                .into_rust_if_some("ProtoSqlServerSourceExportDetails::table")?,
            text_columns: proto.text_columns,
            exclude_columns: proto.exclude_columns,
        })
    }
}

impl SourceTimestamp for Lsn {
    fn encode_row(&self) -> mz_repr::Row {
        Row::pack_slice(&[Datum::Bytes(&self.as_bytes())])
    }

    fn decode_row(row: &mz_repr::Row) -> Self {
        let mut datums = row.iter();
        match (datums.next(), datums.next()) {
            (Some(Datum::Bytes(bytes)), None) => {
                let lsn: [u8; 10] = bytes.try_into().expect("invalid LSN, wrong length");
                Lsn::try_from_bytes(&lsn).expect("invalid LSN")
            }
            _ => panic!("invalid row {row:?}"),
        }
    }
}
