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

use mz_ore::future::InTask;
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
        let (mut client, conn) = mz_sql_server_util::Client::connect(config).await?;
        // TODO(sql_server1): Make spawning this task automatic.
        mz_ore::task::spawn(|| "sql server connection", async move { conn.await });

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

impl SourceTimestamp for Lsn {
    fn encode_row(&self) -> mz_repr::Row {
        Row::pack_slice(&[Datum::Bytes(self.as_bytes())])
    }

    fn decode_row(row: &mz_repr::Row) -> Self {
        let mut datums = row.iter();
        match (datums.next(), datums.next()) {
            (Some(Datum::Bytes(bytes)), None) => {
                let lsn: [u8; 10] = bytes.try_into().expect("invalid LSN, wrong length");
                Lsn::interpret(lsn)
            }
            _ => panic!("invalid row {row:?}"),
        }
    }
}
