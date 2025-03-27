// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Types related to SQL Server sources

use std::fmt;
use std::sync::LazyLock;

use columnation::{Columnation, CopyRegion};
use mz_ore::future::InTask;
use mz_proto::{IntoRustIfSome, RustType};
use mz_repr::{CatalogItemId, Datum, GlobalId, RelationDesc, Row, ScalarType};
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};
use timely::progress::Antichain;

use crate::connections::inline::{
    ConnectionAccess, ConnectionResolver, InlinedConnection, IntoInlineConnection,
    ReferencedConnection,
};
use crate::controller::AlterError;
use crate::sources::{SourceConnection, SourceExportDetails, SourceTimestamp};
use crate::AlterCompatible;

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

        let maximum_raw_lsn = mz_sql_server_util::inspect::get_max_lsn(&mut client).await?;
        let maximum_lsn = Lsn::try_from(&maximum_raw_lsn[..])?;

        Ok(Antichain::from_elem(maximum_lsn))
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
        "sql server"
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

/// This type is used to represent the progress of each SQL Server instance in
/// the ingestion dataflow.
///
/// A SQL Server LSN is essentially an opaque binary blob that provides a
/// __total order__ to all transations within a database. Technically though an
/// LSN has three components:
///
/// 1. A Virtual Log File (VLF) sequence number, bytes [0, 4)
/// 2. Log block number, bytes [4, 8)
/// 3. Log record number, bytes [8, 10)
///
/// To increment an LSN you need to call the [`sys.fn_cdc_increment_lsn`] T-SQL
/// function.
///
/// [`sys.fn_cdc_increment_lsn`](https://learn.microsoft.com/en-us/sql/relational-databases/system-functions/sys-fn-cdc-increment-lsn-transact-sql?view=sql-server-ver16)
#[derive(
    Copy, Clone, Debug, Eq, PartialEq, PartialOrd, Ord, Hash, Serialize, Deserialize, Arbitrary,
)]
pub struct Lsn([u8; 10]);

impl TryFrom<&[u8]> for Lsn {
    // TODO(sql_server2): Add an InvalidLsnError type that can identify when
    // the LSN is reported as 0xNULL, which I think happens when CDC is not enabled.
    type Error = anyhow::Error;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        let raw = value
            .try_into()
            .map_err(|_| anyhow::anyhow!("invalid SQL Server LSN"))?;
        Ok(Lsn(raw))
    }
}

impl fmt::Display for Lsn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // TODO(sql_server1): Pretty print this as the three components.
        write!(f, "{self:?}")
    }
}

impl Columnation for Lsn {
    type InnerRegion = CopyRegion<Lsn>;
}

impl timely::progress::Timestamp for Lsn {
    // No need to describe complex summaries.
    type Summary = ();

    fn minimum() -> Self {
        Lsn(Default::default())
    }
}

impl timely::progress::PathSummary<Lsn> for () {
    fn results_in(&self, src: &Lsn) -> Option<Lsn> {
        Some(*src)
    }

    fn followed_by(&self, _other: &Self) -> Option<Self> {
        Some(())
    }
}

impl timely::progress::timestamp::Refines<()> for Lsn {
    fn to_inner(_other: ()) -> Self {
        use timely::progress::Timestamp;
        Self::minimum()
    }
    fn to_outer(self) -> () {}

    fn summarize(_path: <Self as timely::progress::Timestamp>::Summary) -> () {}
}

impl timely::order::PartialOrder for Lsn {
    fn less_equal(&self, other: &Self) -> bool {
        self <= other
    }

    fn less_than(&self, other: &Self) -> bool {
        self < other
    }
}
impl timely::order::TotalOrder for Lsn {}

impl SourceTimestamp for Lsn {
    fn encode_row(&self) -> mz_repr::Row {
        Row::pack_slice(&[Datum::Bytes(&self.0[..])])
    }

    fn decode_row(row: &mz_repr::Row) -> Self {
        let mut datums = row.iter();
        match (datums.next(), datums.next()) {
            (Some(Datum::Bytes(bytes)), None) => {
                let lsn: [u8; 10] = bytes.try_into().expect("invalid LSN, wrong length");
                Lsn(lsn)
            }
            _ => panic!("invalid row {row:?}"),
        }
    }
}
