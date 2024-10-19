// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Types related to postgres sources

use mz_expr::MirScalarExpr;
use mz_postgres_util::desc::PostgresTableDesc;
use mz_proto::{IntoRustIfSome, RustType, TryFromProtoError};
use mz_repr::{CatalogItemId, GlobalId, RelationDesc, ScalarType};
use proptest::prelude::any;
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};
use std::sync::LazyLock;

use crate::connections::inline::{
    ConnectionAccess, ConnectionResolver, InlinedConnection, IntoInlineConnection,
    ReferencedConnection,
};
use crate::controller::AlterError;
use crate::sources::{MzOffset, SourceConnection};
use crate::AlterCompatible;

use super::SourceExportDetails;

include!(concat!(
    env!("OUT_DIR"),
    "/mz_storage_types.sources.postgres.rs"
));

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Arbitrary)]
pub struct PostgresSourceConnection<C: ConnectionAccess = InlinedConnection> {
    pub connection_id: CatalogItemId,
    pub connection: C::Pg,
    pub publication: String,
    pub publication_details: PostgresSourcePublicationDetails,
}

impl<R: ConnectionResolver> IntoInlineConnection<PostgresSourceConnection, R>
    for PostgresSourceConnection<ReferencedConnection>
{
    fn into_inline_connection(self, r: R) -> PostgresSourceConnection {
        let PostgresSourceConnection {
            connection_id,
            connection,
            publication,
            publication_details,
        } = self;

        PostgresSourceConnection {
            connection_id,
            connection: r.resolve_connection(connection).unwrap_pg(),
            publication,
            publication_details,
        }
    }
}

pub static PG_PROGRESS_DESC: LazyLock<RelationDesc> = LazyLock::new(|| {
    RelationDesc::builder()
        .with_column("lsn", ScalarType::UInt64.nullable(true))
        .finish()
});

impl PostgresSourceConnection {
    pub async fn fetch_write_frontier(
        self,
        storage_configuration: &crate::configuration::StorageConfiguration,
    ) -> Result<timely::progress::Antichain<MzOffset>, anyhow::Error> {
        use timely::progress::Antichain;

        let config = self
            .connection
            .config(
                &storage_configuration.connection_context.secrets_reader,
                storage_configuration,
                mz_ore::future::InTask::No,
            )
            .await?;
        let client = config
            .connect(
                "postgres_wal_lsn",
                &storage_configuration.connection_context.ssh_tunnel_manager,
            )
            .await?;

        let lsn = mz_postgres_util::get_current_wal_lsn(&client).await?;

        let current_upper = Antichain::from_elem(MzOffset::from(u64::from(lsn)));
        Ok(current_upper)
    }
}

impl<C: ConnectionAccess> SourceConnection for PostgresSourceConnection<C> {
    fn name(&self) -> &'static str {
        "postgres"
    }

    fn external_reference(&self) -> Option<&str> {
        None
    }

    fn default_key_desc(&self) -> RelationDesc {
        RelationDesc::empty()
    }

    fn default_value_desc(&self) -> RelationDesc {
        // The postgres source only outputs data to its subsources. The catalog object
        // representing the source itself is just an empty relation with no columns
        RelationDesc::empty()
    }

    fn timestamp_desc(&self) -> RelationDesc {
        PG_PROGRESS_DESC.clone()
    }

    fn connection_id(&self) -> Option<CatalogItemId> {
        Some(self.connection_id)
    }

    fn primary_export_details(&self) -> SourceExportDetails {
        SourceExportDetails::None
    }

    fn supports_read_only(&self) -> bool {
        false
    }
}

impl<C: ConnectionAccess> AlterCompatible for PostgresSourceConnection<C> {
    fn alter_compatible(&self, id: GlobalId, other: &Self) -> Result<(), AlterError> {
        if self == other {
            return Ok(());
        }

        let PostgresSourceConnection {
            connection_id,
            connection,
            publication,
            publication_details,
        } = self;

        let compatibility_checks = [
            (connection_id == &other.connection_id, "connection_id"),
            (
                connection.alter_compatible(id, &other.connection).is_ok(),
                "connection",
            ),
            (publication == &other.publication, "publication"),
            (
                publication_details
                    .alter_compatible(id, &other.publication_details)
                    .is_ok(),
                "publication_details",
            ),
        ];

        for (compatible, field) in compatibility_checks {
            if !compatible {
                tracing::warn!(
                    "PostgresSourceConnection incompatible at {field}:\nself:\n{:#?}\n\nother\n{:#?}",
                    self,
                    other
                );

                return Err(AlterError { id });
            }
        }

        Ok(())
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Arbitrary)]
pub enum CastType {
    /// This cast is corresponds to its type in the upstream system.
    Natural,
    /// This cast was generated with the `TEXT COLUMNS` option.
    Text,
}

impl RustType<ProtoCastType> for CastType {
    fn into_proto(&self) -> ProtoCastType {
        use proto_cast_type::Kind::*;
        ProtoCastType {
            kind: Some(match self {
                CastType::Natural => Natural(()),
                CastType::Text => Text(()),
            }),
        }
    }

    fn from_proto(proto: ProtoCastType) -> Result<Self, TryFromProtoError> {
        use proto_cast_type::Kind::*;
        Ok(match proto.kind {
            Some(Natural(())) => CastType::Natural,
            Some(Text(())) => CastType::Text,
            None => {
                return Err(TryFromProtoError::missing_field(
                    "ProtoWindowFrameUnits::kind",
                ))
            }
        })
    }
}

impl RustType<ProtoPostgresSourceConnection> for PostgresSourceConnection {
    fn into_proto(&self) -> ProtoPostgresSourceConnection {
        ProtoPostgresSourceConnection {
            connection: Some(self.connection.into_proto()),
            connection_id: Some(self.connection_id.into_proto()),
            publication: self.publication.clone(),
            details: Some(self.publication_details.into_proto()),
        }
    }

    fn from_proto(proto: ProtoPostgresSourceConnection) -> Result<Self, TryFromProtoError> {
        Ok(PostgresSourceConnection {
            connection: proto
                .connection
                .into_rust_if_some("ProtoPostgresSourceConnection::connection")?,
            connection_id: proto
                .connection_id
                .into_rust_if_some("ProtoPostgresSourceConnection::connection_id")?,
            publication: proto.publication,
            publication_details: proto
                .details
                .into_rust_if_some("ProtoPostgresSourceConnection::details")?,
        })
    }
}

/// The details of a source export from a postgres source.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Arbitrary)]
pub struct PostgresSourceExportDetails {
    /// The cast expressions to convert the incoming string encoded rows to
    /// their target types
    #[proptest(
        strategy = "proptest::collection::vec((any::<CastType>(), any::<MirScalarExpr>()), 0..4)"
    )]
    pub column_casts: Vec<(CastType, MirScalarExpr)>,
    pub table: PostgresTableDesc,
}

impl AlterCompatible for PostgresSourceExportDetails {
    fn alter_compatible(&self, _id: GlobalId, _other: &Self) -> Result<(), AlterError> {
        // compatibility checks are performed against the upstream table in the source
        // render operators instead
        let Self {
            column_casts: _,
            table: _,
        } = self;
        Ok(())
    }
}

impl RustType<ProtoPostgresSourceExportDetails> for PostgresSourceExportDetails {
    fn into_proto(&self) -> ProtoPostgresSourceExportDetails {
        let mut column_casts = Vec::with_capacity(self.column_casts.len());

        for (col_type, cast) in self.column_casts.iter() {
            column_casts.push(ProtoPostgresColumnCast {
                cast: Some(cast.into_proto()),
                cast_type: Some(col_type.into_proto()),
            });
        }

        ProtoPostgresSourceExportDetails {
            table: Some(self.table.into_proto()),
            column_casts,
        }
    }

    fn from_proto(proto: ProtoPostgresSourceExportDetails) -> Result<Self, TryFromProtoError> {
        let mut column_casts = vec![];
        for column_cast in proto.column_casts.into_iter() {
            column_casts.push((
                column_cast
                    .cast_type
                    .into_rust_if_some("ProtoPostgresColumnCast::cast_type")?,
                column_cast
                    .cast
                    .into_rust_if_some("ProtoPostgresColumnCast::cast")?,
            ));
        }

        Ok(PostgresSourceExportDetails {
            table: proto
                .table
                .into_rust_if_some("ProtoPostgresSourceExportDetails::table")?,
            column_casts,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Arbitrary)]
pub struct PostgresSourcePublicationDetails {
    pub slot: String,
    /// The active timeline_id when this source was created
    /// The None value indicates an unknown timeline, to account for sources that existed
    /// prior to this field being introduced
    pub timeline_id: Option<u64>,
    pub database: String,
}

impl RustType<ProtoPostgresSourcePublicationDetails> for PostgresSourcePublicationDetails {
    fn into_proto(&self) -> ProtoPostgresSourcePublicationDetails {
        ProtoPostgresSourcePublicationDetails {
            slot: self.slot.clone(),
            timeline_id: self.timeline_id.clone(),
            database: self.database.clone(),
        }
    }

    fn from_proto(proto: ProtoPostgresSourcePublicationDetails) -> Result<Self, TryFromProtoError> {
        Ok(PostgresSourcePublicationDetails {
            slot: proto.slot,
            timeline_id: proto.timeline_id,
            database: proto.database,
        })
    }
}

impl AlterCompatible for PostgresSourcePublicationDetails {
    fn alter_compatible(&self, id: GlobalId, other: &Self) -> Result<(), AlterError> {
        let PostgresSourcePublicationDetails {
            slot,
            timeline_id,
            database,
        } = self;

        let compatibility_checks = [
            (slot == &other.slot, "slot"),
            (
                match (timeline_id, &other.timeline_id) {
                    (Some(curr_id), Some(new_id)) => curr_id == new_id,
                    (None, Some(_)) => true,
                    // New values must always have timeline ID
                    (_, None) => false,
                },
                "timeline_id",
            ),
            (database == &other.database, "database"),
        ];

        for (compatible, field) in compatibility_checks {
            if !compatible {
                tracing::warn!(
                    "PostgresSourcePublicationDetails incompatible at {field}:\nself:\n{:#?}\n\nother\n{:#?}",
                    self,
                    other
                );

                return Err(AlterError { id });
            }
        }

        Ok(())
    }
}
