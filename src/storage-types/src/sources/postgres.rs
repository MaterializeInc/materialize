// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Types related to postgres sources

use std::collections::BTreeMap;

use itertools::Itertools;
use mz_expr::MirScalarExpr;
use mz_postgres_util::desc::PostgresTableDesc;
use mz_proto::{IntoRustIfSome, ProtoType, RustType, TryFromProtoError};
use mz_repr::{ColumnType, GlobalId, RelationDesc, ScalarType};
use once_cell::sync::Lazy;
use proptest::prelude::any;
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};

use crate::connections::inline::{
    ConnectionAccess, ConnectionResolver, InlinedConnection, IntoInlineConnection,
    ReferencedConnection,
};
use crate::controller::AlterError;
use crate::sources::SourceConnection;
use crate::AlterCompatible;

include!(concat!(
    env!("OUT_DIR"),
    "/mz_storage_types.sources.postgres.rs"
));

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Arbitrary)]
pub struct PostgresSourceConnection<C: ConnectionAccess = InlinedConnection> {
    pub connection_id: GlobalId,
    pub connection: C::Pg,
    /// The cast expressions to convert the incoming string encoded rows to
    /// their target types, keyed by their position in the source.
    #[proptest(
        strategy = "proptest::collection::btree_map(any::<usize>(), proptest::collection::vec((any::<CastType>(), any::<MirScalarExpr>()), 0..4), 0..4)"
    )]
    pub table_casts: BTreeMap<usize, Vec<(CastType, MirScalarExpr)>>,
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
            table_casts,
            publication,
            publication_details,
        } = self;

        PostgresSourceConnection {
            connection_id,
            connection: r.resolve_connection(connection).unwrap_pg(),
            table_casts,
            publication,
            publication_details,
        }
    }
}

pub static PG_PROGRESS_DESC: Lazy<RelationDesc> =
    Lazy::new(|| RelationDesc::empty().with_column("lsn", ScalarType::UInt64.nullable(true)));

impl<C: ConnectionAccess> SourceConnection for PostgresSourceConnection<C> {
    fn name(&self) -> &'static str {
        "postgres"
    }

    fn upstream_name(&self) -> Option<&str> {
        None
    }

    fn key_desc(&self) -> RelationDesc {
        RelationDesc::empty()
    }

    fn value_desc(&self) -> RelationDesc {
        // The postgres source only outputs data to its subsources. The catalog object
        // representing the source itself is just an empty relation with no columns
        RelationDesc::empty()
    }

    fn timestamp_desc(&self) -> RelationDesc {
        PG_PROGRESS_DESC.clone()
    }

    fn connection_id(&self) -> Option<GlobalId> {
        Some(self.connection_id)
    }

    fn metadata_columns(&self) -> Vec<(&str, ColumnType)> {
        vec![]
    }

    fn output_idx_for_name(&self, name: &mz_sql_parser::ast::UnresolvedItemName) -> Option<usize> {
        self.publication_details.output_idx_for_name(name)
    }
}

impl<C: ConnectionAccess> crate::AlterCompatible for PostgresSourceConnection<C> {
    fn alter_compatible(&self, id: GlobalId, other: &Self) -> Result<(), AlterError> {
        if self == other {
            return Ok(());
        }

        let PostgresSourceConnection {
            connection_id,
            connection,
            // Table casts may change and we will not, in the long term, have a
            // means of understanding which tables are actually being used by
            // the source so it's unclear if these changes will be breaking or
            // not. This suggests that we might not want to maintain this
            // information here statically––we could, instead, derive these
            // casts dynamically from the table's schema for the subsource, and
            // then the subsource itself understands how to cast its data from
            // the source.
            table_casts: _,
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

impl RustType<proto_postgres_source_connection::ProtoCastType> for CastType {
    fn into_proto(&self) -> proto_postgres_source_connection::ProtoCastType {
        use proto_postgres_source_connection::proto_cast_type::Kind::*;
        proto_postgres_source_connection::ProtoCastType {
            kind: Some(match self {
                CastType::Natural => Natural(()),
                CastType::Text => Text(()),
            }),
        }
    }

    fn from_proto(
        proto: proto_postgres_source_connection::ProtoCastType,
    ) -> Result<Self, TryFromProtoError> {
        use proto_postgres_source_connection::proto_cast_type::Kind::*;
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
        use proto_postgres_source_connection::ProtoPostgresTableCast;
        let mut table_casts = Vec::with_capacity(self.table_casts.len());
        let mut table_cast_pos = Vec::with_capacity(self.table_casts.len());
        for (pos, table_cast_inner) in self.table_casts.iter() {
            let mut column_casts = Vec::with_capacity(table_casts.len());
            let mut column_cast_types = Vec::with_capacity(table_casts.len());

            for (table_cast_col_type, table_cast_col) in table_cast_inner {
                column_casts.push(table_cast_col.into_proto());
                column_cast_types.push(table_cast_col_type.into_proto());
            }

            table_casts.push(ProtoPostgresTableCast {
                column_casts,
                column_cast_types,
            });
            table_cast_pos.push(mz_ore::cast::usize_to_u64(*pos));
        }

        ProtoPostgresSourceConnection {
            connection: Some(self.connection.into_proto()),
            connection_id: Some(self.connection_id.into_proto()),
            publication: self.publication.clone(),
            details: Some(self.publication_details.into_proto()),
            table_casts,
            table_cast_pos,
        }
    }

    fn from_proto(proto: ProtoPostgresSourceConnection) -> Result<Self, TryFromProtoError> {
        // If we get the wrong number of table cast positions, we have to just
        // accept all of the table casts. This is somewhat harmless, as the
        // worst thing that happens is that we generate unused snapshots from
        // the upstream PG publication, and this will (hopefully) correct
        // itself on the next version upgrade.
        let table_cast_pos = if proto.table_casts.len() == proto.table_cast_pos.len() {
            proto.table_cast_pos
        } else {
            (1..proto.table_casts.len() + 1)
                .map(mz_ore::cast::usize_to_u64)
                .collect()
        };

        let mut table_casts = BTreeMap::new();
        for (pos, cast) in table_cast_pos
            .into_iter()
            .zip_eq(proto.table_casts.into_iter())
        {
            let mut column_casts = vec![];
            for (cast, cast_type) in cast
                .column_casts
                .into_iter()
                .zip_eq(cast.column_cast_types.into_iter())
            {
                column_casts.push((cast_type.into_rust()?, cast.into_rust()?));
            }

            table_casts.insert(mz_ore::cast::u64_to_usize(pos), column_casts);
        }

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
            table_casts,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Arbitrary)]
pub struct PostgresSourcePublicationDetails {
    #[proptest(strategy = "proptest::collection::vec(any::<PostgresTableDesc>(), 0..4)")]
    pub tables: Vec<PostgresTableDesc>,
    pub slot: String,
    /// The active timeline_id when this source was created
    /// The None value indicates an unknown timeline, to account for sources that existed
    /// prior to this field being introduced
    pub timeline_id: Option<u64>,
}

impl PostgresSourcePublicationDetails {
    pub fn output_idx_for_name(
        &self,
        name: &mz_sql_parser::ast::UnresolvedItemName,
    ) -> Option<usize> {
        // We do not validate the database qualification because we don't have
        // it stored anywhere on the source itself, so we assume that the
        // database is correctly validated during purification.
        self.tables
            .iter()
            .position(|t| {
                let inner = &name.0;
                t.namespace == inner[1].as_str() && t.name == inner[2].as_str()
            })
            .map(|idx| idx + 1)
    }
}

impl RustType<ProtoPostgresSourcePublicationDetails> for PostgresSourcePublicationDetails {
    fn into_proto(&self) -> ProtoPostgresSourcePublicationDetails {
        ProtoPostgresSourcePublicationDetails {
            tables: self.tables.iter().map(|t| t.into_proto()).collect(),
            slot: self.slot.clone(),
            timeline_id: self.timeline_id.clone(),
        }
    }

    fn from_proto(proto: ProtoPostgresSourcePublicationDetails) -> Result<Self, TryFromProtoError> {
        Ok(PostgresSourcePublicationDetails {
            tables: proto
                .tables
                .into_iter()
                .map(PostgresTableDesc::from_proto)
                .collect::<Result<_, _>>()?,
            slot: proto.slot,
            timeline_id: proto.timeline_id,
        })
    }
}

impl AlterCompatible for PostgresSourcePublicationDetails {
    fn alter_compatible(&self, id: mz_repr::GlobalId, other: &Self) -> Result<(), AlterError> {
        let PostgresSourcePublicationDetails {
            tables: _,
            slot,
            timeline_id,
        } = self;

        let compatibility_checks = [
            (slot == &other.slot, "slot"),
            (timeline_id == &other.timeline_id, "timeline_id"),
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
