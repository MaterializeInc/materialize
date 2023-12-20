// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Types related to mysql sources

use mz_proto::{IntoRustIfSome, RustType, TryFromProtoError};
use mz_repr::{ColumnType, GlobalId, RelationDesc, ScalarType};
use once_cell::sync::Lazy;
use proptest::prelude::{any, Arbitrary, BoxedStrategy, Strategy};
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};

use crate::connections::inline::{
    ConnectionAccess, ConnectionResolver, InlinedConnection, IntoInlineConnection,
    ReferencedConnection,
};
use crate::sources::SourceConnection;

include!(concat!(
    env!("OUT_DIR"),
    "/mz_storage_types.sources.mysql.rs"
));

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MySqlSourceConnection<C: ConnectionAccess = InlinedConnection> {
    pub connection_id: GlobalId,
    pub connection: C::MySql,
    pub details: MySqlSourceDetails,
}

impl<R: ConnectionResolver> IntoInlineConnection<MySqlSourceConnection, R>
    for MySqlSourceConnection<ReferencedConnection>
{
    fn into_inline_connection(self, r: R) -> MySqlSourceConnection {
        let MySqlSourceConnection {
            connection_id,
            connection,
            details,
        } = self;

        MySqlSourceConnection {
            connection_id,
            connection: r.resolve_connection(connection).unwrap_mysql(),
            details,
        }
    }
}

impl<C: ConnectionAccess> Arbitrary for MySqlSourceConnection<C> {
    type Strategy = BoxedStrategy<Self>;
    type Parameters = ();

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        (
            any::<C::MySql>(),
            any::<GlobalId>(),
            any::<MySqlSourceDetails>(),
        )
            .prop_map(|(connection, connection_id, details)| Self {
                connection,
                connection_id,
                details,
            })
            .boxed()
    }
}

pub static MYSQL_PROGRESS_DESC: Lazy<RelationDesc> = Lazy::new(|| {
    RelationDesc::empty()
        // TODO(petrosagg): UUIDs in ranges are not supported and additionally the
        // Partitioned timestamp type forces you to deal with open/closed ranges which are
        // difficult to represent manually. In this case they are unecessary though because
        // we could just have two columns `source_id_start`, `source_id_end` that are always
        // inclusive and have the convention that a range of a single element corresponds to
        // an actual source_id being ingested.
        // use
        // .with_column(
        //     "source_id",
        //     ScalarType::Range {
        //         element_type: Box::new(ScalarType::Uuid),
        //     }
        //     .nullable(false),
        // )
        .with_column("transaction_id", ScalarType::UInt64.nullable(false))
});

impl<C: ConnectionAccess> SourceConnection for MySqlSourceConnection<C> {
    fn name(&self) -> &'static str {
        "mysql"
    }

    fn upstream_name(&self) -> Option<&str> {
        None
    }

    fn timestamp_desc(&self) -> RelationDesc {
        MYSQL_PROGRESS_DESC.clone()
    }

    fn connection_id(&self) -> Option<GlobalId> {
        Some(self.connection_id)
    }

    fn metadata_columns(&self) -> Vec<(&str, ColumnType)> {
        vec![]
    }
}

// TODO(roshan): implement alter comaptibility logic
impl<C: ConnectionAccess> crate::AlterCompatible for MySqlSourceConnection<C> {}

impl RustType<ProtoMySqlSourceConnection> for MySqlSourceConnection {
    fn into_proto(&self) -> ProtoMySqlSourceConnection {
        ProtoMySqlSourceConnection {
            connection: Some(self.connection.into_proto()),
            connection_id: Some(self.connection_id.into_proto()),
            details: Some(self.details.into_proto()),
        }
    }

    fn from_proto(proto: ProtoMySqlSourceConnection) -> Result<Self, TryFromProtoError> {
        Ok(MySqlSourceConnection {
            connection: proto
                .connection
                .into_rust_if_some("ProtoMySqlSourceConnection::connection")?,
            connection_id: proto
                .connection_id
                .into_rust_if_some("ProtoMySqlSourceConnection::connection_id")?,
            details: proto
                .details
                .into_rust_if_some("ProtoMySqlSourceConnection::details")?,
        })
    }
}

#[derive(Arbitrary, Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MySqlSourceDetails {}

impl RustType<ProtoMySqlSourceDetails> for MySqlSourceDetails {
    fn into_proto(&self) -> ProtoMySqlSourceDetails {
        ProtoMySqlSourceDetails {}
    }

    fn from_proto(_proto: ProtoMySqlSourceDetails) -> Result<Self, TryFromProtoError> {
        Ok(MySqlSourceDetails {})
    }
}
