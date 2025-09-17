// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Contains traits and types to support inlining connection details.
//!
//! Because we ultimately want to support the ability to alter the details of a
//! connection, we cannot simply inline their state into structs--we instead
//! need a means of understanding which connection we're referring to and inline
//! its current state when sending the struct off to be used to handle a
//! connection to an external service.

use std::fmt::Debug;
use std::hash::Hash;

use mz_repr::CatalogItemId;
use serde::{Deserialize, Serialize};

use crate::AlterCompatible;

use super::Connection;

/// Permits any struct to take a [`CatalogItemId`] into an inlined connection.
///
/// It is safe to assume that if this `id` does not refer to a catalog
/// connection, this function will panic.
pub trait ConnectionResolver {
    fn resolve_connection(&self, id: CatalogItemId) -> Connection<InlinedConnection>;
}

impl<R: ConnectionResolver + ?Sized> ConnectionResolver for &R {
    fn resolve_connection(&self, id: CatalogItemId) -> Connection<InlinedConnection> {
        (*self).resolve_connection(id)
    }
}

/// Takes ~`T<ReferencedConnection>` to ~`T<InlinedConnection>` by recursively
/// inlining connections and resolving any referenced connections into their
/// inlined version.
///
/// Note that this trait is overly generic.
// TODO: this trait could be derived for types that are generic over a type that
// implements `ConnectionAccess`, e.g. `derive(IntoInlineConnection)`.
pub trait IntoInlineConnection<T, R: ConnectionResolver + ?Sized> {
    fn into_inline_connection(self, connection_resolver: R) -> T;
}

/// Expresses how a struct/enum can access details about any connections it
/// uses. Meant to be used as a type constraint on structs that use connections.
pub trait ConnectionAccess: Clone + Debug + Eq + PartialEq + Serialize + 'static {
    type Kafka: Clone
        + Debug
        + Eq
        + PartialEq
        + Hash
        + Serialize
        + for<'a> Deserialize<'a>
        + AlterCompatible;
    type Pg: Clone
        + Debug
        + Eq
        + PartialEq
        + Hash
        + Serialize
        + for<'a> Deserialize<'a>
        + AlterCompatible;
    type Aws: Clone
        + Debug
        + Eq
        + PartialEq
        + Hash
        + Serialize
        + for<'a> Deserialize<'a>
        + AlterCompatible;
    type Ssh: Clone
        + Debug
        + Eq
        + PartialEq
        + Hash
        + Serialize
        + for<'a> Deserialize<'a>
        + AlterCompatible;
    type Csr: Clone
        + Debug
        + Eq
        + PartialEq
        + Hash
        + Serialize
        + for<'a> Deserialize<'a>
        + AlterCompatible;
    type MySql: Clone
        + Debug
        + Eq
        + PartialEq
        + Hash
        + Serialize
        + for<'a> Deserialize<'a>
        + AlterCompatible;
    type SqlServer: Clone
        + Debug
        + Eq
        + PartialEq
        + Hash
        + Serialize
        + for<'a> Deserialize<'a>
        + AlterCompatible;
    type IcebergCatalog: Clone
        + Debug
        + Eq
        + PartialEq
        + Hash
        + Serialize
        + for<'a> Deserialize<'a>
        + AlterCompatible;
}

/// Expresses that the struct contains references to connections. Use a
/// combination of [`IntoInlineConnection`] and [`ConnectionResolver`] to take
/// this into [`InlinedConnection`].
#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct ReferencedConnection;

impl ConnectionAccess for ReferencedConnection {
    type Kafka = CatalogItemId;
    type Pg = CatalogItemId;
    type Aws = CatalogItemId;
    type Ssh = CatalogItemId;
    type Csr = CatalogItemId;
    type MySql = CatalogItemId;
    type SqlServer = CatalogItemId;
    type IcebergCatalog = CatalogItemId;
}

/// Expresses that the struct contains an inlined definition of a connection.
#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct InlinedConnection;

impl ConnectionAccess for InlinedConnection {
    type Kafka = super::KafkaConnection;
    type Pg = super::PostgresConnection;
    type Aws = super::aws::AwsConnection;
    type Ssh = super::SshConnection;
    type Csr = super::CsrConnection;
    type MySql = super::MySqlConnection;
    type SqlServer = super::SqlServerConnectionDetails;
    type IcebergCatalog = super::IcebergCatalogConnection;
}
