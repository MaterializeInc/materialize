// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Wire-format dispatch for Avro encoding/decoding on Kafka topics.
//!
//! [`WireFormat`] picks how schema identifiers are framed inside a Kafka
//! record payload. Both source decode and sink encode share this enum. Sinks
//! always require a registry; that invariant is upheld by construction, since
//! the only sink-Avro constructor produces `Confluent { registry: Some(_) }`
//! and other variants are never built for a sink.
//!
//! Variants:
//!
//! * [`WireFormat::None`] — no framing. The single schema carried by the
//!   surrounding `AvroEncoding` applies to every record. Source-only.
//! * [`WireFormat::Confluent`] — Confluent Schema Registry framing
//!   (magic byte + i32 schema id). `registry` is `None` for sources that
//!   ingest Confluent-framed bytes without an attached CSR connection
//!   (the schema id is read but the schema is not fetched).
//! * [`WireFormat::Glue`] — AWS Glue Schema Registry framing
//!   (magic byte + UUID schema-version id). Constructable from SQL on the
//!   source side; sinks do not yet support Glue and remain Confluent-only.

use mz_repr::GlobalId;
use serde::{Deserialize, Serialize};

use crate::AlterCompatible;
use crate::connections::inline::{
    ConnectionAccess, ConnectionResolver, InlinedConnection, IntoInlineConnection,
    ReferencedConnection,
};
use crate::controller::AlterError;

/// How schema identifiers are framed in a Kafka record payload.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum WireFormat<C: ConnectionAccess = InlinedConnection> {
    /// No framing; the surrounding `AvroEncoding`'s single schema applies
    /// to every record. Source-only — never constructed for a sink.
    None,
    /// Confluent Schema Registry framing (magic byte + i32 schema id).
    Confluent {
        /// CSR connection used to fetch writer schemas (sources) or
        /// register schemas (sinks). `None` is only valid on the source
        /// side, when the user supplied `CONFLUENT WIRE FORMAT` without a
        /// registry.
        registry: Option<C::Csr>,
    },
    /// AWS Glue Schema Registry framing (magic byte + UUID schema-version id).
    Glue {
        /// Glue Schema Registry connection. `None` is reserved for future
        /// source configurations that consume Glue-framed bytes without
        /// fetching writer schemas.
        registry: Option<C::GlueSchemaRegistry>,
    },
}

impl<R: ConnectionResolver> IntoInlineConnection<WireFormat, R>
    for WireFormat<ReferencedConnection>
{
    fn into_inline_connection(self, r: R) -> WireFormat {
        match self {
            WireFormat::None => WireFormat::None,
            WireFormat::Confluent { registry } => WireFormat::Confluent {
                registry: registry.map(|c| r.resolve_connection(c).unwrap_csr()),
            },
            WireFormat::Glue { registry } => WireFormat::Glue {
                registry: registry.map(|c| r.resolve_connection(c).unwrap_glue_schema_registry()),
            },
        }
    }
}

impl<C: ConnectionAccess> AlterCompatible for WireFormat<C> {
    fn alter_compatible(&self, id: GlobalId, other: &Self) -> Result<(), AlterError> {
        if self == other {
            return Ok(());
        }
        let compatible = match (self, other) {
            (WireFormat::None, WireFormat::None) => true,
            (
                WireFormat::Confluent { registry: Some(s) },
                WireFormat::Confluent { registry: Some(o) },
            ) => s.alter_compatible(id, o).is_ok(),
            (
                WireFormat::Confluent { registry: None },
                WireFormat::Confluent { registry: None },
            ) => true,
            (WireFormat::Glue { registry: Some(s) }, WireFormat::Glue { registry: Some(o) }) => {
                s.alter_compatible(id, o).is_ok()
            }
            (WireFormat::Glue { registry: None }, WireFormat::Glue { registry: None }) => true,
            _ => false,
        };
        if !compatible {
            tracing::warn!(
                "WireFormat incompatible:\nself:\n{:#?}\n\nother\n{:#?}",
                self,
                other
            );
            return Err(AlterError { id });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[mz_ore::test]
    fn wire_format_alter_compatible_cross_variant_rejected() {
        let id = GlobalId::User(1);
        let none = WireFormat::<InlinedConnection>::None;
        let conf_none = WireFormat::<InlinedConnection>::Confluent { registry: None };
        let glue_none = WireFormat::<InlinedConnection>::Glue { registry: None };

        // Same-variant, no registry on either side: compatible.
        assert!(none.alter_compatible(id, &none).is_ok());
        assert!(conf_none.alter_compatible(id, &conf_none).is_ok());
        assert!(glue_none.alter_compatible(id, &glue_none).is_ok());

        // Cross-variant: rejected.
        assert!(none.alter_compatible(id, &conf_none).is_err());
        assert!(none.alter_compatible(id, &glue_none).is_err());
        assert!(conf_none.alter_compatible(id, &glue_none).is_err());
    }
}
