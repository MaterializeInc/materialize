// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/// The encoding format for a [`Value`](crate::Value).
///
/// See the ["Formats and Format Codes"][pgdocs] section of the PostgreSQL
/// protocol documentation for details on the available formats.
///
/// [pgdocs]:
/// https://www.postgresql.org/docs/current/protocol-overview.html#PROTOCOL-FORMAT-CODES
#[derive(Copy, Clone, Debug)]
pub enum Format {
    /// Text encoding.
    Text,
    /// Binary encoding.
    Binary,
}
