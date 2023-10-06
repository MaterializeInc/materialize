// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use tokio::io;

use crate::codec::input_err;

/// The encoding format for a `mz_pgrepr::Value`.
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

impl TryFrom<i16> for Format {
    type Error = io::Error;

    fn try_from(value: i16) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Format::Text),
            1 => Ok(Format::Binary),
            n => Err(input_err(format!("unknown format code: {}", n))),
        }
    }
}

impl From<Format> for i8 {
    fn from(val: Format) -> Self {
        match val {
            Format::Text => 0,
            Format::Binary => 1,
        }
    }
}
