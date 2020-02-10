// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::convert::TryFrom;

/// The encoding format for a [`Value`](crate::Value).
///
/// PostgreSQL documentation: https://www.postgresql.org/docs/12/protocol-overview.html#PROTOCOL-FORMAT-CODES
#[derive(Copy, Clone, Debug)]
pub enum Format {
    /// Text encoding.
    ///
    /// From the PostgreSQL docs:
    ///
    /// > The text representation of values is whatever strings are produced and
    /// > accepted by the input/output conversion functions for the particular
    /// > data type. In the transmitted representation, there is no trailing
    /// > null character; the frontend must add one to received values if it
    /// > wants to process them as C strings. (The text format does not allow
    /// > embedded nulls, by the way.)
    Text = 0,
    /// Binary encoding.
    ///
    /// From the PostgreSQL docs:
    ///
    /// > Binary representations for integers use network byte order (most
    /// > significant byte first). For other data types consult the
    /// > documentation or source code to learn about the binary representation.
    /// > Keep in mind that binary representations for complex data types might
    /// > change across server versions; the text format is usually the more
    /// > portable choice.
    Binary = 1,
}

impl TryFrom<i16> for Format {
    type Error = failure::Error;

    fn try_from(n: i16) -> Result<Format, Self::Error> {
        match n {
            0 => Ok(Format::Text),
            1 => Ok(Format::Binary),
            _ => failure::bail!("invalid format code: {}", n),
        }
    }
}
