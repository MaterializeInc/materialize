// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Encoding and decoding for PostgreSQL COPY data format
//!
//! # Useful references
//!
//!   * [PostgreSQL COPY](https://www.postgresql.org/docs/14/sql-copy.html)

mod copy;

pub use copy::{decode_copy_format, encode_copy_row_binary, encode_copy_row_text};
pub use copy::{CopyCsvFormatParams, CopyFormatParams, CopyTextFormatParams, CopyTextFormatParser};
