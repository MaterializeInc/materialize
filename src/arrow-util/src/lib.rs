// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use arrow::array::{make_array, ArrayRef};
use arrow::buffer::NullBuffer;

pub mod builder;
pub mod reader;

/// Merge the provided null buffer with the existing array's null buffer, if any.
pub fn mask_nulls(column: &ArrayRef, null_mask: Option<&NullBuffer>) -> ArrayRef {
    if null_mask.is_none() {
        Arc::clone(column)
    } else {
        let nulls = NullBuffer::union(null_mask, column.nulls());
        let data = column
            .to_data()
            .into_builder()
            .nulls(nulls)
            .build()
            .expect("changed only null mask");
        make_array(data)
    }
}
