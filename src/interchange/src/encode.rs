// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use repr::Row;

pub trait Encode {
    fn get_format_name(&self) -> &str;

    fn encode_key_unchecked(&self, row: Row) -> Vec<u8>;

    fn encode_value_unchecked(&self, row: Row) -> Vec<u8>;
}
