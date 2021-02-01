// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use honggfuzz::fuzz;
use sql_parser::parser::parse_statements;

fn main() {
    loop {
        fuzz!(|data: String| {
            let _ = parse_statements(&data);
        });
    }
}
