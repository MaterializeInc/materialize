// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use repr::strconv;

sqlfunc!(
    #[sqlname = "bytestostr"]
    #[preserves_uniqueness = true]
    fn cast_bytes_to_string(a: &'a [u8]) -> String {
        let mut buf = String::new();
        strconv::format_bytes(&mut buf, a);
        buf
    }
);
