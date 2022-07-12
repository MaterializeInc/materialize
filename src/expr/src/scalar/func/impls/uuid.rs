// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use uuid::Uuid;

use mz_repr::strconv;

sqlfunc!(
    #[sqlname = "uuid_to_text"]
    #[preserves_uniqueness = true]
    fn cast_uuid_to_string(u: Uuid) -> String {
        let mut buf = String::with_capacity(36);
        strconv::format_uuid(&mut buf, u);
        buf
    }
);
