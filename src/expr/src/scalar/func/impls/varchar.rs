// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_repr::adt::varchar::VarChar;

// This function simply allows the expression of changing a's type from varchar to string
sqlfunc!(
    #[sqlname = "varchar_to_text"]
    #[preserves_uniqueness = true]
    fn cast_var_char_to_string<'a>(a: VarChar<&'a str>) -> &'a str {
        a.0
    }
);
