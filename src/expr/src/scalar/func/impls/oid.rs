// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use repr::adt::system::{Oid, RegProc, RegType};

sqlfunc!(
    #[sqlname = "oidtoi32"]
    #[preserves_uniqueness = true]
    fn cast_oid_to_int32(a: Oid) -> i32 {
        a.0
    }
);

sqlfunc!(
    #[sqlname = "oidtoregproc"]
    #[preserves_uniqueness = true]
    fn cast_oid_to_reg_proc(a: Oid) -> RegProc {
        RegProc(a.0)
    }
);

sqlfunc!(
    #[sqlname = "oidtoregtype"]
    #[preserves_uniqueness = true]
    fn cast_oid_to_reg_type(a: Oid) -> RegType {
        RegType(a.0)
    }
);
