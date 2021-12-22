// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use repr::adt::system::{Oid, RegClass, RegProc, RegType};

use crate::EvalError;

sqlfunc!(
    #[sqlname = "oidtoi32"]
    #[preserves_uniqueness = true]
    fn cast_oid_to_int32(a: Oid) -> i32 {
        a.0
    }
);

sqlfunc!(
    #[sqlname = "oidtoi64"]
    #[preserves_uniqueness = true]
    fn cast_oid_to_int64(a: Oid) -> i64 {
        i64::from(a.0)
    }
);

sqlfunc!(
    #[sqlname = "oidtoregclass"]
    #[preserves_uniqueness = true]
    fn cast_oid_to_reg_class(a: Oid) -> RegClass {
        RegClass(a.0)
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

sqlfunc!(
    fn pg_get_constraintdef(_oid: Option<Oid>) -> Result<String, EvalError> {
        Err(EvalError::Unsupported {
            feature: "pg_get_constraintdef".to_string(),
            issue_no: Some(9483),
        })
    }
);
