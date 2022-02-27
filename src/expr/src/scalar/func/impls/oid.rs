// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_pgrepr::Type;
use mz_repr::adt::system::{Oid, RegClass, RegProc, RegType};
use mz_repr::strconv;

use crate::EvalError;

sqlfunc!(
    #[sqlname = "oidtostring"]
    #[preserves_uniqueness = true]
    fn cast_oid_to_string(a: Oid) -> String {
        let mut buf = String::new();
        strconv::format_oid(&mut buf, a.0);
        buf
    }
);

sqlfunc!(
    #[sqlname = "oidtoi32"]
    #[preserves_uniqueness = true]
    fn cast_oid_to_int32(a: Oid) -> i32 {
        // For historical reasons in PostgreSQL, the bytes of the `u32` are
        // reinterpreted as an `i32` without bounds checks, so very large
        // positive OIDs become negative `i32`s.
        //
        // Do not use this as a model for behavior in other contexts. OIDs
        // should not in general be thought of as freely convertible to `i32`s.
        i32::from_ne_bytes(a.0.to_ne_bytes())
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

sqlfunc!(
    fn mz_type_name<'a>(oid: Oid) -> Option<String> {
        if let Some(t) = Type::from_oid(oid.0) {
            Some(t.name().to_string())
        } else {
            None
        }
    }
);
