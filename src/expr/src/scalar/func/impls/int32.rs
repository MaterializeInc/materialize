// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::EvalError;
use repr::{
    adt::system::{Oid, RegProc, RegType},
    strconv,
};

sqlfunc!(
    #[sqlname = "-"]
    fn neg_int32(a: i32) -> i32 {
        -a
    }
);

sqlfunc!(
    #[sqlname = "~"]
    fn bit_not_int32(a: i32) -> i32 {
        !a
    }
);

sqlfunc!(
    #[sqlname = "abs"]
    fn abs_int32(a: i32) -> i32 {
        a.abs()
    }
);

sqlfunc!(
    #[sqlname = "i32tobool"]
    fn cast_int32_to_bool(a: i32) -> bool {
        a != 0
    }
);

sqlfunc!(
    #[sqlname = "i32tof32"]
    fn cast_int32_to_float32(a: i32) -> f32 {
        a as f32
    }
);

sqlfunc!(
    #[sqlname = "i32tof64"]
    #[preserves_uniqueness = true]
    fn cast_int32_to_float64(a: i32) -> f64 {
        f64::from(a)
    }
);

sqlfunc!(
    #[sqlname = "i32toi16"]
    fn cast_int32_to_int16(a: i32) -> Result<i16, EvalError> {
        i16::try_from(a).or(Err(EvalError::Int16OutOfRange))
    }
);

sqlfunc!(
    #[sqlname = "i32toi64"]
    #[preserves_uniqueness = true]
    fn cast_int32_to_int64(a: i32) -> i64 {
        i64::from(a)
    }
);

sqlfunc!(
    #[sqlname = "i32tostr"]
    #[preserves_uniqueness = true]
    fn cast_int32_to_string(a: i32) -> String {
        let mut buf = String::new();
        strconv::format_int32(&mut buf, a);
        buf
    }
);

sqlfunc!(
    #[sqlname = "i32tooid"]
    #[preserves_uniqueness = true]
    fn cast_int32_to_oid(a: i32) -> Oid {
        Oid(a)
    }
);

sqlfunc!(
    #[sqlname = "i32toregproc"]
    #[preserves_uniqueness = true]
    fn cast_int32_to_reg_proc(a: i32) -> RegProc {
        RegProc(a)
    }
);

sqlfunc!(
    #[sqlname = "i32toregtype"]
    #[preserves_uniqueness = true]
    fn cast_int32_to_reg_type(a: i32) -> RegType {
        RegType(a)
    }
);
