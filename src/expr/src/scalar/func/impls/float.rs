// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

sqlfunc!(
    #[sqlname = "-"]
    fn neg_float32(a: f32) -> f32 {
        -a
    }
);

sqlfunc!(
    #[sqlname = "-"]
    fn neg_float64(a: f64) -> f64 {
        -a
    }
);

sqlfunc!(
    #[sqlname = "abs"]
    fn abs_float32(a: f32) -> f32 {
        a.abs()
    }
);

sqlfunc!(
    #[sqlname = "abs"]
    fn abs_float64(a: f64) -> f64 {
        a.abs()
    }
);

sqlfunc!(
    #[sqlname = "f32tof64"]
    fn cast_float32_to_float64(a: f32) -> f64 {
        a.into()
    }
);
