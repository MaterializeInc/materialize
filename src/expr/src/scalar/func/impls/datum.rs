// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_repr::{Datum, DatumList};

use crate::EvalError;

sqlfunc!(
    #[sqlname = "isnull"]
    #[is_monotone = true]
    fn is_null<'a>(a: Datum<'a>) -> bool {
        a.is_null()
    }
);

sqlfunc!(
    #[sqlname = "istrue"]
    fn is_true<'a>(a: Datum<'a>) -> bool {
        a == Datum::True
    }
);

sqlfunc!(
    #[sqlname = "isfalse"]
    fn is_false<'a>(a: Datum<'a>) -> bool {
        a == Datum::False
    }
);

sqlfunc!(
    fn pg_column_size<'a>(a: Datum<'a>) -> Result<Option<i32>, EvalError> {
        match a {
            Datum::Null => Ok(None),
            datum => {
                let sz = mz_repr::datum_size(&datum);
                i32::try_from(sz)
                    .map(Some)
                    .or(Err(EvalError::Int32OutOfRange))
            }
        }
    }
);

sqlfunc!(
    fn mz_row_size<'a>(a: DatumList<'a>) -> Result<i32, EvalError> {
        let sz = mz_repr::row_size(a.iter());
        i32::try_from(sz).or(Err(EvalError::Int32OutOfRange))
    }
);
