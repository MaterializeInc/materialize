// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::EvalError;
use repr::Datum;
use std::convert::TryFrom;

sqlfunc!(
    #[sqlname = "isnull"]
    #[propagates_nulls = false]
    #[introduces_nulls = false]
    #[preserves_uniqueness = false]
    fn is_null(a: Datum<'_>) -> Result<Option<bool>, EvalError> {
        Ok(Some(a == Datum::Null))
    }
);

// TODO: Once issue #7581 is fixed, we can remove `IsTrue` and `IsFalse` and
// replace them with `BinaryFunc::eq`.  We can't do this yet because that
// function propagates NULLs which we do not want here.
sqlfunc!(
    #[sqlname = "istrue"]
    #[propagates_nulls = false]
    #[introduces_nulls = false]
    #[preserves_uniqueness = false]
    fn is_true(a: Datum<'_>) -> Result<Option<bool>, EvalError> {
        Ok(Some(a == Datum::True))
    }
);

sqlfunc!(
    #[sqlname = "isfalse"]
    #[propagates_nulls = false]
    #[introduces_nulls = false]
    #[preserves_uniqueness = false]
    fn is_false(a: Datum<'_>) -> Result<Option<bool>, EvalError> {
        Ok(Some(a == Datum::False))
    }
);

sqlfunc!(
    #[sqlname = "pg_column_size"]
    #[propagates_nulls = true]
    #[introduces_nulls = false]
    #[preserves_uniqueness = false]
    fn pg_column_size(a: Datum<'_>) -> Result<Option<i32>, EvalError> {
        match a {
            Datum::Null => Ok(None),
            d => {
                let sz = repr::datum_size(&d);
                i32::try_from(sz)
                    .map(Some)
                    .or(Err(EvalError::Int32OutOfRange))
            }
        }
    }
);

sqlfunc!(
    #[sqlname = "mz_row_size"]
    #[propagates_nulls = true]
    #[introduces_nulls = false]
    #[preserves_uniqueness = false]
    // Return the number of bytes this Record (List) datum would use if packed as a Row.
    fn mz_row_size(a: Datum<'_>) -> Result<Option<i32>, EvalError> {
        match a {
            Datum::Null => Ok(None),
            d => {
                let sz = repr::row_size(d.unwrap_list().iter());
                i32::try_from(sz)
                    .map(Some)
                    .or(Err(EvalError::Int32OutOfRange))
            }
        }
    }
);
