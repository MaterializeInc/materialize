// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use std::error::Error;
use std::iter;

use catalog::Catalog;
use ore::collections::CollectionExt;
use repr::ScalarType;

#[test]
fn test_parameter_type_inference() -> Result<(), Box<dyn Error>> {
    let catalog = Catalog::open(None, iter::empty())?;
    let test_cases = vec![
        (
            "SELECT $1, $2, $3",
            vec![ScalarType::String, ScalarType::String, ScalarType::String],
        ),
        (
            "VALUES($1, $2, $3)",
            vec![ScalarType::String, ScalarType::String, ScalarType::String],
        ),
        (
            "SELECT 1 GROUP BY $1, $2, $3",
            vec![ScalarType::String, ScalarType::String, ScalarType::String],
        ),
        (
            "SELECT 1 ORDER BY $1, $2, $3",
            vec![ScalarType::String, ScalarType::String, ScalarType::String],
        ),
        (
            "SELECT ($1), (((($2))))",
            vec![ScalarType::String, ScalarType::String],
        ),
        ("SELECT $1::int", vec![ScalarType::Int64]),
        ("SELECT 1 WHERE $1", vec![ScalarType::Bool]),
        ("SELECT 1 HAVING $1", vec![ScalarType::Bool]),
        (
            "SELECT 1 FROM (VALUES (1)) a JOIN (VALUES (1)) b ON $1",
            vec![ScalarType::Bool],
        ),
        (
            "SELECT CASE WHEN $1 THEN 1 ELSE 0 END",
            vec![ScalarType::Bool],
        ),
        (
            "SELECT CASE WHEN true THEN $1 ELSE $2 END",
            vec![ScalarType::String, ScalarType::String],
        ),
        (
            "SELECT CASE WHEN true THEN $1 ELSE 1 END",
            vec![ScalarType::Int64],
        ),
        ("SELECT abs($1)", vec![ScalarType::Float64]),
        ("SELECT ascii($1)", vec![ScalarType::String]),
        (
            "SELECT coalesce($1, $2, $3)",
            vec![ScalarType::String, ScalarType::String, ScalarType::String],
        ),
        ("SELECT coalesce($1, 1)", vec![ScalarType::Int64]),
        (
            "SELECT substr($1, $2)",
            vec![ScalarType::String, ScalarType::Int64],
        ),
        (
            "SELECT substring($1, $2)",
            vec![ScalarType::String, ScalarType::Int64],
        ),
        (
            "SELECT $1 LIKE $2",
            vec![ScalarType::String, ScalarType::String],
        ),
        ("SELECT NOT $1", vec![ScalarType::Bool]),
        ("SELECT $1 AND $2", vec![ScalarType::Bool, ScalarType::Bool]),
        ("SELECT $1 OR $2", vec![ScalarType::Bool, ScalarType::Bool]),
        ("SELECT +$1", vec![ScalarType::Float64]),
        ("SELECT -$1", vec![ScalarType::Float64]),
        ("SELECT $1 < 1", vec![ScalarType::Int64]),
        (
            "SELECT $1 < $2",
            vec![ScalarType::String, ScalarType::String],
        ),
        ("SELECT $1 + 1", vec![ScalarType::Int64]),
        ("SELECT $1 + 1.0", vec![ScalarType::Decimal(2, 1)]),
        ("SELECT DATE '1970-01-01' + $1", vec![ScalarType::Interval]),
        (
            "SELECT TIMESTAMP '1970-01-01 00:00:00' + $1",
            vec![ScalarType::Interval],
        ),
        ("SELECT $1 + DATE '1970-01-01'", vec![ScalarType::Interval]),
        (
            "SELECT $1 + TIMESTAMP '1970-01-01 00:00:00'",
            vec![ScalarType::Interval],
        ),
        (
            "SELECT $1::int, $1 + $2",
            vec![ScalarType::Int64, ScalarType::Int64],
        ),
    ];
    for (sql, types) in test_cases {
        println!("> {}", sql);
        let stmt = sql::parse(sql.into())?.into_element();
        let (_desc, param_types) = sql::describe(&catalog, stmt)?;
        assert_eq!(param_types, types);
    }
    Ok(())
}
