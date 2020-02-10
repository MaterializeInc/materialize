// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::error::Error;

use catalog::{BincodeSerializer, Catalog};
use ore::collections::CollectionExt;
use pgrepr::Type;
use sql::Session;

#[test]
fn test_parameter_type_inference() -> Result<(), Box<dyn Error>> {
    let catalog = Catalog::open::<BincodeSerializer, _>(None, |_| ())?;
    let session = Session::default();
    let test_cases = vec![
        (
            "SELECT $1, $2, $3",
            vec![Type::Text, Type::Text, Type::Text],
        ),
        (
            "VALUES($1, $2, $3)",
            vec![Type::Text, Type::Text, Type::Text],
        ),
        (
            "SELECT 1 GROUP BY $1, $2, $3",
            vec![Type::Text, Type::Text, Type::Text],
        ),
        (
            "SELECT 1 ORDER BY $1, $2, $3",
            vec![Type::Text, Type::Text, Type::Text],
        ),
        ("SELECT ($1), (((($2))))", vec![Type::Text, Type::Text]),
        ("SELECT $1::int", vec![Type::Int4]),
        ("SELECT 1 WHERE $1", vec![Type::Bool]),
        ("SELECT 1 HAVING $1", vec![Type::Bool]),
        (
            "SELECT 1 FROM (VALUES (1)) a JOIN (VALUES (1)) b ON $1",
            vec![Type::Bool],
        ),
        ("SELECT CASE WHEN $1 THEN 1 ELSE 0 END", vec![Type::Bool]),
        (
            "SELECT CASE WHEN true THEN $1 ELSE $2 END",
            vec![Type::Text, Type::Text],
        ),
        ("SELECT CASE WHEN true THEN $1 ELSE 1 END", vec![Type::Int4]),
        ("SELECT abs($1)", vec![Type::Float8]),
        ("SELECT ascii($1)", vec![Type::Text]),
        (
            "SELECT coalesce($1, $2, $3)",
            vec![Type::Text, Type::Text, Type::Text],
        ),
        ("SELECT coalesce($1, 1)", vec![Type::Int4]),
        ("SELECT substr($1, $2)", vec![Type::Text, Type::Int8]),
        ("SELECT substring($1, $2)", vec![Type::Text, Type::Int8]),
        ("SELECT $1 LIKE $2", vec![Type::Text, Type::Text]),
        ("SELECT NOT $1", vec![Type::Bool]),
        ("SELECT $1 AND $2", vec![Type::Bool, Type::Bool]),
        ("SELECT $1 OR $2", vec![Type::Bool, Type::Bool]),
        ("SELECT +$1", vec![Type::Float8]),
        ("SELECT -$1", vec![Type::Float8]),
        ("SELECT $1 < 1", vec![Type::Int4]),
        ("SELECT $1 < $2", vec![Type::Text, Type::Text]),
        ("SELECT $1 + 1", vec![Type::Int4]),
        ("SELECT $1 + 1.0", vec![Type::Numeric]),
        ("SELECT DATE '1970-01-01' + $1", vec![Type::Interval]),
        (
            "SELECT TIMESTAMP '1970-01-01 00:00:00' + $1",
            vec![Type::Interval],
        ),
        ("SELECT $1 + DATE '1970-01-01'", vec![Type::Interval]),
        (
            "SELECT $1 + TIMESTAMP '1970-01-01 00:00:00'",
            vec![Type::Interval],
        ),
        ("SELECT $1::int, $1 + $2", vec![Type::Int4, Type::Int4]),
        ("SELECT '[0, 1, 2]'::jsonb - $1", vec![Type::Text]),
    ];
    for (sql, types) in test_cases {
        println!("> {}", sql);
        let stmt = sql::parse(sql.into())?.into_element();
        let (_desc, param_types) = sql::describe(&catalog, &session, stmt)?;
        assert_eq!(param_types, types);
    }
    Ok(())
}
