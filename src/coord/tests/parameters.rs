// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::error::Error;

use tempfile::NamedTempFile;

use mz_coord::catalog::Catalog;
use mz_ore::collections::CollectionExt;
use mz_ore::now::NOW_ZERO;
use mz_pgrepr::Type;
use mz_sql::plan::PlanContext;

#[tokio::test]
async fn test_parameter_type_inference() -> Result<(), Box<dyn Error>> {
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
        ("SELECT $1::pg_catalog.int4", vec![Type::Int4]),
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
        ("SELECT pg_catalog.abs($1)", vec![Type::Float8]),
        ("SELECT pg_catalog.ascii($1)", vec![Type::Text]),
        (
            "SELECT coalesce($1, $2, $3)",
            vec![Type::Text, Type::Text, Type::Text],
        ),
        ("SELECT coalesce($1, 1)", vec![Type::Int4]),
        (
            "SELECT pg_catalog.substr($1, $2)",
            vec![Type::Text, Type::Int8],
        ),
        (
            "SELECT pg_catalog.substring($1, $2)",
            vec![Type::Text, Type::Int8],
        ),
        ("SELECT $1 LIKE $2", vec![Type::Text, Type::Text]),
        ("SELECT NOT $1", vec![Type::Bool]),
        ("SELECT $1 AND $2", vec![Type::Bool, Type::Bool]),
        ("SELECT $1 OR $2", vec![Type::Bool, Type::Bool]),
        ("SELECT +$1", vec![Type::Float8]),
        ("SELECT $1 < 1", vec![Type::Int4]),
        ("SELECT $1 < $2", vec![Type::Text, Type::Text]),
        ("SELECT $1 + 1", vec![Type::Int4]),
        ("SELECT $1 + 1.0", vec![Type::Numeric]),
        (
            "SELECT '1970-01-01 00:00:00'::pg_catalog.timestamp + $1",
            vec![Type::Interval],
        ),
        (
            "SELECT $1 + '1970-01-01 00:00:00'::pg_catalog.timestamp",
            vec![Type::Interval],
        ),
        (
            "SELECT $1::pg_catalog.int4, $1 + $2",
            vec![Type::Int4, Type::Int4],
        ),
        (
            "SELECT '[0, 1, 2]'::pg_catalog.jsonb - $1",
            vec![Type::Text],
        ),
    ];

    let catalog_file = NamedTempFile::new()?;
    let catalog = Catalog::open_debug(catalog_file.path(), NOW_ZERO.clone()).await?;
    let catalog = catalog.for_system_session();
    for (sql, types) in test_cases {
        let stmt = mz_sql::parse::parse(sql)?.into_element();
        let desc = mz_sql::plan::describe(&PlanContext::zero(), &catalog, stmt, &[])?;
        assert_eq!(desc.param_types, types);
    }
    Ok(())
}
