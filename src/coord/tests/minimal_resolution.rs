// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::error::Error;

use tempfile::NamedTempFile;

use coord::catalog::builtin::{MZ_CATALOG_SCHEMA, PG_CATALOG_SCHEMA};
use coord::session::Session;
use sql::catalog::Catalog;
use sql::names::{DatabaseSpecifier, FullName, PartialName};

/// System sessions have an empty `search_path` so it's necessary to
/// schema-qualify all referenced items.
///
/// Dummy (and ostensibly client) sessions contain system schemas in their
/// search paths, so do not require schema qualification on system objects such
/// as types.
#[test]
fn test_minimal_qualification() -> Result<(), Box<dyn Error>> {
    struct TestCase {
        input: FullName,
        system_output: PartialName,
        normal_output: PartialName,
    }

    let test_cases = vec![
        TestCase {
            input: FullName {
                database: DatabaseSpecifier::Ambient,
                schema: PG_CATALOG_SCHEMA.to_string(),
                item: "numeric".to_string(),
            },
            system_output: PartialName {
                database: None,
                schema: Some(PG_CATALOG_SCHEMA.to_string()),
                item: "numeric".to_string(),
            },
            normal_output: PartialName {
                database: None,
                schema: None,
                item: "numeric".to_string(),
            },
        },
        TestCase {
            input: FullName {
                database: DatabaseSpecifier::Ambient,
                schema: MZ_CATALOG_SCHEMA.to_string(),
                item: "mz_array_types".to_string(),
            },
            system_output: PartialName {
                database: None,
                schema: Some(MZ_CATALOG_SCHEMA.to_string()),
                item: "mz_array_types".to_string(),
            },
            normal_output: PartialName {
                database: None,
                schema: None,
                item: "mz_array_types".to_string(),
            },
        },
    ];

    let catalog_file = NamedTempFile::new()?;
    let catalog = coord::catalog::Catalog::open_debug(catalog_file.path())?;
    for tc in test_cases {
        assert_eq!(
            catalog
                .for_system_session()
                .minimal_qualification(&tc.input),
            tc.system_output
        );
        assert_eq!(
            catalog
                .for_session(&Session::dummy())
                .minimal_qualification(&tc.input),
            tc.normal_output
        );
    }
    Ok(())
}
