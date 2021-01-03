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

use coord::{
    catalog::builtin::{MZ_CATALOG_SCHEMA, PG_CATALOG_SCHEMA},
    session::Session,
};
use sql::{
    catalog::Catalog,
    names::{DatabaseSpecifier, FullName, PartialName},
};

/// System sessions have an empty `search_path` so it's necessary to
/// schema-qualify all referenced items.
#[test]
fn test_system_session_resolution() -> Result<(), Box<dyn Error>> {
    let test_cases = vec![
        (
            FullName {
                database: DatabaseSpecifier::Ambient,
                schema: PG_CATALOG_SCHEMA.to_string(),
                item: "numeric".to_string(),
            },
            PartialName {
                database: None,
                schema: Some(PG_CATALOG_SCHEMA.to_string()),
                item: "numeric".to_string(),
            },
        ),
        (
            FullName {
                database: DatabaseSpecifier::Ambient,
                schema: MZ_CATALOG_SCHEMA.to_string(),
                item: "mz_array_types".to_string(),
            },
            PartialName {
                database: None,
                schema: Some(MZ_CATALOG_SCHEMA.to_string()),
                item: "mz_array_types".to_string(),
            },
        ),
    ];

    let catalog_file = NamedTempFile::new()?;
    let catalog = coord::catalog::Catalog::open_debug(catalog_file.path())?;
    let catalog = catalog.for_system_session();
    for (full_name, partial_name) in test_cases {
        assert_eq!(catalog.minimal_qualification(&full_name), partial_name);
    }
    Ok(())
}

#[test]
/// Dummy (and ostensibly client) sessions contain system schemas in their
/// search paths, so do not require schema qualification on system objects such
/// as types.
fn test_dummy_session_resolution() -> Result<(), Box<dyn Error>> {
    let test_cases = vec![
        (
            FullName {
                database: DatabaseSpecifier::Ambient,
                schema: PG_CATALOG_SCHEMA.to_string(),
                item: "numeric".to_string(),
            },
            PartialName {
                database: None,
                schema: None,
                item: "numeric".to_string(),
            },
        ),
        (
            FullName {
                database: DatabaseSpecifier::Ambient,
                schema: MZ_CATALOG_SCHEMA.to_string(),
                item: "mz_array_types".to_string(),
            },
            PartialName {
                database: None,
                schema: None,
                item: "mz_array_types".to_string(),
            },
        ),
    ];

    let catalog_file = NamedTempFile::new()?;
    let catalog = coord::catalog::Catalog::open_debug(catalog_file.path())?;
    let session = Session::dummy();
    let catalog = catalog.for_session(&session);

    for (full_name, partial_name) in test_cases {
        assert_eq!(catalog.minimal_qualification(&full_name), partial_name);
    }
    Ok(())
}
