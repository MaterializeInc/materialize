// Copyright 2019-2020 Materialize Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use failure::bail;

use catalog::names::PartialName;
use ore::collections::CollectionExt;
use repr::ColumnName;
use sql_parser::ast::{Ident, ObjectName};

pub fn ident(ident: Ident) -> String {
    if ident.quote_style.is_some() {
        ident.value
    } else {
        ident.value.to_lowercase()
    }
}

pub fn function_name(name: ObjectName) -> Result<String, failure::Error> {
    if name.0.len() != 1 {
        bail!("qualified function names are not supported");
    }
    Ok(ident(name.0.into_element()))
}

pub fn column_name(id: Ident) -> ColumnName {
    ColumnName::from(ident(id))
}

pub fn object_name(mut name: ObjectName) -> Result<PartialName, failure::Error> {
    if name.0.len() < 1 || name.0.len() > 3 {
        bail!(
            "qualified names must have between 1 and 3 components, got {}: {}",
            name.0.len(),
            name
        );
    }
    let out = PartialName {
        item: ident(
            name.0
                .pop()
                .expect("name checked to have at least one component"),
        ),
        schema: name.0.pop().map(ident),
        database: name.0.pop().map(ident),
    };
    assert!(name.0.is_empty());
    Ok(out)
}
