// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use repr::ColumnName;
use sqlparser::ast::Ident;

pub fn ident_to_col_name(ident: Ident) -> ColumnName {
    if ident.quote_style.is_some() {
        ColumnName::from(ident.value)
    } else {
        ColumnName::from(ident.value.to_lowercase())
    }
}
