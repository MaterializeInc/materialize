// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use crate::dataflow2::*;

pub struct ColumnName {
    pub table_name: Option<String>,
    pub column_name: Option<String>,
}

pub fn plan(
    _ast: sqlparser::sqlast::ASTNode,
) -> (RelationExpr, OwnedRelationType, Vec<ColumnName>) {
    unimplemented!();
}
