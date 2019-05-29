// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use crate::dataflow2::*;

struct ColumnName {
    table_name: Option<String>,
    column_name: Option<String>,
}

fn plan(ast: sqlparser::sqlast::ASTNode) -> (RelationExpr, RelationType, Vec<ColumnName>) {
    unimplemented!();
}
