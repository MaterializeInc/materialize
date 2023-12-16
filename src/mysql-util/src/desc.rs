// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct MySqlTableDesc {
    /// In MySQL the schema and database of a table are synonymous.
    pub schema_name: String,
    /// The name of the table.
    pub name: String,
    /// Columns for the table
    ///
    /// The index of each column is based on its `ordinal_position`
    /// reported by the information_schema.columns table, which defines
    /// the order of column values when received in a row.
    pub columns: Vec<MySqlColumnDesc>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct MySqlColumnDesc {
    /// The name of the column.
    pub name: String,
    /// The MySQL datatype of the column.
    pub data_type: MySqlDataType,
    /// Whether the column is nullable.
    pub nullable: bool,
    // TODO: add more column properties
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum MySqlDataType {
    Int,
    Varchar(usize),
    // TODO: add more data types
}
