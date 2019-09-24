// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

//! Abstract syntax tree nodes for sqllogictest.

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Type {
    Text,
    Integer,
    Timestamp,
    Real,
    Bool,
    Oid,
}

/// How to sort some row outputs
///
/// See sqlite/about.wiki
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Sort {
    /// Do not sort. Default. signifier `nosort`
    No,
    /// Sort each column in each row lexicographically. signifier: `rowsort`
    Row,
    /// Sert each value as though they're in one big list. signifier: `valuesort`
    ///
    /// Every value in every column will end up being sorted with no respect
    /// for columns or rows.
    Value,
}

impl Sort {
    /// Is true if any kind of sorting should happen
    pub fn yes(&self) -> bool {
        use Sort::*;
        match self {
            No => false,
            Row | Value => true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Output {
    Values(Vec<String>),
    Hashed { num_values: usize, md5: String },
}

impl std::fmt::Display for Output {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Output::Values(strings) if strings.len() == 1 => f.write_str(&strings[0]),
            _ => write!(f, "{:?}", self),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryOutput<'a> {
    pub types: Vec<Type>,
    pub sort: Sort,
    pub label: Option<&'a str>,
    pub column_names: Option<Vec<&'a str>>,
    pub mode: Mode,
    pub output: Output,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Record<'a> {
    Statement {
        should_run: bool,
        rows_inserted: Option<usize>,
        sql: &'a str,
    },
    Query {
        sql: &'a str,
        output: Result<QueryOutput<'a>, &'a str>,
    },
    HashThreshold {
        threshold: u64,
    },
    Skip,
    Halt,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// In `Standard` mode, expected query output is formatted so that every
    /// value is always on its own line, like so:
    ///
    ///
    ///    query II
    ///    SELECT * FROM VALUES (1, 2), (3, 4)
    ///    ----
    ///    1
    ///    2
    ///    3
    ///    4
    ///
    /// Row boundaries are not visually represented, but they can be inferred
    /// because the number of columns per row is specified by the `query`
    /// directive.
    Standard,
    /// In `Cockroach` mode, expected query output is formatted so that rows
    /// can contain multiple whitespace-separated columns:
    ///
    ///    query II
    ///    SELECT * FROM VALUES (1, 2), (3, 4)
    ///    ----
    ///    1 2
    ///    3 4
    ///
    /// This formatting, while easier to parse visually, is thoroughly
    /// frustrating when column values contain whitespace, e.g., strings like
    /// "one two", as there is no way to know where the column boundaries are.
    /// We jump through some hoops to make this work. You might want to
    /// refer to this upstream Cockroach commit [0] for additional details.
    ///
    /// [0]: https://github.com/cockroachdb/cockroach/commit/75c3023ec86a76fe6fb60fe1c6f00752b9784801
    Cockroach,
}
