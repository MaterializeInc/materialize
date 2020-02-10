// Copyright 2018 sqlparser-rs contributors. All rights reserved.
// Copyright Materialize, Inc. All rights reserved.
//
// This file is derived from the sqlparser-rs project, available at
// https://github.com/andygrove/sqlparser-rs. It was incorporated
// directly into Materialize on December 21, 2019.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file at the
// root of this repository, or online at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fmt;

/// Unary operators
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum UnaryOperator {
    Plus,
    Minus,
    Not,
}

impl fmt::Display for UnaryOperator {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(match self {
            UnaryOperator::Plus => "+",
            UnaryOperator::Minus => "-",
            UnaryOperator::Not => "NOT",
        })
    }
}

/// Binary operators
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum BinaryOperator {
    Plus,
    Minus,
    Multiply,
    Divide,
    Modulus,
    Gt,
    Lt,
    GtEq,
    LtEq,
    Eq,
    NotEq,
    And,
    Or,
    Like,
    NotLike,
    JsonGet,
    JsonGetAsText,
    JsonGetPath,
    JsonGetPathAsText,
    JsonContainsJson,
    JsonContainedInJson,
    JsonContainsField,
    JsonContainsAnyFields,
    JsonContainsAllFields,
    JsonConcat,
    JsonDeletePath,
    JsonContainsPath,
    JsonApplyPathPredicate,
}

impl fmt::Display for BinaryOperator {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(match self {
            BinaryOperator::Plus => "+",
            BinaryOperator::Minus => "-",
            BinaryOperator::Multiply => "*",
            BinaryOperator::Divide => "/",
            BinaryOperator::Modulus => "%",
            BinaryOperator::Gt => ">",
            BinaryOperator::Lt => "<",
            BinaryOperator::GtEq => ">=",
            BinaryOperator::LtEq => "<=",
            BinaryOperator::Eq => "=",
            BinaryOperator::NotEq => "<>",
            BinaryOperator::And => "AND",
            BinaryOperator::Or => "OR",
            BinaryOperator::Like => "LIKE",
            BinaryOperator::NotLike => "NOT LIKE",
            BinaryOperator::JsonGet => "->",
            BinaryOperator::JsonGetAsText => "->>",
            BinaryOperator::JsonGetPath => "#>",
            BinaryOperator::JsonGetPathAsText => "#>>",
            BinaryOperator::JsonContainsJson => "@>",
            BinaryOperator::JsonContainedInJson => "<@",
            BinaryOperator::JsonContainsField => "?",
            BinaryOperator::JsonContainsAnyFields => "?|",
            BinaryOperator::JsonContainsAllFields => "?&",
            BinaryOperator::JsonConcat => "||",
            BinaryOperator::JsonDeletePath => "#-",
            BinaryOperator::JsonContainsPath => "@?",
            BinaryOperator::JsonApplyPathPredicate => "@@",
        })
    }
}
