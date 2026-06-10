// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use serde::Serialize;

#[derive(Serialize)]
pub struct ErrorResponse {
    pub errors: Vec<String>,
}

#[derive(Debug, PartialEq)]
pub enum SortBy {
    Email,
    Id,
}

#[derive(Debug, PartialEq)]
pub enum Order {
    ASC,
    DESC,
}

impl TryFrom<&str> for Order {
    type Error = String;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value.to_uppercase().as_str() {
            "ASC" => Ok(Order::ASC),
            "DESC" => Ok(Order::DESC),
            _ => Err(format!("'{}' is not a valid order option", value)),
        }
    }
}

impl TryFrom<&str> for SortBy {
    type Error = String;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "email" => Ok(SortBy::Email),
            "id" => Ok(SortBy::Id),
            _ => Err(format!("'{}' is not a valid sort option", value)),
        }
    }
}
