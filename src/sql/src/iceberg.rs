// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::Duration;

use crate::names::Aug;
use mz_sql_parser::ast::display::AstDisplay;
use mz_sql_parser::ast::{IcebergSinkConfigOption, IcebergSinkConfigOptionName};

generate_extracted_config!(
    IcebergSinkConfigOption,
    (Table, String),
    (Namespace, String),
    (CommitInterval, Duration)
);
