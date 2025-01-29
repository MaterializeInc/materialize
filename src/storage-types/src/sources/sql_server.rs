// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Types related to SQL Server sources

use std::sync::LazyLock;

use mz_repr::{RelationDesc, ScalarType};

pub static SQL_SERVER_PROGRESS_DESC: LazyLock<RelationDesc> = LazyLock::new(|| {
    RelationDesc::builder()
        .with_column("lsn", ScalarType::Bytes.nullable(true))
        .finish()
});
