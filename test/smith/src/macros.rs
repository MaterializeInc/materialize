// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/// Inline a query from the resources/views directory
#[macro_export]
macro_rules! query {
    ($fname:tt) => {
        include_str!(concat!("../resources/views/", $fname, ".sql"))
    };
}

/// Execute a named query
#[macro_export]
macro_rules! exec_query {
    ($client:ident, $fname:tt) => {{
        let q = query!($fname);
        if let Err(e) = $client.execute(&q, &[]).await {
            log::error!("{} ({}) executing query {}", e, e.source().unwrap(), q)
        }
    }};
}
