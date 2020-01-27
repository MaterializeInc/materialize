// Copyright 2020 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

/// Inline a query from the resources/views directory
#[macro_export]
macro_rules! query {
    ($fname:tt) => {
        include_str!(concat!(env!("VIEWS_DIR"), "/", $fname, ".sql.in"))
    };
}

/// Execute a named query
///
/// With three arguments, the `source_name` property from the second arg will be passed in
///
/// With two arguments, no source_name will be passed to the query
#[macro_export]
macro_rules! exec_query {
    ($client:ident, $config:ident, $fname:tt) => {{
        let q = format!(query!($fname), source_name = $config.source_name);
        if let Err(e) = $client.execute(&q, &[]).await {
            log::error!("{} ({}) executing query {}", e, e.source().unwrap(), q)
        }
    }};

    ($client:ident, $fname:tt) => {{
        let q = format!(query!($fname));
        if let Err(e) = $client.execute(&q, &[]).await {
            log::error!("{} ({}) executing query {}", e, e.source().unwrap(), q)
        }
    }};
}
