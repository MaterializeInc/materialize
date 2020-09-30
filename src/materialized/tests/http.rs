// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Integration tests for HTTP functionality.

use std::collections::HashMap;
use std::error::Error;

use reqwest::{blocking::Client, StatusCode, Url};

pub mod util;

#[test]
fn test_http_sql() -> Result<(), Box<dyn Error>> {
    let (server, _client) = util::start_server(util::Config::default())?;
    let url = Url::parse(&format!("http://{}/sql", server.inner.local_addr()))?;
    let mut params = HashMap::new();

    struct TestCase {
        query: &'static str,
        status: StatusCode,
        body: &'static str,
    }

    let tests = vec![
        // Regular query works.
        TestCase {
            query: "select 1+2 as col",
            status: StatusCode::OK,
            body: r#"{"rows":[[3]],"col_names":["col"]}"#,
        },
        // Only one query at a time.
        TestCase {
            query: "select 1; select 2",
            status: StatusCode::BAD_REQUEST,
            body: r#"expected exactly 1 statement"#,
        },
        // CREATEs should not work.
        TestCase {
            query: "create view v as select 1",
            status: StatusCode::BAD_REQUEST,
            body: r#"unsupported plan"#,
        },
    ];

    for tc in tests {
        params.insert("sql", tc.query);
        let res = Client::new().post(url.clone()).form(&params).send()?;
        assert_eq!(res.status(), tc.status);
        assert_eq!(res.text()?, tc.body);
    }

    Ok(())
}
