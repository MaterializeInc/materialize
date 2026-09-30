// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! End-to-end tests for WebAssembly user-defined functions.
//!
//! The guest module is `src/wasm-udf/tests/fixtures/udfs`, which is too large
//! to inline in a sqllogictest file.

// The tests issue fixed SQL text, which `Sql` composition would not make safer.
#![allow(clippy::disallowed_methods)]

use std::io::Read;

use base64::Engine;
use mz_environmentd::test_util::{self, TestHarness};
use mz_ore::assert_contains;

fn module_base64() -> String {
    let gz: &[u8] = include_bytes!("../../wasm-udf/tests/fixtures/udfs.wasm.gz");
    let mut bytes = Vec::new();
    flate2::read::GzDecoder::new(gz)
        .read_to_end(&mut bytes)
        .expect("valid gzip");
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

fn harness() -> TestHarness {
    TestHarness::default()
        .with_system_parameter_default("enable_wasm_functions".to_string(), "true".to_string())
}

fn create_functions(client: &mut postgres::Client) {
    let module = module_base64();
    client
        .batch_execute(&format!(
            "CREATE FUNCTION gcd(int, int) RETURNS int LANGUAGE wasm STRICT USING BASE64 '{module}';
             CREATE FUNCTION safe_div(bigint, bigint) RETURNS bigint LANGUAGE wasm
                 USING BASE64 '{module}';
             CREATE FUNCTION similarity(a text, b text) RETURNS double precision LANGUAGE wasm
                 USING BASE64 '{module}' WITH (EXPORT = 'jaro_winkler');"
        ))
        .unwrap();
}

fn gcds(client: &mut postgres::Client, query: &str) -> Vec<(i32, Option<i32>)> {
    client
        .query(query, &[])
        .unwrap()
        .into_iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect()
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function
fn wasm_functions_end_to_end() {
    let data_dir = tempfile::tempdir().unwrap();
    let harness = harness().data_directory(data_dir.path());

    {
        let server = harness.clone().start_blocking();
        let mut client = server.connect(postgres::NoTls).unwrap();
        create_functions(&mut client);

        let row = client.query_one("SHOW CREATE FUNCTION gcd", &[]).unwrap();
        assert_contains!(row.get::<_, String>(1), "LANGUAGE wasm STRICT USING BASE64");
        let row = client
            .query_one("SHOW REDACTED CREATE FUNCTION gcd", &[])
            .unwrap();
        assert_contains!(row.get::<_, String>(1), "USING BASE64 '<REDACTED>'");

        // Constant arguments are not folded in environmentd; the call runs on
        // the cluster.
        let row = client.query_one("SELECT gcd(12, 18)", &[]).unwrap();
        assert_eq!(row.get::<_, i32>(0), 6);

        client
            .batch_execute(
                "CREATE TABLE t (a int, b int);
                 INSERT INTO t VALUES (12, 18), (7, 3), (5, NULL);
                 CREATE MATERIALIZED VIEW mv AS SELECT a, gcd(a, b) FROM t;",
            )
            .unwrap();
        assert_eq!(
            gcds(&mut client, "SELECT * FROM mv ORDER BY a"),
            vec![(5, None), (7, Some(1)), (12, Some(6))]
        );

        // Retractions recompute the function and cancel the insertions.
        client
            .batch_execute("DELETE FROM t WHERE a = 7; INSERT INTO t VALUES (8, 12);")
            .unwrap();
        assert_eq!(
            gcds(&mut client, "SELECT * FROM mv ORDER BY a"),
            vec![(5, None), (8, Some(4)), (12, Some(6))]
        );

        let similarity: f64 = client
            .query_one("SELECT similarity('MARTHA', 'MARHTA')", &[])
            .unwrap()
            .get(0);
        assert!((similarity - 0.961_111).abs() < 1e-5, "{similarity}");

        let err = client.query("SELECT safe_div(1, 0)", &[]).unwrap_err();
        assert_contains!(err.to_string(), "division by zero");

        let err = client.batch_execute("DROP FUNCTION gcd").unwrap_err();
        assert_contains!(err.to_string(), "still depend");
    }

    // Functions are re-planned from the catalog at boot, and their modules
    // reinstalled before the materialized view's dataflow.
    {
        let server = harness.start_blocking();
        let mut client = server.connect(postgres::NoTls).unwrap();
        assert_eq!(
            gcds(&mut client, "SELECT * FROM mv ORDER BY a"),
            vec![(5, None), (8, Some(4)), (12, Some(6))]
        );
        client.batch_execute("INSERT INTO t VALUES (9, 6)").unwrap();
        assert_eq!(
            gcds(&mut client, "SELECT * FROM mv WHERE a = 9"),
            vec![(9, Some(3))]
        );

        client
            .batch_execute("DROP MATERIALIZED VIEW mv; DROP FUNCTION gcd;")
            .unwrap();
        let err = client.query("SELECT gcd(1, 2)", &[]).unwrap_err();
        assert_contains!(err.to_string(), "does not exist");
    }
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function
fn wasm_function_validation() {
    let server = harness().start_blocking();
    let mut client = server.connect(postgres::NoTls).unwrap();
    let module = module_base64();

    let err = client
        .batch_execute(&format!(
            "CREATE FUNCTION gcd(bigint, bigint) RETURNS bigint LANGUAGE wasm USING BASE64 '{module}'"
        ))
        .unwrap_err();
    assert_contains!(err.to_string(), "gcd(int32,int32)->int32");

    let err = client
        .batch_execute(&format!(
            "CREATE FUNCTION gcd(int, int) RETURNS int LANGUAGE wasm VOLATILE USING BASE64 '{module}'"
        ))
        .unwrap_err();
    assert_contains!(err.to_string(), "must be IMMUTABLE");

    let err = client
        .batch_execute("CREATE FUNCTION f(int) RETURNS int LANGUAGE wasm USING BASE64 'AAAA'")
        .unwrap_err();
    assert_contains!(err.to_string(), "invalid function module");

    let err = client
        .batch_execute(&format!(
            "CREATE FUNCTION gcd(int, int) RETURNS int LANGUAGE wasm USING BASE64 '{module}' \
             WITH (FUEL = 1000000000000)"
        ))
        .unwrap_err();
    assert_contains!(err.to_string(), "max_wasm_function_fuel");

    // The same function behind a feature flag that is off is rejected.
    let server = test_util::TestHarness::default().start_blocking();
    let mut client = server.connect(postgres::NoTls).unwrap();
    let err = client
        .batch_execute(&format!(
            "CREATE FUNCTION gcd(int, int) RETURNS int LANGUAGE wasm USING BASE64 '{module}'"
        ))
        .unwrap_err();
    assert_contains!(err.to_string(), "CREATE FUNCTION ... LANGUAGE wasm");
}
