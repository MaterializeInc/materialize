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

fn module_bytes() -> Vec<u8> {
    let gz: &[u8] = include_bytes!("../../wasm-udf/tests/fixtures/udfs.wasm.gz");
    let mut bytes = Vec::new();
    flate2::read::GzDecoder::new(gz)
        .read_to_end(&mut bytes)
        .expect("valid gzip");
    bytes
}

fn module_base64() -> String {
    base64::engine::general_purpose::STANDARD.encode(module_bytes())
}

fn harness() -> TestHarness {
    TestHarness::default()
        .with_system_parameter_default("enable_wasm_functions".to_string(), "true".to_string())
}

/// Runs `sql`, a `CREATE WASM MODULE ... USING BASE64 $1`, with the fixture
/// module bound to `$1`. The module is larger than the statement text limit.
fn create_module(client: &mut postgres::Client, sql: &str) -> Result<u64, postgres::Error> {
    client.execute(sql, &[&module_base64()])
}

fn create_functions(client: &mut postgres::Client) {
    create_module(client, "CREATE WASM MODULE udfs USING BASE64 $1").unwrap();
    execute_each(
        client,
        &[
            "CREATE FUNCTION gcd(int, int) RETURNS int LANGUAGE wasm STRICT USING MODULE udfs",
            "CREATE FUNCTION safe_div(bigint, bigint) RETURNS bigint LANGUAGE wasm
                 USING MODULE udfs",
            "CREATE FUNCTION similarity(a text, b text) RETURNS double precision LANGUAGE wasm
                 USING MODULE udfs WITH (EXPORT = 'jaro_winkler')",
        ],
    );
}

/// Runs each statement on its own. A multi-statement batch is an implicit
/// transaction, which DDL cannot run in.
fn execute_each(client: &mut postgres::Client, statements: &[&str]) {
    for statement in statements {
        client
            .batch_execute(statement)
            .unwrap_or_else(|e| panic!("{statement}: {}", message(&e)));
    }
}

/// The server's message for `err`, which `postgres::Error`'s `Display` omits.
fn message(err: &postgres::Error) -> String {
    match err.as_db_error() {
        Some(db) => db.message().to_string(),
        None => err.to_string(),
    }
}

fn gcds(client: &mut postgres::Client, query: &str) -> Vec<(i32, Option<i32>)> {
    client
        .query(query, &[])
        .unwrap()
        .into_iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect()
}

fn show_create(client: &mut postgres::Client, object: &str) -> String {
    client
        .query_one(&format!("SHOW CREATE {object}"), &[])
        .unwrap()
        .get(1)
}

fn show_redacted_create(client: &mut postgres::Client, object: &str) -> String {
    client
        .query_one(&format!("SHOW REDACTED CREATE {object}"), &[])
        .unwrap()
        .get(1)
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

        assert_eq!(
            show_create(&mut client, "FUNCTION gcd"),
            "CREATE FUNCTION materialize.public.gcd(pg_catalog.int4, pg_catalog.int4) \
             RETURNS pg_catalog.int4 LANGUAGE wasm IMMUTABLE STRICT \
             USING MODULE materialize.public.udfs \
             WITH (EXPORT = 'gcd', FUEL = 100000000, MEMORY = '64MB');"
        );
        assert_contains!(
            show_create(&mut client, "WASM MODULE udfs"),
            "CREATE WASM MODULE materialize.public.udfs USING BASE64 'AGFzbQ"
        );
        assert_eq!(
            show_redacted_create(&mut client, "WASM MODULE udfs"),
            "CREATE WASM MODULE materialize.public.udfs USING BASE64 '<REDACTED>';"
        );
        let row = client
            .query_one("SELECT name, size::int8 FROM (SHOW WASM MODULES)", &[])
            .unwrap();
        assert_eq!(
            (row.get::<_, String>(0), row.get::<_, i64>(1)),
            ("udfs".into(), i64::try_from(module_bytes().len()).unwrap())
        );

        let row = client
            .query_one(
                "SELECT prorettype::int8 FROM pg_proc \
                 JOIN pg_namespace ON pronamespace = pg_namespace.oid \
                 WHERE proname = 'gcd' AND nspname = 'public'",
                &[],
            )
            .unwrap();
        assert_eq!(row.get::<_, i64>(0), 23, "int4");

        // Constant arguments are not folded in environmentd; the call runs on
        // the cluster.
        let row = client.query_one("SELECT gcd(12, 18)", &[]).unwrap();
        assert_eq!(row.get::<_, i32>(0), 6);

        // Expressions environmentd evaluates itself cannot call a guest.
        client.batch_execute("PREPARE p AS SELECT $1::int").unwrap();
        let err = client.batch_execute("EXECUTE p(gcd(12, 18))").unwrap_err();
        assert_contains!(message(&err), "outside a query that runs on a cluster");
        let err = client
            .batch_execute("CREATE TABLE d (x int DEFAULT gcd(1, 2))")
            .unwrap_err();
        assert_contains!(message(&err), "DEFAULT expressions cannot call");

        execute_each(
            &mut client,
            &[
                "CREATE TABLE t (a int, b int)",
                "INSERT INTO t VALUES (12, 18), (7, 3), (5, NULL)",
                "CREATE MATERIALIZED VIEW mv AS SELECT a, gcd(a, b) FROM t",
                // A view with no dependencies sorts ahead of the module at
                // boot, and its index ships right after it.
                "CREATE VIEW pair AS SELECT 12 AS a, 18 AS b",
                "CREATE INDEX pair_gcd ON pair (gcd(a, b))",
            ],
        );
        assert_eq!(
            gcds(&mut client, "SELECT * FROM mv ORDER BY a"),
            vec![(5, None), (7, Some(1)), (12, Some(6))]
        );

        // Retractions recompute the function and cancel the insertions.
        execute_each(
            &mut client,
            &["DELETE FROM t WHERE a = 7", "INSERT INTO t VALUES (8, 12)"],
        );
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
        assert_contains!(message(&err), "division by zero");

        // The source's MFP spans more than one batch, filters before the call,
        // and calls `safe_div` under a `CASE` that rows with `b = 0` never
        // take. The guest's errors for those rows must not surface.
        execute_each(
            &mut client,
            &[
                "CREATE TABLE big (a bigint, b bigint)",
                "INSERT INTO big SELECT x, x % 7 FROM generate_series(1, 40000) AS x",
                "CREATE MATERIALIZED VIEW quotients AS
                     SELECT a, CASE WHEN b > 0 THEN safe_div(a, b) ELSE -1 END AS q
                     FROM big WHERE a % 3 <> 0",
            ],
        );
        let expected = "SELECT a, CASE WHEN b > 0 THEN a / b ELSE -1 END FROM big WHERE a % 3 <> 0";
        let row = client
            .query_one(
                &format!(
                    "SELECT
                         (SELECT count(*) FROM quotients),
                         (SELECT count(*) FROM (
                             (SELECT * FROM quotients EXCEPT ALL {expected})
                             UNION ALL
                             ({expected} EXCEPT ALL SELECT * FROM quotients)
                         ))"
                ),
                &[],
            )
            .unwrap();
        assert_eq!((row.get::<_, i64>(0), row.get::<_, i64>(1)), (26667, 0));

        let err = client.batch_execute("DROP FUNCTION gcd").unwrap_err();
        assert_contains!(message(&err), "still depend");
        let err = client.batch_execute("DROP WASM MODULE udfs").unwrap_err();
        assert_contains!(message(&err), "still depend");

        // Renaming the module and the schema rewrites the functions that
        // reference them, which the restart below re-plans.
        execute_each(&mut client, &["ALTER WASM MODULE udfs RENAME TO guest"]);
        // `mz_system` owns `public`.
        server
            .connect_internal(postgres::NoTls)
            .unwrap()
            .batch_execute("ALTER SCHEMA materialize.public RENAME TO renamed")
            .unwrap();
        execute_each(&mut client, &["SET search_path = renamed"]);
        assert_contains!(
            show_create(&mut client, "FUNCTION gcd"),
            "USING MODULE materialize.renamed.guest"
        );
    }

    // Functions are re-planned from the catalog at boot, and their modules
    // registered before the materialized view's dataflow.
    {
        let server = harness.start_blocking();
        let mut client = server.connect(postgres::NoTls).unwrap();
        client.batch_execute("SET search_path = renamed").unwrap();
        assert_eq!(
            gcds(&mut client, "SELECT * FROM mv ORDER BY a"),
            vec![(5, None), (8, Some(4)), (12, Some(6))]
        );
        client.batch_execute("INSERT INTO t VALUES (9, 6)").unwrap();
        assert_eq!(
            gcds(&mut client, "SELECT * FROM mv WHERE a = 9"),
            vec![(9, Some(3))]
        );
        let row = client.query_one("SELECT gcd(21, 14)", &[]).unwrap();
        assert_eq!(row.get::<_, i32>(0), 7);
        let row = client.query_one("SELECT gcd(a, b) FROM pair", &[]).unwrap();
        assert_eq!(row.get::<_, i32>(0), 6);

        execute_each(
            &mut client,
            &[
                "DROP MATERIALIZED VIEW mv",
                "DROP MATERIALIZED VIEW quotients",
                "DROP INDEX pair_gcd",
                "DROP FUNCTION gcd(int4, int4)",
            ],
        );
        let err = client.query("SELECT gcd(1, 2)", &[]).unwrap_err();
        assert_contains!(message(&err), "does not exist");

        // Dropping the module drops the functions that reference it.
        client
            .batch_execute("DROP WASM MODULE guest CASCADE")
            .unwrap();
        let err = client
            .query("SELECT similarity('a', 'b')", &[])
            .unwrap_err();
        assert_contains!(message(&err), "does not exist");
    }
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function
fn wasm_function_validation() {
    let server = harness().start_blocking();
    let mut client = server.connect(postgres::NoTls).unwrap();

    // The module is too large to inline in the statement text.
    let err = client
        .batch_execute(&format!(
            "CREATE WASM MODULE udfs USING BASE64 '{}'",
            module_base64()
        ))
        .unwrap_err();
    assert_contains!(message(&err), "statement batch size cannot exceed");

    let err = client
        .batch_execute("CREATE WASM MODULE m USING BASE64 'AAAA'")
        .unwrap_err();
    assert_contains!(message(&err), "invalid WebAssembly module");

    let err = client
        .execute("CREATE WASM MODULE m USING BASE64 $1", &[&None::<String>])
        .unwrap_err();
    assert_contains!(message(&err), "must be a non-null text value");

    create_module(&mut client, "CREATE WASM MODULE udfs USING BASE64 $1").unwrap();
    // The same bytes under the same name are a no-op.
    create_module(
        &mut client,
        "CREATE WASM MODULE IF NOT EXISTS udfs USING BASE64 $1",
    )
    .unwrap();

    let err = client
        .batch_execute(
            "CREATE FUNCTION gcd(bigint, bigint) RETURNS bigint LANGUAGE wasm USING MODULE udfs",
        )
        .unwrap_err();
    assert_contains!(message(&err), "gcd(int32,int32)->int32");

    let err = client
        .batch_execute(
            "CREATE FUNCTION gcd(int, int) RETURNS int LANGUAGE wasm VOLATILE USING MODULE udfs",
        )
        .unwrap_err();
    assert_contains!(message(&err), "must be IMMUTABLE");

    let err = client
        .batch_execute(
            "CREATE FUNCTION gcd(int, int) RETURNS int LANGUAGE wasm USING MODULE udfs \
             WITH (FUEL = 1000000000000)",
        )
        .unwrap_err();
    assert_contains!(message(&err), "max_wasm_function_fuel");

    let err = client
        .batch_execute("CREATE FUNCTION f(int) RETURNS int LANGUAGE wasm USING MODULE t")
        .unwrap_err();
    assert_contains!(message(&err), "unknown catalog item 't'");
    execute_each(&mut client, &["CREATE TABLE t (a int)"]);
    let err = client
        .batch_execute("CREATE FUNCTION f(int) RETURNS int LANGUAGE wasm USING MODULE t")
        .unwrap_err();
    assert_contains!(message(&err), "is not a WebAssembly module");

    // Builtins are resolved before user schemas, so the function could never
    // be called unqualified.
    let err = client
        .batch_execute(
            "CREATE FUNCTION abs(int, int) RETURNS int LANGUAGE wasm USING MODULE udfs \
             WITH (EXPORT = 'gcd')",
        )
        .unwrap_err();
    assert_contains!(message(&err), "has the name of a builtin function");

    let gcd = "CREATE FUNCTION IF NOT EXISTS gcd(int, int) RETURNS int LANGUAGE wasm STRICT \
               USING MODULE udfs";
    execute_each(&mut client, &[gcd, gcd]);
    let err = client
        .batch_execute(
            "CREATE FUNCTION IF NOT EXISTS gcd(int, int) RETURNS int LANGUAGE wasm \
             USING MODULE udfs",
        )
        .unwrap_err();
    assert_contains!(
        message(&err),
        "function \"gcd\" already exists with a different definition"
    );

    // `DROP FUNCTION` checks the argument types it is given.
    let err = client
        .batch_execute("DROP FUNCTION gcd(bigint, bigint)")
        .unwrap_err();
    assert_contains!(message(&err), "does not exist");
    execute_each(
        &mut client,
        &["DROP FUNCTION IF EXISTS gcd(bigint, bigint)"],
    );
    execute_each(
        &mut client,
        &[
            "COMMENT ON FUNCTION gcd IS 'greatest common divisor'",
            "DROP FUNCTION gcd(integer, int4)",
        ],
    );

    // Functions have their own namespace: a function may share a name with
    // its module or a table, and a name held by another kind of object is
    // reported as such.
    execute_each(
        &mut client,
        &[
            "CREATE FUNCTION udfs(int, int) RETURNS int LANGUAGE wasm USING MODULE udfs \
             WITH (EXPORT = 'gcd')",
            "CREATE FUNCTION t(int, int) RETURNS int LANGUAGE wasm USING MODULE udfs \
             WITH (EXPORT = 'gcd')",
            "DROP FUNCTION udfs",
            "DROP FUNCTION t",
        ],
    );
    let err = create_module(
        &mut client,
        "CREATE WASM MODULE IF NOT EXISTS t USING BASE64 $1",
    )
    .unwrap_err();
    assert_contains!(message(&err), "table \"t\" already exists");

    // Renaming a table leaves calls to a function of the same name alone.
    execute_each(
        &mut client,
        &[
            "CREATE TABLE shared (a int, b int)",
            "CREATE FUNCTION shared(int, int) RETURNS int LANGUAGE wasm USING MODULE udfs \
             WITH (EXPORT = 'gcd')",
            "CREATE VIEW uses_both AS SELECT shared(a, b) FROM shared",
            "ALTER TABLE shared RENAME TO renamed",
        ],
    );
    let definition = show_create(&mut client, "VIEW uses_both");
    assert_contains!(&definition, "materialize.public.shared(");
    assert_contains!(definition, "FROM materialize.public.renamed");
    execute_each(
        &mut client,
        &[
            "DROP VIEW uses_both",
            "DROP FUNCTION shared",
            "DROP TABLE renamed",
        ],
    );

    // Array types cannot carry an element precision at all; lists can.
    for returns in ["timestamp(3)", "timestamptz(3) list"] {
        let err = client
            .batch_execute(&format!(
                "CREATE FUNCTION ts(timestamp) RETURNS {returns} LANGUAGE wasm USING MODULE udfs"
            ))
            .unwrap_err();
        assert_contains!(message(&err), "cannot have type modifiers");
    }

    // Dropping a function without CASCADE must not drop an index whose key
    // calls it.
    execute_each(
        &mut client,
        &[
            "CREATE FUNCTION gcd(int, int) RETURNS int LANGUAGE wasm STRICT USING MODULE udfs",
            "CREATE TABLE pairs (a int, b int)",
            "CREATE INDEX pairs_gcd ON pairs (gcd(a, b))",
        ],
    );
    let err = client.batch_execute("DROP FUNCTION gcd").unwrap_err();
    assert_contains!(message(&err), "still depend");
    execute_each(&mut client, &["DROP FUNCTION gcd CASCADE"]);
    let indexes: i64 = client
        .query_one(
            "SELECT count(*) FROM mz_indexes WHERE name = 'pairs_gcd'",
            &[],
        )
        .unwrap()
        .get(0);
    assert_eq!(indexes, 0);

    execute_each(
        &mut client,
        &[
            "COMMENT ON WASM MODULE udfs IS 'guest functions'",
            "DROP WASM MODULE udfs",
        ],
    );

    // Module size and count limits apply when a module is created.
    let mut system = server.connect_internal(postgres::NoTls).unwrap();
    system
        .batch_execute("ALTER SYSTEM SET max_wasm_module_size = '1kB'")
        .unwrap();
    let err = create_module(&mut client, "CREATE WASM MODULE udfs USING BASE64 $1").unwrap_err();
    assert_contains!(message(&err), "exceeds max_wasm_module_size");
    system
        .batch_execute("ALTER SYSTEM RESET max_wasm_module_size")
        .unwrap();
    system
        .batch_execute("ALTER SYSTEM SET max_wasm_modules = 1")
        .unwrap();
    create_module(&mut client, "CREATE WASM MODULE a USING BASE64 $1").unwrap();
    let err = create_module(&mut client, "CREATE WASM MODULE b USING BASE64 $1").unwrap_err();
    assert_contains!(message(&err), "max_wasm_modules");
    // Redeploying an identical module is a no-op, even at the limit.
    create_module(
        &mut client,
        "CREATE WASM MODULE IF NOT EXISTS a USING BASE64 $1",
    )
    .unwrap();

    // Functions count toward max_objects_per_schema.
    execute_each(
        &mut client,
        &[
            "CREATE SCHEMA limited",
            "CREATE FUNCTION limited.f1(int, int) RETURNS int LANGUAGE wasm USING MODULE a \
             WITH (EXPORT = 'gcd')",
        ],
    );
    system
        .batch_execute("ALTER SYSTEM SET max_objects_per_schema = 1")
        .unwrap();
    let err = client
        .batch_execute(
            "CREATE FUNCTION limited.f2(int, int) RETURNS int LANGUAGE wasm USING MODULE a \
             WITH (EXPORT = 'gcd')",
        )
        .unwrap_err();
    assert_contains!(message(&err), "max_objects_per_schema");
    system
        .batch_execute("ALTER SYSTEM RESET max_objects_per_schema")
        .unwrap();

    // The same statements behind a feature flag that is off are rejected.
    let server = test_util::TestHarness::default().start_blocking();
    let mut client = server.connect(postgres::NoTls).unwrap();
    let err = create_module(&mut client, "CREATE WASM MODULE udfs USING BASE64 $1").unwrap_err();
    assert_contains!(message(&err), "LANGUAGE wasm is not available");
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function
fn wasm_module_privileges() {
    let server = harness().start_blocking();
    let mut owner = server.connect(postgres::NoTls).unwrap();
    let mut system = server.connect_internal(postgres::NoTls).unwrap();
    create_module(&mut owner, "CREATE WASM MODULE udfs USING BASE64 $1").unwrap();

    let mut other = server
        .pg_config()
        .user("other")
        .connect(postgres::NoTls)
        .unwrap();
    system
        .batch_execute("GRANT CREATE ON SCHEMA materialize.public TO other")
        .unwrap();

    // Creating a module takes the CREATEWASMMODULE system privilege in
    // addition to CREATE on the schema.
    let err = create_module(&mut other, "CREATE WASM MODULE mine USING BASE64 $1").unwrap_err();
    assert_contains!(message(&err), "permission denied for SYSTEM");
    system
        .batch_execute("GRANT CREATEWASMMODULE ON SYSTEM TO other")
        .unwrap();
    create_module(&mut other, "CREATE WASM MODULE mine USING BASE64 $1").unwrap();

    // Referencing someone else's module takes USAGE on it.
    let gcd = "CREATE FUNCTION gcd(int, int) RETURNS int LANGUAGE wasm USING MODULE udfs";
    let err = other.batch_execute(gcd).unwrap_err();
    assert_contains!(message(&err), "permission denied for WASM MODULE");
    owner
        .batch_execute("GRANT USAGE ON WASM MODULE udfs TO other")
        .unwrap();
    other.batch_execute(gcd).unwrap();
    let row = other.query_one("SELECT gcd(12, 18)", &[]).unwrap();
    assert_eq!(row.get::<_, i32>(0), 6);

    // Calling a function takes USAGE on its schema.
    execute_each(
        &mut owner,
        &[
            "CREATE SCHEMA private",
            "CREATE FUNCTION private.gcd(int, int) RETURNS int LANGUAGE wasm USING MODULE udfs",
        ],
    );
    let err = other
        .query_one("SELECT private.gcd(12, 18)", &[])
        .unwrap_err();
    assert_contains!(message(&err), "permission denied for SCHEMA");
    owner
        .batch_execute("GRANT USAGE ON SCHEMA private TO other")
        .unwrap();
    let row = other.query_one("SELECT private.gcd(12, 18)", &[]).unwrap();
    assert_eq!(row.get::<_, i32>(0), 6);
}
