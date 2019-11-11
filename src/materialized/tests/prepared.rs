// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use std::error::Error;

use postgres::{Connection, TlsMode};

#[test]
fn test_prepared_statements() -> Result<(), Box<dyn Error>> {
    ore::log::init();
    ore::panic::set_abort_on_panic();

    let _server = materialized::serve(materialized::Config {
        logging_granularity: None,
        version: "TEST".into(),
        threads: 1,
        process: 0,
        addresses: vec!["127.0.0.1:6875".into()],
        sql: "".into(),
        symbiosis_url: None,
        gather_metrics: false,
    })?;

    let conn = Connection::connect("postgresql://root@127.0.0.1:6875", TlsMode::None)?;

    for row in &conn.query("SELECT $1", &[&String::from("42")])? {
        let val: String = row.get(0);
        assert_eq!(val, "42");
    }

    Ok(())
}
