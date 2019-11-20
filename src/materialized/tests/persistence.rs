// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use std::error::Error;

mod util;

#[test]
fn test_persistence() -> Result<(), Box<dyn Error>> {
    ore::log::init();

    let data_dir = tempfile::tempdir()?;
    let data_dir = Some(data_dir.path().to_owned());

    {
        let (_server, conn) = util::start_server(data_dir.clone())?;
        // TODO(benesch): when file sources land, use them here. Creating a
        // populated Kafka source here is too annoying.
        conn.execute("CREATE VIEW persistme AS SELECT 1", &[])?;
    }

    {
        let (_server, conn) = util::start_server(data_dir)?;
        let rows: Vec<String> = conn
            .query("SHOW VIEWS", &[])?
            .into_iter()
            .map(|row| row.get(0))
            .collect();
        assert_eq!(rows, &["persistme"]);
    }

    Ok(())
}
