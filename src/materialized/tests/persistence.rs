// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use std::error::Error;

pub mod util;

#[test]
fn test_persistence() -> Result<(), Box<dyn Error>> {
    ore::log::init();

    let data_directory = tempfile::tempdir()?;
    let config = util::Config::default()
        .data_directory(data_directory.path().to_owned())
        .bootstrap_sql(
            "CREATE VIEW bootstrap1 AS SELECT 1;
             CREATE VIEW bootstrap2 AS SELECT * FROM bootstrap1;",
        );

    {
        let (_server, mut client) = util::start_server(config.clone())?;
        // TODO(benesch): when file sources land, use them here. Creating a
        // populated Kafka source here is too annoying.
        client.execute("CREATE VIEW constant AS SELECT 1", &[])?;
        client.execute(
            "CREATE VIEW logging_derived AS SELECT * FROM mz_arrangement_sizes",
            &[],
        )?;
    }

    {
        let (_server, mut client) = util::start_server(config)?;
        let rows: Vec<String> = client
            .query("SHOW VIEWS", &[])?
            .into_iter()
            .map(|row| row.get(0))
            .collect();
        assert_eq!(
            rows,
            &[
                "bootstrap1",
                "bootstrap2",
                "constant",
                "logging_derived",
                "mz_arrangement_sharing",
                "mz_arrangement_sizes",
                "mz_catalog_names",
                "mz_dataflow_channels",
                "mz_dataflow_operator_addresses",
                "mz_dataflow_operators",
                "mz_materialization_dependencies",
                "mz_materializations",
                "mz_peek_active",
                "mz_peek_durations",
                "mz_scheduling_elapsed",
                "mz_scheduling_histogram",
                "mz_scheduling_parks",
                "mz_view_foreign_keys",
                "mz_view_frontiers",
                "mz_view_keys",
            ]
        );
    }

    Ok(())
}
