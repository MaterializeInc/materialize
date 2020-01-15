// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use std::error::Error;
use std::fs;
use std::io::Write;
use std::path::Path;
use std::thread;
use std::time::Duration;

use futures::stream::TryStreamExt;
use tokio::runtime::Runtime;

pub mod util;

#[test]
fn test_file_sources() -> Result<(), Box<dyn Error>> {
    ore::log::init();

    let temp_dir = tempfile::tempdir()?;
    let config = util::Config::default();

    let (server, mut client) = util::start_server(config)?;

    let fetch_rows = |client: &mut postgres::Client, source| -> Result<_, Box<dyn Error>> {
        // TODO(benesch): use a blocking SELECT when that exists.
        thread::sleep(Duration::from_secs(1));
        Ok(client
            .query(
                &*format!("SELECT * FROM {} ORDER BY mz_line_no", source),
                &[],
            )?
            .into_iter()
            .map(|row| (row.get(0), row.get(1), row.get(2), row.get(3)))
            .collect::<Vec<(String, String, String, i64)>>())
    };

    let append = |path, data| -> Result<_, Box<dyn Error>> {
        let mut file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?;
        file.write_all(data)?;
        file.sync_all()?;
        // We currently have to poll for changes on macOS every 100ms, so sleep
        // for 200ms to be sure that the new data has been noticed and accepted
        // by materialize.
        thread::sleep(Duration::from_millis(200));
        Ok(())
    };

    let static_path = Path::join(temp_dir.path(), "static.csv");
    let dynamic_path = Path::join(temp_dir.path(), "dynamic.csv");

    fs::write(
        &static_path,
        "Rochester,NY,14618
New York,NY,10004
\"bad,place\"\"\",CA,92679
",
    )?;

    let line1 = ("Rochester".into(), "NY".into(), "14618".into(), 1);
    let line2 = ("New York".into(), "NY".into(), "10004".into(), 2);
    let line3 = ("bad,place\"".into(), "CA".into(), "92679".into(), 3);

    client.batch_execute(&*format!(
        "CREATE SOURCE static_csv_source FROM 'file://{}' WITH (format = 'csv', columns = 3)",
        static_path.display(),
    ))?;
    client.batch_execute("CREATE VIEW static_csv AS SELECT * FROM static_csv_source")?;

    assert_eq!(
        fetch_rows(&mut client, "static_csv")?,
        &[line1.clone(), line2.clone(), line3.clone()],
    );

    append(&dynamic_path, b"")?;

    client.batch_execute(&*format!(
        "CREATE SOURCE dynamic_csv_source FROM 'file://{}' WITH (format = 'csv', columns = 3, tail = true)",
        dynamic_path.display()
    ))?;
    client.batch_execute("CREATE VIEW dynamic_csv AS SELECT * FROM dynamic_csv_source")?;

    append(&dynamic_path, b"Rochester,NY,14618\n")?;
    assert_eq!(fetch_rows(&mut client, "dynamic_csv")?, &[line1.clone()]);

    append(&dynamic_path, b"New York,NY,10004\n")?;
    assert_eq!(
        fetch_rows(&mut client, "dynamic_csv")?,
        &[line1.clone(), line2.clone()]
    );

    append(&dynamic_path, b"\"bad,place\"\"\",CA,92679\n")?;
    assert_eq!(
        fetch_rows(&mut client, "dynamic_csv")?,
        &[line1, line2, line3]
    );

    // Test the TAIL SQL command on the tailed file source. This is end-to-end
    // tailing: changes to the file will propagate through Materialized and
    // into the user's SQL console.
    //
    // We need to use the asynchronous Postgres client here, unless
    // https://github.com/sfackler/rust-postgres/pull/531 gets fixed.
    Runtime::new()?.block_on(async {
        let client = server.connect_async().await?;
        let mut tail_reader = Box::pin(client.copy_out("TAIL dynamic_csv").await?);

        append(&dynamic_path, b"City 1,ST,00001\n")?;
        assert!(tail_reader
            .try_next()
            .await?
            .unwrap()
            .starts_with(&b"City 1\tST\t00001\t4\tDiff: 1 at "[..]));

        append(&dynamic_path, b"City 2,ST,00002\n")?;
        assert!(tail_reader
            .try_next()
            .await?
            .unwrap()
            .starts_with(&b"City 2\tST\t00002\t5\tDiff: 1 at "[..]));

        // The tail won't end until a cancellation request is sent.
        client.cancel_query(tokio_postgres::NoTls).await?;

        assert_eq!(tail_reader.try_next().await?, None);
        Ok::<_, Box<dyn Error>>(())
    })?;

    // Check that writing to the tailed file after the view and source are
    // dropped doesn't cause a crash (#1361).
    client.execute("DROP VIEW dynamic_csv", &[])?;
    client.execute("DROP SOURCE dynamic_csv_source", &[])?;
    thread::sleep(Duration::from_millis(100));
    append(&dynamic_path, b"Glendale,AZ,85310\n")?;
    thread::sleep(Duration::from_millis(100));
    Ok(())
}
