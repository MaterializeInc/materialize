// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use std::error::Error;
use std::fs;
use std::io::Write;
use std::path::Path;
use std::process::{Command, Stdio};
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
        client.execute(
            &*format!("DROP VIEW IF EXISTS {0}_{1}", source, "mirror"),
            &[],
        )?;
        thread::sleep(Duration::from_secs(1));
        client.execute(
            &*format!("CREATE VIEW {0}_{1} AS SELECT * FROM {0}", source, "mirror"),
            &[],
        )?;
        thread::sleep(Duration::from_secs(1));
        Ok(client
            .query(
                &*format!("SELECT * FROM {}_{} ORDER BY 1", source, "mirror"),
                &[],
            )?
            .into_iter()
            .map(|row| (row.get(0), row.get(1), row.get(2)))
            .collect::<Vec<(String, String, String)>>())
    };

    // macOS doesn't send filesystem events to the process that caused them, so
    // this function appends data to a file via a `tee` child process so that
    // the dataflow layer actually gets notified of the changes.
    let append = |path, data| -> Result<_, Box<dyn Error>> {
        let mut child = Command::new("tee")
            .arg("-a")
            .arg(path)
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .spawn()?;
        child.stdin.as_mut().unwrap().write_all(data)?;
        child.wait()?;
        Ok(())
    };

    let static_path = Path::join(temp_dir.path(), "static.csv");
    let dynamic_path = Path::join(temp_dir.path(), "dynamic.csv");

    fs::write(
        &static_path,
        "Rochester,NY,14618
New York,NY,10004
\"Rancho Santa Margarita\",CA,92679
",
    )?;

    let line1 = ("New York".into(), "NY".into(), "10004".into());
    let line2 = ("Rancho Santa Margarita".into(), "CA".into(), "92679".into());
    let line3 = ("Rochester".into(), "NY".into(), "14618".into());

    client.execute(
        &*format!(
            "CREATE SOURCE static_csv FROM 'file://{}' WITH (format = 'csv', columns = 3)",
            static_path.display(),
        ),
        &[],
    )?;

    assert_eq!(
        fetch_rows(&mut client, "static_csv")?,
        &[line1.clone(), line2.clone(), line3.clone()],
    );

    append(&dynamic_path, b"")?;

    client.execute(&*format!(
        "CREATE SOURCE dynamic_csv FROM 'file://{}' WITH (format = 'csv', columns = 3, tail = true)",
        dynamic_path.display()
    ), &[])?;

    append(&dynamic_path, b"New York,NY,10004\n")?;
    assert_eq!(fetch_rows(&mut client, "dynamic_csv")?, &[line1.clone()]);

    append(&dynamic_path, b"Rochester,NY,14618\n")?;
    assert_eq!(
        fetch_rows(&mut client, "dynamic_csv")?,
        &[line1.clone(), line3.clone()]
    );

    append(&dynamic_path, b"\"Rancho Santa Margarita\",CA,92679\n")?;
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
        let mut tail_reader = Box::pin(client.copy_out("TAIL dynamic_csv_mirror").await?);

        append(&dynamic_path, b"City 1,ST,00001\n")?;
        assert!(tail_reader
            .try_next()
            .await?
            .unwrap()
            .starts_with(&b"City 1\tST\t00001\tDiff: 1 at "[..]));

        append(&dynamic_path, b"City 2,ST,00002\n")?;
        assert!(tail_reader
            .try_next()
            .await?
            .unwrap()
            .starts_with(&b"City 2\tST\t00002\tDiff: 1 at "[..]));

        // The tail won't end until a cancellation request is sent.
        client.cancel_query(tokio_postgres::NoTls).await?;

        assert_eq!(tail_reader.try_next().await?, None);
        Ok::<_, Box<dyn Error>>(())
    })?;

    // Check that writing to the tailed file after the view and source are dropped doesn't
    // cause a crash (#1361).
    client.execute("DROP VIEW dynamic_csv_mirror", &[])?;
    client.execute("DROP SOURCE dynamic_csv", &[])?;
    thread::sleep(Duration::from_millis(100));
    append(&dynamic_path, b"Glendale,AZ,85310\n")?;
    thread::sleep(Duration::from_millis(100));
    Ok(())
}
