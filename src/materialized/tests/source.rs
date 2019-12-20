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

pub mod util;

#[test]
fn test_file_sources() -> Result<(), Box<dyn Error>> {
    ore::log::init();

    let temp_dir = tempfile::tempdir()?;
    let config = util::Config::default();

    let (_server, mut client) = util::start_server(config.clone())?;

    let fetch_rows = |client: &mut postgres::Client, source| -> Result<_, Box<dyn Error>> {
        // TODO(benesch): use a blocking SELECT when that exists.
        thread::sleep(Duration::from_secs(1));
        Ok(client
            .query(&*format!("SELECT * FROM {} ORDER BY 1", source), &[])?
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
        &[line1.clone(), line2.clone(), line3.clone()]
    );

    // Check that writing to the file after a source was dropped doesn't cause a crash.
    client.execute("DROP SOURCE dynamic_csv", &[])?;
    std::thread::sleep(Duration::from_millis(100));
    append(&dynamic_path, b"Glendale,AZ,85310\n")?;
    std::thread::sleep(Duration::from_millis(100));
    Ok(())
}
