// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// This file does comparison tests between postgres and materialize for the pg_timezone_abbrevs and
// pg_timezone_names tables. The output of those is dependent on the system clock. We have ways to
// mock the wall clock to simulate arbitrary timestamps. We're not aware of a way to mock the time
// easily in postgres beyond changing the system clock, which is not a thing we want to do. To
// ensure we have correct timezone output, we dump postgres' output for those tables to testdata
// files and compare them here. The method below should be used when we need to recreate those
// files.

// 1) Set the date of your computer to mid December:
// > sudo date --set="2023-12-15T00:00:00-00:00"
// 2) Extract the catalog tables to testdata files>
// > echo "SELECT * FROM pg_catalog.pg_timezone_names ORDER BY name" | psql -h 127.0.0.1 -U postgres -X -q -t -A -F"," > testdata/timezones/names-2023-12-15.csv
// > echo "SELECT * FROM pg_catalog.pg_timezone_abbrevs ORDER BY abbrev" | psql -h 127.0.0.1 -U postgres -X -q -t -A -F"," > testdata/timezones/abbrevs-2023-12-15.csv
// 3) Set the date of your computer to mid June:
// > sudo date --set="2023-06-15T00:00:00-00:00"
// 4) Extract the catalog tables to testdata files>
// > echo "SELECT * FROM pg_catalog.pg_timezone_names ORDER BY name" | psql -h 127.0.0.1 -U postgres -X -q -t -A -F"," > testdata/timezones/names-2023-06-15.csv
// > echo "SELECT * FROM pg_catalog.pg_timezone_abbrevs ORDER BY abbrev" | psql -h 127.0.0.1 -U postgres -X -q -t -A -F"," > testdata/timezones/abbrevs-2023-06-15.csv

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::Path;

use mz_environmentd::test_util;
use mz_ore::assert_none;
use mz_pgrepr::Interval;

#[mz_ore::test(tokio::test(flavor = "multi_thread", worker_threads = 1))]
async fn test_pg_timezone_abbrevs() {
    let server = test_util::TestHarness::default()
        .unsafe_mode()
        .start()
        .await;
    let client = server.connect().await.unwrap();

    // These are either not present in postgres or differ from postgres. Allowlist them because
    // there are perhaps differences in the timezone databases we use.
    let allowed_mismatches = BTreeSet::from([
        "names-2023-06-15-America/Bahia_Banderas",
        "names-2023-06-15-America/Chihuahua",
        "names-2023-06-15-America/Ciudad_Juarez",
        "names-2023-06-15-America/Godthab",
        "names-2023-06-15-America/Mazatlan",
        "names-2023-06-15-America/Merida",
        "names-2023-06-15-America/Mexico_City",
        "names-2023-06-15-America/Monterrey",
        "names-2023-06-15-America/Nuuk",
        "names-2023-06-15-America/Ojinaga",
        "names-2023-06-15-Asia/Amman",
        "names-2023-06-15-Asia/Damascus",
        "names-2023-06-15-Asia/Tehran",
        "names-2023-06-15-Europe/Kyiv",
        "names-2023-06-15-Iran",
        "names-2023-06-15-Mexico/BajaSur",
        "names-2023-06-15-Mexico/General",
        "names-2023-06-15-Pacific/Kanton",
        "names-2023-12-15-America/Chihuahua",
        "names-2023-12-15-America/Ciudad_Juarez",
        "names-2023-12-15-America/Godthab",
        "names-2023-12-15-America/Nuuk",
        "names-2023-12-15-America/Ojinaga",
        "names-2023-12-15-Asia/Amman",
        "names-2023-12-15-Asia/Damascus",
        "names-2023-12-15-Europe/Kyiv",
        "names-2023-12-15-Pacific/Fiji",
        "names-2023-12-15-Pacific/Kanton",
    ]);

    let entries = fs::read_dir(Path::new("tests").join("testdata").join("timezones")).unwrap();
    for entry in entries {
        let entry = entry.unwrap();
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let (name, date) = path
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .split_once('-')
            .unwrap();
        let date = date.strip_suffix(".csv").unwrap();
        let mut pg_map = BTreeMap::new();
        let contents = fs::read_to_string(&path).unwrap();
        for line in contents.lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            let key = line.split_once(',').unwrap().0;
            assert_none!(pg_map.insert(key, line));
        }

        client
            .batch_execute(&format!(
                "SET unsafe_new_transaction_wall_time = '{date} 00:00:00 UTC'"
            ))
            .await
            .unwrap();

        let mut mismatches = Vec::new();
        for row in client
            .query(&format!("SELECT * FROM pg_timezone_{name}"), &[])
            .await
            .unwrap()
        {
            let (key, mz_entry) = match name {
                "abbrevs" => {
                    let abbrev: String = row.get(0);
                    let utc_offset: Interval = row.get(1);
                    let is_dst = if row.get::<_, bool>(2) { "t" } else { "f" };
                    (abbrev.clone(), format!("{abbrev},{utc_offset},{is_dst}"))
                }
                "names" => {
                    let name: String = row.get(0);
                    let abbrev: String = row.get(1);
                    let utc_offset: Interval = row.get(2);
                    let is_dst = if row.get::<_, bool>(3) { "t" } else { "f" };
                    (
                        name.clone(),
                        format!("{name},{abbrev},{utc_offset},{is_dst}"),
                    )
                }
                _ => unreachable!(),
            };
            let Some(pg_entry) = pg_map.get(key.as_str()) else {
                mismatches.push(format!("{name}-{date}-{key}"));
                continue;
            };
            if pg_entry != &mz_entry {
                mismatches.push(format!("{name}-{date}-{key}"));
                continue;
            }
        }
        for mismatch in mismatches {
            if !allowed_mismatches.contains(mismatch.as_str()) {
                panic!("unexpected mismatch: {mismatch}");
            }
        }
    }
}
