// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// This file does comparison tests between postgres and materialize for the pg_timezone_abbrevs and
// pg_timezone_names tables. The output of those is dependent on the system clock. We can mock
// Materialize's wall clock to simulate arbitrary timestamps, but not postgres's, so the postgres
// side is a set of CSV dumps taken from a postgres that was lied to about the time via libfaketime.
// Regenerate them with testdata/timezones/regenerate.sh, which also documents the constraint that
// the postgres container's IANA tzdata version must match the one compiled into Materialize via
// chrono-tz. The date in each CSV filename is the instant the snapshot asserts about, not when it
// was taken.

#![recursion_limit = "256"]

use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

use mz_environmentd::test_util;
use mz_ore::assert_none;
use mz_pgrepr::Interval;

#[mz_ore::test(tokio::test(flavor = "multi_thread", worker_threads = 1))]
#[allow(clippy::disallowed_methods)]
async fn test_pg_timezone_abbrevs() {
    let server = test_util::TestHarness::default()
        .unsafe_mode()
        .start()
        .await;
    let client = server.connect().await.unwrap();

    let entries = fs::read_dir(Path::new("tests").join("testdata").join("timezones")).unwrap();
    for entry in entries {
        let entry = entry.unwrap();
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        // The directory also holds regenerate.sh. Only <table>-<date>.csv
        // files are snapshots.
        let Some(stem) = path
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .strip_suffix(".csv")
        else {
            continue;
        };
        let (name, date) = stem.split_once('-').unwrap();
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
        // The snapshots come from a postgres whose IANA tzdata matches the
        // version compiled into Materialize (see the regeneration script), so
        // a mismatch is a real divergence from postgres, never excusable as
        // database-version skew.
        if !mismatches.is_empty() {
            panic!("mismatches with postgres snapshots: {mismatches:#?}");
        }
    }
}
