// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// BEGIN LINT CONFIG
// DO NOT EDIT. Automatically generated by bin/gen-lints.
// Have complaints about the noise? See the note in misc/python/materialize/cli/gen-lints.py first.
#![allow(unknown_lints)]
#![allow(clippy::style)]
#![allow(clippy::complexity)]
#![allow(clippy::large_enum_variant)]
#![allow(clippy::mutable_key_type)]
#![allow(clippy::stable_sort_primitive)]
#![allow(clippy::map_entry)]
#![allow(clippy::box_default)]
#![allow(clippy::drain_collect)]
#![warn(clippy::bool_comparison)]
#![warn(clippy::clone_on_ref_ptr)]
#![warn(clippy::no_effect)]
#![warn(clippy::unnecessary_unwrap)]
#![warn(clippy::dbg_macro)]
#![warn(clippy::todo)]
#![warn(clippy::wildcard_dependencies)]
#![warn(clippy::zero_prefixed_literal)]
#![warn(clippy::borrowed_box)]
#![warn(clippy::deref_addrof)]
#![warn(clippy::double_must_use)]
#![warn(clippy::double_parens)]
#![warn(clippy::extra_unused_lifetimes)]
#![warn(clippy::needless_borrow)]
#![warn(clippy::needless_question_mark)]
#![warn(clippy::needless_return)]
#![warn(clippy::redundant_pattern)]
#![warn(clippy::redundant_slicing)]
#![warn(clippy::redundant_static_lifetimes)]
#![warn(clippy::single_component_path_imports)]
#![warn(clippy::unnecessary_cast)]
#![warn(clippy::useless_asref)]
#![warn(clippy::useless_conversion)]
#![warn(clippy::builtin_type_shadow)]
#![warn(clippy::duplicate_underscore_argument)]
#![warn(clippy::double_neg)]
#![warn(clippy::unnecessary_mut_passed)]
#![warn(clippy::wildcard_in_or_patterns)]
#![warn(clippy::crosspointer_transmute)]
#![warn(clippy::excessive_precision)]
#![warn(clippy::overflow_check_conditional)]
#![warn(clippy::as_conversions)]
#![warn(clippy::match_overlapping_arm)]
#![warn(clippy::zero_divided_by_zero)]
#![warn(clippy::must_use_unit)]
#![warn(clippy::suspicious_assignment_formatting)]
#![warn(clippy::suspicious_else_formatting)]
#![warn(clippy::suspicious_unary_op_formatting)]
#![warn(clippy::mut_mutex_lock)]
#![warn(clippy::print_literal)]
#![warn(clippy::same_item_push)]
#![warn(clippy::useless_format)]
#![warn(clippy::write_literal)]
#![warn(clippy::redundant_closure)]
#![warn(clippy::redundant_closure_call)]
#![warn(clippy::unnecessary_lazy_evaluations)]
#![warn(clippy::partialeq_ne_impl)]
#![warn(clippy::redundant_field_names)]
#![warn(clippy::transmutes_expressible_as_ptr_casts)]
#![warn(clippy::unused_async)]
#![warn(clippy::disallowed_methods)]
#![warn(clippy::disallowed_macros)]
#![warn(clippy::disallowed_types)]
#![warn(clippy::from_over_into)]
// END LINT CONFIG

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
use mz_pgrepr::Interval;

#[mz_ore::test]
fn test_pg_timezone_abbrevs() {
    let config = test_util::Config::default().unsafe_mode();
    let server = test_util::start_server(config).unwrap();
    let mut client = server.connect(postgres::NoTls).unwrap();

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
            assert!(pg_map.insert(key, line).is_none());
        }

        client
            .batch_execute(&format!(
                "SET unsafe_new_transaction_wall_time = '{date} 00:00:00 UTC'"
            ))
            .unwrap();

        let mut mismatches = Vec::new();
        for row in client
            .query(&format!("SELECT * FROM pg_timezone_{name}"), &[])
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
