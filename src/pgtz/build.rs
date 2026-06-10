// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file at the
// root of this repository, or online at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::path::PathBuf;
use std::{env, fs};

use anyhow::{Context, Result, bail};
use chrono_tz::TZ_VARIANTS;
use mz_ore_build::codegen::CodegenBuf;
use uncased::UncasedStr;

const DEFAULT_TZNAMES: &str = "tznames/Default";

enum TimezoneAbbrevSpec<'a> {
    FixedOffset { utc_offset_secs: i32, is_dst: bool },
    Tz(&'a str),
}

fn main() -> Result<()> {
    let out_dir = PathBuf::from(env::var_os("OUT_DIR").context("Cannot read OUT_DIR env var")?);

    // Convert the default PostgreSQL timezone abbreviation file into a Rust
    // constants, one for each abbrevation in the file, and the SQL definition
    // of the `pg_timezone_abbrevs` view.
    //
    // See: https://www.postgresql.org/docs/16/datetime-config-files.html
    {
        let mut sql_buf = CodegenBuf::new();
        let mut rust_buf = CodegenBuf::new();
        let mut phf_map = phf_codegen::Map::new();

        sql_buf.writeln("VALUES");

        let tznames = fs::read_to_string(DEFAULT_TZNAMES)?;
        let mut emitted_abbrev = false;
        for (i, line) in tznames.lines().enumerate() {
            let pieces = line.split_ascii_whitespace().collect::<Vec<_>>();

            if let Some(p) = pieces.first() {
                if p.starts_with('#') {
                    // Comment line.
                    continue;
                }
            } else if pieces.len() == 0 {
                // Empty line.
                continue;
            } else if pieces.len() < 2 {
                bail!("line {}: did not find at least two fields", i + 1);
            }

            let abbrev = pieces[0];
            let spec = match pieces[1].parse::<i32>() {
                Ok(utc_offset_secs) => TimezoneAbbrevSpec::FixedOffset {
                    utc_offset_secs,
                    is_dst: pieces.get(2) == Some(&"D"),
                },
                Err(_) => TimezoneAbbrevSpec::Tz(pieces[1]),
            };

            rust_buf.write_block(
                format!("pub const {abbrev}: TimezoneAbbrev = TimezoneAbbrev"),
                |rust_buf| {
                    rust_buf.writeln(format!("abbrev: \"{abbrev}\","));
                    match &spec {
                        TimezoneAbbrevSpec::FixedOffset {
                            utc_offset_secs,
                            is_dst,
                        } => {
                            rust_buf.write_block(
                                "spec: TimezoneAbbrevSpec::FixedOffset",
                                |rust_buf| {
                                    rust_buf.writeln(format!(
                                        "offset: make_fixed_offset({utc_offset_secs}),"
                                    ));
                                    rust_buf.writeln(format!("is_dst: {is_dst},"));
                                },
                            );
                        }
                        TimezoneAbbrevSpec::Tz(name) => {
                            let name = name.replace('/', "__");
                            rust_buf.writeln(format!("spec: TimezoneAbbrevSpec::Tz(Tz::{name})"));
                        }
                    }
                },
            );
            rust_buf.writeln(";");

            let (mz_sql_fixed_offset, mz_sql_is_dst, mz_sql_tz_name) = match &spec {
                TimezoneAbbrevSpec::FixedOffset {
                    utc_offset_secs,
                    is_dst,
                } => {
                    let utc_offset = format!("interval '{utc_offset_secs} seconds'");
                    let is_dst = is_dst.to_string();
                    (utc_offset, is_dst, "NULL".to_string())
                }
                TimezoneAbbrevSpec::Tz(name) => {
                    ("NULL".to_string(), "NULL".to_string(), format!("'{name}'"))
                }
            };
            if emitted_abbrev {
                sql_buf.writeln(",");
            }
            sql_buf.writeln(format!(
                "('{abbrev}', {mz_sql_fixed_offset}, {mz_sql_is_dst}, {mz_sql_tz_name})"
            ));

            phf_map.entry(UncasedStr::new(abbrev), abbrev);

            emitted_abbrev = true;
        }

        sql_buf.end_line();

        rust_buf.writeln(format!(
            "pub static TIMEZONE_ABBREVS: phf::Map<&'static UncasedStr, TimezoneAbbrev> = {};",
            phf_map.build(),
        ));

        fs::write(out_dir.join("abbrev.gen.sql"), sql_buf.into_string())?;
        fs::write(out_dir.join("abbrev.gen.rs"), rust_buf.into_string())?;
    }

    // Convert chrono-tz's list of timezones into the SQL definition of the mz_timezone_names view.
    {
        let mut sql_buf = CodegenBuf::new();
        sql_buf.writeln("VALUES");

        for (i, tz) in TZ_VARIANTS.iter().enumerate() {
            let name = tz.name();
            if i > 0 {
                sql_buf.writeln(",");
            }
            sql_buf.write("(");
            sql_buf.write(format!("'{name}'"));
            sql_buf.write(")");
        }

        fs::write(out_dir.join("timezone.gen.sql"), sql_buf.into_string())?;
    }

    Ok(())
}
