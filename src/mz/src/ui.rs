// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Terminal user interface utilities.

use std::fmt;
use std::io;
use std::io::Write;
use std::time::Duration;

use indicatif::ProgressBar;
use indicatif::ProgressStyle;
use mz_ore::option::OptionExt;

use serde::{Deserialize, Serialize};
use serde_aux::serde_introspection::serde_introspect;
use tabled::{Style, Table, Tabled};
use termcolor::{Color, ColorChoice, ColorSpec, StandardStream, WriteColor};

use crate::error::Error;

/// Specifies an output format.
#[derive(Debug, Clone, clap::ValueEnum)]
pub enum OutputFormat {
    /// Text output.
    Text,
    /// JSON output.
    Json,
    /// CSV output.
    Csv,
}

/// Formats terminal output according to the configured [`OutputFormat`].
#[derive(Clone)]
pub struct OutputFormatter {
    output_format: OutputFormat,
    no_color: bool,
}

/// Ticks displayed for loading without using colors
const TICKS: [&str; 9] = ["⣾", "⣽", "⣻", "⢿", "⡿", "⣟", "⣯", "⣷", ""];

impl OutputFormatter {
    /// Creates a new output formatter that uses the specified output format.
    pub fn new(output_format: OutputFormat, no_color: bool) -> OutputFormatter {
        OutputFormatter {
            output_format,
            no_color,
        }
    }

    /// Prints a message with color
    pub fn print_with_color(&self, message: &str, color: Color, stderr: bool) -> Result<(), Error> {
        let mut stdeo = match stderr {
            true => StandardStream::stderr(ColorChoice::Always),
            false => StandardStream::stdout(ColorChoice::Always),
        };
        stdeo.set_color(ColorSpec::new().set_fg(Some(color)))?;
        write!(&mut stdeo, "{}", message)?;

        // Reset the std err/out to original setting.
        let _ = stdeo.reset();

        Ok(())
    }

    /// Outputs a prefix warning.
    pub fn output_warning(&self, msg: &str) -> Result<(), Error> {
        if self.no_color {
            eprintln!("\n* Warning * {}", msg);
        } else {
            let _ = self.print_with_color("\n* Warning *", Color::Yellow, true)?;
            eprintln!(" {}", msg);
        }

        Ok(())
    }

    /// Outputs a single value.
    pub fn output_scalar(&self, scalar: Option<&str>) -> Result<(), Error> {
        match self.output_format {
            OutputFormat::Text => println!("{}", scalar.display_or("<unset>")),
            OutputFormat::Json => serde_json::to_writer(io::stdout(), &scalar)?,
            OutputFormat::Csv => {
                let mut w = csv::Writer::from_writer(io::stdout());
                w.write_record([scalar.unwrap_or("<unset>")])?;
                w.flush()?;
            }
        }
        Ok(())
    }

    /// Outputs a table.
    ///
    /// The provided rows must derive [`Deserialize`], [`Serialize`], and
    /// [`Tabled`]. The `Serialize` implementation is used for CSV and JSON
    /// output. The `Deserialize` implementation is used to determine column
    /// names for CSV output when no rows are present. The `Tabled`
    /// implementation is used for text output.
    pub fn output_table<'a, I, R>(&self, rows: I) -> Result<(), Error>
    where
        I: IntoIterator<Item = R>,
        R: Deserialize<'a> + Serialize + Tabled,
    {
        match self.output_format {
            OutputFormat::Text => {
                let table = Table::new(rows).with(Style::psql()).to_string();
                println!("{table}");
            }
            OutputFormat::Json => {
                let rows = rows.into_iter().collect::<Vec<_>>();
                serde_json::to_writer(io::stdout(), &rows)?;
            }
            OutputFormat::Csv => {
                let mut w = csv::WriterBuilder::new()
                    .has_headers(false)
                    .from_writer(io::stdout());
                w.write_record(serde_introspect::<R>())?;
                for row in rows {
                    w.serialize(row)?;
                }
                w.flush()?;
            }
        }
        Ok(())
    }

    /// Prints a loading spinner followed by a message, until finished.
    pub fn loading_spinner(&self, message: &str) -> ProgressBar {
        let progress_bar = ProgressBar::new_spinner();
        progress_bar.enable_steady_tick(Duration::from_millis(120));

        let tick_strings: Vec<&str> = match self.no_color {
            true => TICKS.to_vec(),
            false => TICKS.to_vec(),
        };

        progress_bar.set_style(
            ProgressStyle::default_spinner()
                .template("{spinner:1.green/green} {msg}")
                .expect("template known to be valid")
                // For more spinners check out the cli-spinners project:
                // https://github.com/sindresorhus/cli-spinners/blob/master/spinners.json
                .tick_strings(&tick_strings),
        );

        progress_bar.set_message(message.to_string());

        progress_bar
    }
}

/// An optional `str` that renders as `<unset>` when `None`.
#[derive(Serialize, Deserialize)]
#[serde(transparent)]
pub struct OptionalStr<'a>(pub Option<&'a str>);

impl fmt::Display for OptionalStr<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match &self.0 {
            None => f.write_str("<unset>"),
            Some(s) => s.fmt(f),
        }
    }
}
