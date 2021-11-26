// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use atty::Stream;
use similar::{ChangeTag, TextDiff};
use termcolor::{Color, ColorChoice, ColorSpec, StandardStream, WriteColor};

/// Trims trailing whitespace from each line of `s`.
pub fn trim_trailing_space(s: &str) -> String {
    let mut lines: Vec<_> = s.lines().map(|line| line.trim_end()).collect();
    while lines.last().map_or(false, |l| l.is_empty()) {
        lines.pop();
    }
    lines.join("\n")
}

/// Prints a colorized line diff of `expected` and `actual`.
pub fn print_diff(expected: &str, actual: &str) {
    let color_choice = if atty::is(Stream::Stderr) {
        ColorChoice::Auto
    } else {
        ColorChoice::Never
    };
    let mut stderr = StandardStream::stderr(color_choice);
    let diff = TextDiff::from_lines(expected, actual);
    println!("--- expected");
    println!("+++ actual");
    for op in diff.ops() {
        for change in diff.iter_changes(op) {
            let sign = match change.tag() {
                ChangeTag::Delete => {
                    let _ = stderr.set_color(ColorSpec::new().set_fg(Some(Color::Red)));
                    "-"
                }
                ChangeTag::Insert => {
                    let _ = stderr.set_color(ColorSpec::new().set_fg(Some(Color::Green)));
                    "+"
                }
                ChangeTag::Equal => " ",
            };
            print!("{}{}", sign, change);
            let _ = stderr.reset();
        }
    }
}
