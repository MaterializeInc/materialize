// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_repr::adt::regex::Regex;

pub fn regexp_split_to_array<'a>(text: &'a str, regexp: &Regex) -> Vec<&'a str> {
    // Postgres regex split handling differs a bit from spec regex split, so we can't use
    // regexp.split here. See: https://www.postgresql.org/docs/15/functions-matching.html:
    // > the regexp split functions ignore zero-length matches that occur at the start or end
    // > of the string or immediately after a previous match

    let mut finder = regexp.find_iter(text);
    let mut last = 0;
    let mut found = Vec::new();
    loop {
        match finder.next() {
            None => {
                if last <= text.len() {
                    let s = &text[last..];
                    found.push(s);
                }
                break;
            }
            Some(m) => {
                // Ignore zero length matches at start and end of string.
                if m.end() > 0 && m.start() < text.len() {
                    let matched = &text[last..m.start()];
                    last = m.end();
                    found.push(matched);
                }
            }
        }
    }
    found
}

#[cfg(test)]
mod tests {
    use mz_repr::adt::regex::Regex;

    use crate::regexp_split_to_array;

    fn build_regex(needle: &str, flags: &str) -> Result<Regex, anyhow::Error> {
        let mut case_insensitive = false;
        // Note: Postgres accepts it when both flags are present, taking the last one. We do the same.
        for f in flags.chars() {
            match f {
                'i' => {
                    case_insensitive = true;
                }
                'c' => {
                    case_insensitive = false;
                }
                _ => anyhow::bail!("unexpected regex flags"),
            }
        }
        Regex::new(needle, case_insensitive).map_err(|e| anyhow::anyhow!("{}", e))
    }

    // Assert equivalency to postgres and generate TestCases.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: returning ready events from epoll_wait is not yet implemented
    fn test_pg_regexp_split_array() {
        let Ok(postgres_url) = std::env::var("POSTGRES_URL") else {
            return;
        };
        let mut client = postgres::Client::connect(&postgres_url, postgres::NoTls).unwrap();

        let inputs = vec!["", " ", "  ", "12 34", "12  34", " 12 34 "];
        let regexps = vec!["", "\\s", "\\s+", "\\s*"];
        for input in inputs {
            for re in &regexps {
                let regex = build_regex(re, "").unwrap();
                let pg: Vec<String> = client
                    .query_one("select regexp_split_to_array($1, $2)", &[&input, re])
                    .unwrap()
                    .get(0);
                let mz = regexp_split_to_array(input, &regex);
                assert_eq!(pg, mz);
                // Generate TestCases for static use.
                println!(
                    r#"TestCase {{
                text: "{input}",
                regexp: "{}",
                expect: &{pg:?},
            }},"#,
                    re.replace('\\', "\\\\"),
                );
            }
        }
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // too slow
    fn test_regexp_split_array() {
        // Expected outputs generated from postgres.
        struct TestCase {
            text: &'static str,
            regexp: &'static str,
            expect: &'static [&'static str],
        }
        let tests = vec![
            TestCase {
                text: "",
                regexp: "",
                expect: &[""],
            },
            TestCase {
                text: "",
                regexp: "\\s",
                expect: &[""],
            },
            TestCase {
                text: "",
                regexp: "\\s+",
                expect: &[""],
            },
            TestCase {
                text: "",
                regexp: "\\s*",
                expect: &[""],
            },
            TestCase {
                text: " ",
                regexp: "",
                expect: &[" "],
            },
            TestCase {
                text: " ",
                regexp: "\\s",
                expect: &["", ""],
            },
            TestCase {
                text: " ",
                regexp: "\\s+",
                expect: &["", ""],
            },
            TestCase {
                text: " ",
                regexp: "\\s*",
                expect: &["", ""],
            },
            TestCase {
                text: "  ",
                regexp: "",
                expect: &[" ", " "],
            },
            TestCase {
                text: "  ",
                regexp: "\\s",
                expect: &["", "", ""],
            },
            TestCase {
                text: "  ",
                regexp: "\\s+",
                expect: &["", ""],
            },
            TestCase {
                text: "  ",
                regexp: "\\s*",
                expect: &["", ""],
            },
            TestCase {
                text: "12 34",
                regexp: "",
                expect: &["1", "2", " ", "3", "4"],
            },
            TestCase {
                text: "12 34",
                regexp: "\\s",
                expect: &["12", "34"],
            },
            TestCase {
                text: "12 34",
                regexp: "\\s+",
                expect: &["12", "34"],
            },
            TestCase {
                text: "12 34",
                regexp: "\\s*",
                expect: &["1", "2", "3", "4"],
            },
            TestCase {
                text: "12  34",
                regexp: "",
                expect: &["1", "2", " ", " ", "3", "4"],
            },
            TestCase {
                text: "12  34",
                regexp: "\\s",
                expect: &["12", "", "34"],
            },
            TestCase {
                text: "12  34",
                regexp: "\\s+",
                expect: &["12", "34"],
            },
            TestCase {
                text: "12  34",
                regexp: "\\s*",
                expect: &["1", "2", "3", "4"],
            },
            TestCase {
                text: " 12 34 ",
                regexp: "",
                expect: &[" ", "1", "2", " ", "3", "4", " "],
            },
            TestCase {
                text: " 12 34 ",
                regexp: "\\s",
                expect: &["", "12", "34", ""],
            },
            TestCase {
                text: " 12 34 ",
                regexp: "\\s+",
                expect: &["", "12", "34", ""],
            },
            TestCase {
                text: " 12 34 ",
                regexp: "\\s*",
                expect: &["", "1", "2", "3", "4", ""],
            },
        ];
        for tc in tests {
            let regex = build_regex(tc.regexp, "").unwrap();
            let result = regexp_split_to_array(tc.text, &regex);
            if tc.expect != result {
                println!(
                    "input: `{}`, regex: `{}`, got: {:?}, expect: {:?}",
                    tc.text, tc.regexp, result, tc.expect
                );
            }
        }
    }
}
