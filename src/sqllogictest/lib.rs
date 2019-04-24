//! https://www.sqlite.org/sqllogictest/doc/trunk/about.wiki

use std::path::PathBuf;

use lazy_static::lazy_static;
use regex::Regex;
use walkdir::WalkDir;

use materialize::repr::FType;
use sqlparser::dialect::AnsiSqlDialect;
use sqlparser::sqlast::SQLStatement;

macro_rules! unexpected {
    ( $other:expr ) => {{
        panic!("Unexpected: {}", $other)
    }};
}

#[derive(Debug, Clone)]
pub enum TestRecord<'a> {
    Statement {
        should_run: bool,
        sql: &'a str,
    },
    Query {
        types: Vec<FType>,
        sort: &'a str,
        label: Option<&'a str>,
        sql: &'a str,
        output: &'a str,
    },
    HashThreshold {
        threshold: u64,
    },
}

fn split_at<'a>(input: &mut &'a str, sep: &Regex) -> &'a str {
    match sep.find(input) {
        Some(found) => {
            let result = &input[..found.start()];
            *input = &input[found.end()..];
            result
        }
        None => panic!("Couldn't split {:?} at {}", input, sep),
    }
}

fn parse_types(input: &str) -> Vec<FType> {
    input
        .chars()
        .map(|char| match char {
            'T' => FType::String,
            'I' => FType::Int64,
            'R' => FType::Float64,
            other => unexpected!(other),
        })
        .collect()
}

fn parse_sql<'a>(input: &mut &'a str) -> &'a str {
    lazy_static! {
        static ref QUERY_OUTPUT_REGEX: Regex = Regex::new("(\n----\n|$)").unwrap();
    }
    split_at(input, &QUERY_OUTPUT_REGEX)
}

pub fn parse_test_record(mut input: &str) -> Option<TestRecord> {
    while input != "" {
        lazy_static! {
            static ref COMMENT_AND_LINE_REGEX: Regex = Regex::new("(#[^\n]*)?\r?(\n|$)").unwrap();
        }
        let next_line = split_at(&mut input, &COMMENT_AND_LINE_REGEX).trim();
        if next_line != "" {
            let mut words = next_line.split(' ');
            match words.next().unwrap() {
                "statement" => {
                    let should_run = match words.next().unwrap() {
                        "ok" => true,
                        "error" => false,
                        other => unexpected!(other),
                    };
                    let sql = parse_sql(&mut input);
                    assert!(input == "");
                    return Some(TestRecord::Statement { should_run, sql });
                }
                "query" => {
                    let types = parse_types(words.next().unwrap());
                    let sort = words.next().unwrap();
                    let label = words.next();
                    let sql = parse_sql(&mut input);
                    let output = input;
                    return Some(TestRecord::Query {
                        types,
                        sort,
                        label,
                        sql,
                        output,
                    });
                }
                "hash-threshold" => {
                    let threshold = words.next().unwrap().parse::<u64>().unwrap();
                    assert!(input == "");
                    return Some(TestRecord::HashThreshold { threshold });
                }
                "skipif" | "onlyif" => return None,
                other => unexpected!(other),
            }
        }
    }
    None
}

pub fn parse_test_records(input: &str) -> impl Iterator<Item = TestRecord> {
    lazy_static! {
        static ref DOUBLE_LINE_REGEX: Regex = Regex::new("(\n|\r\n)(\n|\r\n)").unwrap();
    }
    DOUBLE_LINE_REGEX
        .split(input)
        .map(|lines| lines.trim())
        .filter(|lines| *lines != "")
        .filter_map(parse_test_record)
}

pub fn all_test_files() -> impl Iterator<Item = PathBuf> {
    WalkDir::new("../../sqllogictest/test/")
        .into_iter()
        .map(|entry| entry.unwrap().path().to_owned())
        .filter(|path| path.is_file())
}

pub fn run(string: String) {
    if let Ok(stmts) = sqlparser::sqlparser::Parser::parse_sql(&AnsiSqlDialect {}, string) {
        if let [SQLStatement::SQLSelect(query)] = &*stmts {
            let parser = materialize::sql::Parser::new(vec![]);
            let result = parser.parse_view_query(&query);
            drop(result);
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use std::fs::File;
    use std::io::Read;

    #[test]
    #[ignore]
    fn test_parsing() {
        let mut input = String::new();
        for filename in all_test_files() {
            input.clear();
            File::open(filename)
                .unwrap()
                .read_to_string(&mut input)
                .unwrap();
            parse_test_records(&input).for_each(|record| {
                drop(record);
            });
        }
    }

    #[test]
    fn test_artifacts() {
        for entry in WalkDir::new("../../fuzz/artifacts/fuzz_sqllogictest/") {
            let entry = entry.unwrap();
            if entry.path().is_file() {
                let mut contents = String::new();
                File::open(&entry.path())
                    .unwrap()
                    .read_to_string(&mut contents)
                    .unwrap();
                run(contents);
            }
        }
    }
}
