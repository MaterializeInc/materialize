//! https://www.sqlite.org/sqllogictest/doc/trunk/about.wiki

use std::fs::File;
use std::io::Read;
use std::io::Write;
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

pub enum TestRecord {
    Statement {
        should_run: bool,
        sql: String,
    },
    Query {
        types: Vec<FType>,
        sort: String,
        label: Option<String>,
        sql: String,
    },
    HashThreshold {
        threshold: u64,
    },
}

pub struct Test {
    filename: PathBuf,
    records: Vec<TestRecord>,
}

pub fn parse_types(input: &str) -> Vec<FType> {
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

pub fn parse_sql<'a, I: Iterator<Item = &'a str>>(lines: &mut I) -> String {
    let mut sql = String::new();
    while let Some(line) = lines.next() {
        if line == "----" {
            break;
        } else {
            sql.push_str(line);
        }
    }
    assert!(sql != "");
    sql
}

pub fn parse_test_record(input: &str) -> Option<TestRecord> {
    dbg!(input);
    let mut lines = input
        .lines()
        .filter(|line| !line.starts_with("#") && !(*line == ""));
    lazy_static! {
        static ref COMMENT_REGEX: Regex = Regex::new(r"#(.*)").unwrap();
    }
    let first_line = COMMENT_REGEX.replace(lines.next()?, "");
    let words = first_line.trim().split(" ").collect::<Vec<_>>();
    dbg!(&words);
    match *words {
        ["statement", should_run] => {
            let should_run = match should_run {
                "ok" => true,
                "error" => false,
                other => unexpected!(other),
            };
            let sql = parse_sql(&mut lines);
            Some(TestRecord::Statement { should_run, sql })
        }
        ["query", types, sort] => {
            let types = parse_types(types);
            let sort = sort.to_string();
            let label = None;
            let sql = parse_sql(&mut lines);
            Some(TestRecord::Query {
                types,
                sort,
                label,
                sql,
            })
        }
        ["query", types, sort, label] => {
            let types = parse_types(types);
            let sort = sort.to_string();
            let label = Some(label.to_string());
            let sql = parse_sql(&mut lines);
            Some(TestRecord::Query {
                types,
                sort,
                label,
                sql,
            })
        }
        ["hash-threshold", threshold] => {
            assert!(lines.next().is_none());
            let threshold = threshold.parse().unwrap();
            Some(TestRecord::HashThreshold { threshold })
        }
        ["skipif", _db] | ["onlyif", _db] => None,
        _ => unexpected!(first_line),
    }
}

pub fn parse_test_file(filename: PathBuf) -> Test {
    dbg!(&filename);
    let mut input = String::new();
    File::open(&filename)
        .unwrap()
        .read_to_string(&mut input)
        .unwrap();
    let records = input
        .replace("\r", "")
        .split("\n\n")
        .map(|lines| lines.trim())
        .filter(|lines| *lines != "")
        .filter_map(parse_test_record)
        .collect();
    Test {
        filename: filename,
        records: records,
    }
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

    #[test]
    #[ignore]
    fn test_parsing() {
        all_test_files().map(parse_test_file).for_each(drop);
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
