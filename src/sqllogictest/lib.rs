use sqlparser::dialect::AnsiSqlDialect;
use sqlparser::sqlast::SQLStatement;

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
    use std::io::prelude::*;

    use walkdir::WalkDir;

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
