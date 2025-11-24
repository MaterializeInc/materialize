use super::error::ParseError;
use mz_sql_parser::ast::{Raw, Statement};
use std::path::PathBuf;

/// Parses one or more SQL statements from an iterable collection of strings.
pub fn parse_statements<I, S>(raw: I) -> Result<Vec<Statement<Raw>>, ParseError>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let mut statements = vec![];
    for s in raw {
        let parsed_results = mz_sql_parser::parser::parse_statements_with_limit(s.as_ref())
            .map_err(|e| ParseError::StatementsParseFailed {
                message: format!("Parser limit error: {}", e),
            })?
            .map_err(|e| ParseError::StatementsParseFailed {
                message: format!("Parse error: {}", e.error),
            })?;

        let mut parsed: Vec<Statement<Raw>> = parsed_results
            .into_iter()
            .map(|result| result.ast)
            .collect();

        statements.append(&mut parsed);
    }

    Ok(statements)
}

/// Parse SQL statements and add file context to any errors.
///
/// This function directly parses SQL and creates SqlParseFailed errors with full context
/// including file path and SQL content for better error reporting.
pub fn parse_statements_with_context(
    sql: &str,
    path: PathBuf,
) -> Result<Vec<Statement<Raw>>, ParseError> {
    let mut statements = vec![];

    let parsed_results = mz_sql_parser::parser::parse_statements_with_limit(sql)
        .map_err(|e| ParseError::StatementsParseFailed {
            message: format!("Parser limit error in file {}: {}", path.display(), e),
        })?
        .map_err(|e| ParseError::SqlParseFailed {
            path: path.clone(),
            sql: sql.to_string(),
            source: e,
        })?;

    let mut parsed: Vec<Statement<Raw>> = parsed_results
        .into_iter()
        .map(|result| result.ast)
        .collect();

    statements.append(&mut parsed);

    Ok(statements)
}

#[cfg(test)]
mod test {
    use crate::project::parser::parse_statements;

    #[test]
    fn validate() {
        let _ = parse_statements(vec!["CREATE CLUSTER c (INTROSPECTION INTERVAL = 0)"]).unwrap();
    }

    // TODO: Re-enable when mz_sql_parser supports IN CLUSTER for indexes
    // #[test]
    // fn test_index_in_cluster() {
    //     let result = parse_statements(vec!["CREATE INDEX test_idx ON test (id) IN CLUSTER quickstart"]);
    //     println!("Parse result for INDEX: {:?}", result);
    //     assert!(result.is_ok(), "Failed to parse INDEX with IN CLUSTER: {:?}", result.err());
    // }

    #[test]
    fn test_mv_in_cluster() {
        let result = parse_statements(vec![
            "CREATE MATERIALIZED VIEW mv IN CLUSTER quickstart AS SELECT 1",
        ]);
        println!("Parse result for MV: {:?}", result);
        assert!(
            result.is_ok(),
            "Failed to parse MV with IN CLUSTER: {:?}",
            result.err()
        );
    }
}
