//! SQL identifier quoting.
//!
//! Provides [`quote_identifier`] for safely double-quoting identifiers in
//! generated SQL statements, escaping any embedded double quotes.

pub fn quote_identifier(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}
