//! Shared helper functions used across LSP endpoint builders.

use mz_sql_parser::ast::{CommentObjectType, CommentStatement, Raw};

/// Map an AST statement to its display type name.
pub fn object_type_name(stmt: &crate::project::ast::Statement) -> &'static str {
    match stmt {
        crate::project::ast::Statement::CreateView(_) => "view",
        crate::project::ast::Statement::CreateMaterializedView(_) => "materialized-view",
        crate::project::ast::Statement::CreateTable(_) => "table",
        crate::project::ast::Statement::CreateTableFromSource(_) => "table-from-source",
        crate::project::ast::Statement::CreateSource(_) => "source",
        crate::project::ast::Statement::CreateSink(_) => "sink",
        crate::project::ast::Statement::CreateSecret(_) => "secret",
        crate::project::ast::Statement::CreateConnection(_) => "connection",
    }
}

/// Extract the object-level description from comment statements.
///
/// Returns the text of the first non-column `COMMENT ON` statement, if any.
pub fn extract_description(comments: &[CommentStatement<Raw>]) -> Option<String> {
    for c in comments {
        if !matches!(c.object, CommentObjectType::Column { .. }) {
            return c.comment.clone();
        }
    }
    None
}
