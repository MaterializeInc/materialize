//! Shared mapping from [`ObjectKind`] to LSP [`SymbolKind`].
//!
//! Used by both [`document_symbol`](super::document_symbol) and
//! [`workspace_symbol`](super::workspace_symbol) to present consistent
//! icons in the editor's outline and symbol search.

use crate::types::ObjectKind;
use tower_lsp::lsp_types::SymbolKind;

/// Map an [`ObjectKind`] to an LSP [`SymbolKind`].
///
/// LSP has no SQL-specific symbol kinds, so we pick the closest semantic match:
///
/// | ObjectKind        | SymbolKind | Rationale                          |
/// |-------------------|------------|------------------------------------|
/// | Table             | STRUCT     | Structured row data                |
/// | View              | FUNCTION   | Computed from a query              |
/// | MaterializedView  | CLASS      | Persistent computed relation       |
/// | Source            | INTERFACE  | External data contract             |
/// | Sink              | MODULE     | Data export target                 |
/// | Secret            | CONSTANT   | Opaque stored value                |
/// | Connection        | NAMESPACE  | Named config grouping              |
pub fn object_kind_to_symbol_kind(kind: ObjectKind) -> SymbolKind {
    match kind {
        ObjectKind::Table => SymbolKind::STRUCT,
        ObjectKind::View => SymbolKind::FUNCTION,
        ObjectKind::MaterializedView => SymbolKind::CLASS,
        ObjectKind::Source => SymbolKind::INTERFACE,
        ObjectKind::Sink => SymbolKind::MODULE,
        ObjectKind::Secret => SymbolKind::CONSTANT,
        ObjectKind::Connection => SymbolKind::NAMESPACE,
    }
}
