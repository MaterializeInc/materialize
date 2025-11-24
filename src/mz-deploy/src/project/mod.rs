use std::path::Path;

pub mod ast;
pub mod changeset;
pub mod deployment_snapshot;
pub mod error;
pub mod hir;
pub mod mir;
pub mod normalize;
mod parser;
pub mod raw;
pub mod object_id;

// Re-export commonly used types
pub use mir::ModStatement;

/// Load, validate, and convert a project to MIR for deployment planning.
pub fn plan<P: AsRef<Path>>(root: P) -> Result<mir::Project, error::ProjectError> {
    let raw_project = raw::load_project(root)?;
    let hir_project = hir::Project::try_from(raw_project)?;
    let mir_project = mir::Project::from(hir_project);
    Ok(mir_project)
}
