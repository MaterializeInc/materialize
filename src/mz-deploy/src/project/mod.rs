use std::path::Path;

pub mod ast;
pub mod changeset;
pub mod deployment_snapshot;
pub mod error;
pub mod typed;
pub mod planned;
pub mod normalize;
pub mod object_id;
mod parser;
pub mod raw;

// Re-export commonly used types
pub use planned::ModStatement;

/// Load, validate, and convert a project to a planned deployment representation.
pub fn plan<P: AsRef<Path>>(root: P) -> Result<planned::Project, error::ProjectError> {
    let raw_project = raw::load_project(root)?;
    let typed_project = typed::Project::try_from(raw_project)?;
    let planned_project = planned::Project::from(typed_project);
    Ok(planned_project)
}
