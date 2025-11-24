//! No-op type checker that always passes.

use super::typechecker::{TypeCheckError, TypeChecker};
use crate::project::mir::Project;
use crate::verbose;

/// A no-op type checker that always passes
///
/// This is used when:
/// - Docker is not available
/// - Type checking is explicitly skipped
/// - Running in environments where containers are not supported
#[derive(Debug, Default)]
pub struct NoOpTypeChecker;

impl NoOpTypeChecker {
    /// Create a new no-op type checker
    pub fn new() -> Self {
        Self
    }
}

#[async_trait::async_trait]
impl TypeChecker for NoOpTypeChecker {
    async fn typecheck(&self, project: &Project) -> Result<(), TypeCheckError> {
        verbose!("Skipping type checking (no-op type checker)");
        verbose!("  {} databases", project.databases.len());
        verbose!("  {} objects", project.iter_objects().count());
        verbose!("  {} external dependencies", project.external_dependencies.len());
        Ok(())
    }
}