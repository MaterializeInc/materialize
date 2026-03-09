//! Delete command — drop an object from Materialize and remove its project file.

use crate::cli::CliError;
use crate::cli::progress;
use crate::client::{Client, ConnectionError, quote_identifier};
use crate::config::Settings;
use crate::project::object_id::ObjectId;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::time::Instant;

/// The kind of object to delete.
#[derive(Debug, Clone, Copy)]
pub enum ObjectKind {
    Cluster,
    Connection,
    NetworkPolicy,
    Role,
    Secret,
    Table,
}

impl ObjectKind {
    fn label(&self) -> &'static str {
        match self {
            ObjectKind::Cluster => "cluster",
            ObjectKind::Connection => "connection",
            ObjectKind::NetworkPolicy => "network policy",
            ObjectKind::Role => "role",
            ObjectKind::Secret => "secret",
            ObjectKind::Table => "table",
        }
    }

    fn sql_keyword(&self) -> &'static str {
        match self {
            ObjectKind::Cluster => "CLUSTER",
            ObjectKind::Connection => "CONNECTION",
            ObjectKind::NetworkPolicy => "NETWORK POLICY",
            ObjectKind::Role => "ROLE",
            ObjectKind::Secret => "SECRET",
            ObjectKind::Table => "TABLE",
        }
    }
}

/// A resolved delete target: the parsed name, file path, and DROP SQL.
struct DeleteTarget {
    file_path: PathBuf,
    drop_sql: String,
}

impl DeleteTarget {
    fn resolve(directory: &Path, kind: ObjectKind, name: &str) -> Result<Self, CliError> {
        let keyword = kind.sql_keyword();
        let (file_path, drop_sql) = match kind {
            ObjectKind::Cluster => (
                directory.join("clusters").join(format!("{}.sql", name)),
                format!("DROP {} {}", keyword, quote_identifier(name)),
            ),
            ObjectKind::NetworkPolicy => (
                directory
                    .join("network_policies")
                    .join(format!("{}.sql", name)),
                format!("DROP {} {}", keyword, quote_identifier(name)),
            ),
            ObjectKind::Role => (
                directory.join("roles").join(format!("{}.sql", name)),
                format!("DROP {} {}", keyword, quote_identifier(name)),
            ),
            ObjectKind::Connection | ObjectKind::Secret | ObjectKind::Table => {
                let oid = ObjectId::from_fqn(name).map_err(CliError::Message)?;
                (
                    directory
                        .join("models")
                        .join(oid.database())
                        .join(oid.schema())
                        .join(format!("{}.sql", oid.object())),
                    format!(
                        "DROP {} {}.{}.{}",
                        keyword,
                        quote_identifier(oid.database()),
                        quote_identifier(oid.schema()),
                        quote_identifier(oid.object()),
                    ),
                )
            }
        };
        Ok(Self {
            file_path,
            drop_sql,
        })
    }
}

/// Run the `delete` command.
///
/// Drops the named object from Materialize and removes the corresponding
/// project file. Prompts for confirmation unless `yes` is true.
pub async fn run(
    settings: &Settings,
    kind: ObjectKind,
    name: &str,
    yes: bool,
) -> Result<(), CliError> {
    let profile = settings.connection();
    let directory = &settings.directory;
    let start_time = Instant::now();
    let label = kind.label();

    let target = DeleteTarget::resolve(directory, kind, name)?;
    if !target.file_path.exists() {
        return Err(CliError::Message(format!(
            "'{name}' is not managed by this project (no file at {})",
            target.file_path.display()
        )));
    }

    if !yes {
        println!(
            "This will drop {} '{}' from Materialize and remove {}",
            label,
            name,
            target.file_path.display()
        );
        print!("Continue? [y/N] ");
        io::stdout().flush()?;
        let mut input = String::new();
        io::stdin().read_line(&mut input)?;
        if !input.trim().eq_ignore_ascii_case("y") {
            println!("Aborted.");
            return Ok(());
        }
    }

    progress::stage_start(&format!("Dropping {} '{}'", label, name));
    let client = Client::connect_with_profile(profile.clone())
        .await
        .map_err(CliError::Connection)?;

    client.execute(&target.drop_sql, &[]).await.map_err(|e| {
        let msg = e.to_string();
        if msg.contains("depended upon") || msg.contains("depends on") {
            CliError::Message(format!(
                "cannot drop {} '{}' because other objects depend on it.\n\
                 Drop the dependent objects first, then retry.",
                label, name
            ))
        } else {
            CliError::Connection(ConnectionError::Message(format!(
                "failed to drop {} '{}': {}",
                label, name, e
            )))
        }
    })?;

    if let Err(e) = std::fs::remove_file(&target.file_path) {
        progress::warn(&format!(
            "DROP succeeded but failed to remove {}: {}",
            target.file_path.display(),
            e
        ));
        return Err(e.into());
    }

    progress::stage_success(
        &format!(
            "Dropped {} '{}' and removed {}",
            label,
            name,
            target.file_path.display()
        ),
        start_time.elapsed(),
    );

    Ok(())
}
