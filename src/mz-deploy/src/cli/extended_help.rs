//! Extended help system for mz-deploy.
//!
//! Provides Layer 3 help: detailed usage guides with behavior notes, examples,
//! error recovery guidance, and related commands. Content is loaded at compile
//! time from markdown files in the `help/` directory.

use std::collections::BTreeMap;

/// All known commands with their canonical names and help text.
const COMMANDS: &[(&str, &str)] = &[
    ("abort", include_str!("help/abort.md")),
    ("apply", include_str!("help/apply.md")),
    ("apply-clusters", include_str!("help/apply-clusters.md")),
    (
        "apply-connections",
        include_str!("help/apply-connections.md"),
    ),
    (
        "apply-network-policies",
        include_str!("help/apply-network-policies.md"),
    ),
    ("apply-roles", include_str!("help/apply-roles.md")),
    ("apply-secrets", include_str!("help/apply-secrets.md")),
    ("apply-sources", include_str!("help/apply-sources.md")),
    ("apply-tables", include_str!("help/apply-tables.md")),
    ("compile", include_str!("help/compile.md")),
    ("debug", include_str!("help/debug.md")),
    ("delete", include_str!("help/delete.md")),
    ("deploy", include_str!("help/deploy.md")),
    ("describe", include_str!("help/describe.md")),
    ("list", include_str!("help/list.md")),
    ("lock", include_str!("help/lock.md")),
    ("log", include_str!("help/log.md")),
    ("new", include_str!("help/new.md")),
    ("profiles", include_str!("help/profiles.md")),
    ("wait", include_str!("help/wait.md")),
    ("stage", include_str!("help/stage.md")),
    ("test", include_str!("help/test.md")),
];

/// Aliases that map alternative names to canonical command names.
const ALIASES: &[(&str, &str)] = &[
    ("branches", "list"),
    ("build", "compile"),
    ("clusters", "apply-clusters"),
    ("connections", "apply-connections"),
    ("deployments", "list"),
    ("gen-data-contracts", "lock"),
    ("history", "log"),
    ("network-policies", "apply-network-policies"),
    ("promote", "deploy"),
    ("ready", "wait"),
    ("roles", "apply-roles"),
    ("secrets", "apply-secrets"),
    ("show", "describe"),
    ("sources", "apply-sources"),
    ("tables", "apply-tables"),
];

/// Look up extended help by command name or alias.
///
/// Returns the help text if the name matches a canonical command or a known
/// alias. Returns `None` for unknown commands.
pub fn help_for(name: &str) -> Option<&'static str> {
    let canonical = resolve_alias(name);
    COMMANDS
        .iter()
        .find(|(cmd, _)| *cmd == canonical)
        .map(|(_, text)| *text)
}

/// Concatenate all help texts with delimiters for bulk ingestion.
///
/// Each command's help is preceded by a `--- mz-deploy help <cmd> ---` header,
/// making it easy to parse programmatically.
pub fn all_help() -> String {
    let mut out = String::new();
    for (i, (name, text)) in COMMANDS.iter().enumerate() {
        if i > 0 {
            out.push('\n');
        }
        out.push_str(&format!("--- mz-deploy help {name} ---\n\n"));
        out.push_str(text);
        if !text.ends_with('\n') {
            out.push('\n');
        }
    }
    out
}

/// Print an error message for an unknown command, listing valid commands.
pub fn print_unknown_command(name: &str) {
    use owo_colors::OwoColorize;

    eprintln!(
        "{}: no help entry for '{}'",
        "error".bright_red().bold(),
        name
    );
    eprintln!();

    let mut commands: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
    for (cmd, _) in COMMANDS {
        commands.insert(cmd, vec![]);
    }
    for (alias, canonical) in ALIASES {
        if let Some(aliases) = commands.get_mut(canonical) {
            aliases.push(alias);
        }
    }

    eprintln!("Available commands:");
    for (cmd, aliases) in &commands {
        if aliases.is_empty() {
            eprintln!("  {cmd}");
        } else {
            eprintln!("  {cmd} ({})", aliases.join(", "));
        }
    }
}

fn resolve_alias(name: &str) -> &str {
    ALIASES
        .iter()
        .find(|(alias, _)| *alias == name)
        .map(|(_, canonical)| *canonical)
        .unwrap_or(name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn help_for_canonical_name() {
        assert!(help_for("compile").is_some());
        assert!(help_for("stage").is_some());
        assert!(help_for("apply").is_some());
        assert!(help_for("deploy").is_some());
    }

    #[test]
    fn help_for_alias() {
        assert_eq!(help_for("build"), help_for("compile"));
        assert_eq!(help_for("show"), help_for("describe"));
        assert_eq!(help_for("history"), help_for("log"));
        assert_eq!(help_for("deployments"), help_for("list"));
        assert_eq!(help_for("branches"), help_for("list"));
        assert_eq!(help_for("gen-data-contracts"), help_for("lock"));
        assert_eq!(help_for("ready"), help_for("wait"));
        assert_eq!(help_for("clusters"), help_for("apply-clusters"));
        assert_eq!(help_for("roles"), help_for("apply-roles"));
        assert_eq!(help_for("promote"), help_for("deploy"));
    }

    #[test]
    fn help_for_unknown() {
        assert!(help_for("nonexistent").is_none());
    }

    #[test]
    fn all_help_contains_all_commands() {
        let all = all_help();
        for (name, _) in COMMANDS {
            assert!(
                all.contains(&format!("--- mz-deploy help {name} ---")),
                "missing delimiter for {name}"
            );
        }
    }
}
