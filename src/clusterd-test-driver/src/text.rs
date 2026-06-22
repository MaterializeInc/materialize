// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The text script format: a hand-writable, `datadriven`-style command file.
//!
//! A script is a sequence of stanzas, each a command and its expected output:
//!
//! ```text
//! write-single-ts shard=data ts=0 count=5000
//! ----
//! wrote 5000
//!
//! count id=1001 ts=5
//! ----
//! 10000
//! ```
//!
//! A stanza is a command (a directive line plus an optional indentation-structured
//! body) up to a `----` separator, then the expected output up to a blank line. The
//! `----` block is the assertion; `REWRITE=1` regenerates it (see [`crate::script`]).
//! A `#` at column 0 is a comment; an indented `#0` is a column reference in MIR.
//! Comments and blank lines are preserved across a rewrite.
//!
//! Command bodies are indentation-structured: `define-schema`/`write-rows`/`peek`
//! carry rows or columns, and `define` carries `import`/`build`/`export`
//! sub-commands, with a `build`'s MIR as its own deeper-indented sub-body.

use std::collections::BTreeMap;

use anyhow::{Context, anyhow, bail, ensure};

use crate::script::{BuildSpec, ColumnSpec, Command, ConfigSetting, ExportSpec, ImportSpec};

/// One element of a parsed script file, retained so a `REWRITE` reproduces the
/// file faithfully.
pub enum Item {
    /// A blank line or column-0 `#` comment, kept verbatim.
    Verbatim(String),
    /// A command and its expected output.
    Stanza(Stanza),
}

/// A command stanza: the input block, the expected output, and the parsed command.
pub struct Stanza {
    /// The directive line plus body, verbatim (for rewrite).
    pub input: String,
    /// The expected output block (no trailing newline).
    pub expected: String,
    /// The parsed command.
    pub command: Command,
}

/// Parse a script file into items: each command stanza plus the comments and blank
/// lines around it.
pub fn parse_file(content: &str) -> anyhow::Result<Vec<Item>> {
    let lines: Vec<&str> = content.lines().collect();
    let mut items = Vec::new();
    let mut i = 0;
    while i < lines.len() {
        let line = lines[i];
        // Blank lines and column-0 comments are preserved as-is.
        if line.trim().is_empty() || line.starts_with('#') {
            items.push(Item::Verbatim(line.to_string()));
            i += 1;
            continue;
        }
        // A stanza: slurp the input block up to the `----` separator.
        let start = i;
        while i < lines.len() && lines[i] != "----" {
            i += 1;
        }
        ensure!(
            i < lines.len(),
            "stanza starting at line {} has no `----` separator",
            start + 1
        );
        let input = lines[start..i].join("\n");
        i += 1; // consume `----`
        // The expected output runs to the next blank line (or end of file).
        let exp_start = i;
        while i < lines.len() && !lines[i].trim().is_empty() {
            i += 1;
        }
        let expected = lines[exp_start..i].join("\n");
        let command = parse_command(&input)
            .with_context(|| format!("parsing stanza at line {}", start + 1))?;
        items.push(Item::Stanza(Stanza {
            input,
            expected,
            command,
        }));
    }
    Ok(items)
}

/// Reproduce a script file with each stanza's expected output replaced by its
/// actual output, for `REWRITE`. `actuals` has one entry per [`Item::Stanza`], in
/// order.
pub fn rewrite(items: &[Item], actuals: &[String]) -> String {
    let mut out = String::new();
    let mut next = 0;
    for item in items {
        match item {
            Item::Verbatim(line) => {
                out.push_str(line);
                out.push('\n');
            }
            Item::Stanza(stanza) => {
                let actual = &actuals[next];
                next += 1;
                out.push_str(&stanza.input);
                out.push_str("\n----\n");
                out.push_str(actual);
                if !actual.is_empty() {
                    out.push('\n');
                }
            }
        }
    }
    out
}

/// A non-blank line of a command block, with its leading-space indentation.
struct Line {
    indent: usize,
    text: String,
}

/// Split a command block into non-blank lines, recording each line's indentation.
fn lex(block: &str) -> Vec<Line> {
    block
        .lines()
        .filter_map(|raw| {
            let trimmed = raw.trim_end();
            if trimmed.trim().is_empty() {
                return None;
            }
            let indent = trimmed.len() - trimmed.trim_start().len();
            Some(Line {
                indent,
                text: trimmed.trim_start().to_string(),
            })
        })
        .collect()
}

/// Split body lines into `(header, body)` groups: each header is a line at the
/// minimum indentation, owning the following more-indented lines as its body.
fn group(lines: &[Line]) -> anyhow::Result<Vec<(&Line, &[Line])>> {
    if lines.is_empty() {
        return Ok(vec![]);
    }
    let base = lines.iter().map(|l| l.indent).min().expect("non-empty");
    let mut groups = Vec::new();
    let mut i = 0;
    while i < lines.len() {
        ensure!(
            lines[i].indent == base,
            "inconsistent indentation: `{}`",
            lines[i].text
        );
        let start = i + 1;
        let mut j = start;
        while j < lines.len() && lines[j].indent > base {
            j += 1;
        }
        groups.push((&lines[i], &lines[start..j]));
        i = j;
    }
    Ok(groups)
}

/// Render body lines as text, dedented to the body's minimum indentation so their
/// relative structure (which MIR depends on) is preserved.
fn body_text(lines: &[Line]) -> String {
    if lines.is_empty() {
        return String::new();
    }
    let base = lines.iter().map(|l| l.indent).min().expect("non-empty");
    lines
        .iter()
        .map(|l| format!("{}{}", " ".repeat(l.indent - base), l.text))
        .collect::<Vec<_>>()
        .join("\n")
}

/// Split a header line into whitespace-separated tokens, keeping `"..."` strings
/// and `[...]` lists intact.
fn tokenize(s: &str) -> anyhow::Result<Vec<String>> {
    let mut tokens = Vec::new();
    let mut cur = String::new();
    let mut depth = 0i32;
    let mut in_quote = false;
    for c in s.chars() {
        match c {
            '"' => {
                in_quote = !in_quote;
                cur.push(c);
            }
            '[' if !in_quote => {
                depth += 1;
                cur.push(c);
            }
            ']' if !in_quote => {
                depth -= 1;
                cur.push(c);
            }
            c if c.is_whitespace() && !in_quote && depth == 0 => {
                if !cur.is_empty() {
                    tokens.push(std::mem::take(&mut cur));
                }
            }
            c => cur.push(c),
        }
    }
    ensure!(!in_quote, "unterminated string in `{s}`");
    ensure!(depth == 0, "unterminated list in `{s}`");
    if !cur.is_empty() {
        tokens.push(cur);
    }
    Ok(tokens)
}

/// Parse a header line into its verb, `key=value` arguments, and bare flags.
fn parse_header(header: &str) -> anyhow::Result<(String, BTreeMap<String, String>, Vec<String>)> {
    let mut tokens = tokenize(header)?.into_iter();
    let verb = tokens.next().context("empty command")?;
    let mut args = BTreeMap::new();
    let mut flags = Vec::new();
    for token in tokens {
        match token.split_once('=') {
            Some((key, value)) => {
                args.insert(key.to_string(), value.to_string());
            }
            None => flags.push(token),
        }
    }
    Ok((verb, args, flags))
}

fn req<'a>(args: &'a BTreeMap<String, String>, key: &str) -> anyhow::Result<&'a str> {
    args.get(key)
        .map(String::as_str)
        .ok_or_else(|| anyhow!("missing argument `{key}`"))
}

fn req_u64(args: &BTreeMap<String, String>, key: &str) -> anyhow::Result<u64> {
    req(args, key)?
        .parse()
        .with_context(|| format!("argument `{key}` is not an integer"))
}

fn opt_u64(args: &BTreeMap<String, String>, key: &str) -> anyhow::Result<Option<u64>> {
    args.get(key)
        .map(|v| {
            v.parse()
                .with_context(|| format!("argument `{key}` is not an integer"))
        })
        .transpose()
}

fn opt_usize(args: &BTreeMap<String, String>, key: &str) -> anyhow::Result<Option<usize>> {
    args.get(key)
        .map(|v| {
            v.parse()
                .with_context(|| format!("argument `{key}` is not an integer"))
        })
        .transpose()
}

fn opt_string(args: &BTreeMap<String, String>, key: &str) -> Option<String> {
    args.get(key).cloned()
}

/// Parse a `[a,b,c]` list of `usize`s (`[]` is empty).
fn parse_usize_list(s: &str) -> anyhow::Result<Vec<usize>> {
    let inner = s
        .strip_prefix('[')
        .and_then(|s| s.strip_suffix(']'))
        .ok_or_else(|| anyhow!("expected a list like `[0,1]`, got `{s}`"))?;
    if inner.trim().is_empty() {
        return Ok(vec![]);
    }
    inner
        .split(',')
        .map(|part| {
            part.trim()
                .parse()
                .with_context(|| format!("bad list element `{part}`"))
        })
        .collect()
}

/// Parse an `export` sub-command into an [`ExportSpec`]. The `kind=` argument
/// selects the variant (defaulting to `index`); each kind takes its own arguments.
fn parse_export(args: &BTreeMap<String, String>) -> anyhow::Result<ExportSpec> {
    let on_id = req_u64(args, "on")?;
    Ok(
        match args.get("kind").map(String::as_str).unwrap_or("index") {
            "index" => ExportSpec::Index {
                index_id: req_u64(args, "index")?,
                on_id,
                key: parse_usize_list(req(args, "key")?)?,
            },
            "materialized-view" => ExportSpec::MaterializedView {
                sink_id: req_u64(args, "sink")?,
                on_id,
                shard: req(args, "shard")?.to_string(),
                schema: opt_string(args, "schema"),
            },
            "subscribe" => ExportSpec::Subscribe {
                sink_id: req_u64(args, "sink")?,
                on_id,
                schema: opt_string(args, "schema"),
                up_to: opt_u64(args, "up-to")?,
            },
            "copy-to" => bail!("copy-to export is not implemented"),
            other => bail!("unknown export kind `{other}`"),
        },
    )
}

/// Parse body lines as rows of raw space-separated value tokens (quotes intact).
/// The tokens are typed against the schema server-side; see `cell_from_token`.
fn rows_from_body(body: &[Line]) -> anyhow::Result<Vec<Vec<String>>> {
    body.iter().map(|l| tokenize(&l.text)).collect()
}

/// Parse body lines as dyncfg settings: `name type value`.
fn settings_from_body(body: &[Line]) -> anyhow::Result<Vec<ConfigSetting>> {
    body.iter()
        .map(|l| {
            let tokens = tokenize(&l.text)?;
            ensure!(
                tokens.len() == 3,
                "config setting needs `name type value`, got `{}`",
                l.text
            );
            Ok(ConfigSetting {
                name: tokens[0].clone(),
                ty: tokens[1].clone(),
                value: tokens[2].clone(),
            })
        })
        .collect()
}

/// Parse body lines as column declarations: `name type [nullable]`.
fn columns_from_body(body: &[Line]) -> anyhow::Result<Vec<ColumnSpec>> {
    body.iter()
        .map(|l| {
            let tokens = tokenize(&l.text)?;
            ensure!(
                tokens.len() >= 2,
                "column needs `name type [nullable]`, got `{}`",
                l.text
            );
            Ok(ColumnSpec {
                name: tokens[0].clone(),
                ty: tokens[1].clone(),
                nullable: tokens.get(2).is_some_and(|t| t == "nullable"),
            })
        })
        .collect()
}

/// Parse a `create-dataflow` body of `import`/`build`/`export` sub-commands. The
/// directive's bare flags carry the dataflow-level options (`optimize`).
fn parse_create_dataflow(
    args: &BTreeMap<String, String>,
    flags: &[String],
    body: &[Line],
) -> anyhow::Result<Command> {
    let name = opt_string(args, "name");
    let as_of = req_u64(args, "as-of")?;
    let optimize = flags.iter().any(|f| f == "optimize");
    let mut imports = Vec::new();
    let mut builds = Vec::new();
    let mut exports = Vec::new();
    for (header, sub_body) in group(body)? {
        let (verb, args, _flags) = parse_header(&header.text)?;
        match verb.as_str() {
            "import" => {
                if let Some(index_id) = args.get("index") {
                    imports.push(ImportSpec::Index {
                        index_id: index_id
                            .parse()
                            .with_context(|| format!("bad index id `{index_id}`"))?,
                    });
                } else {
                    imports.push(ImportSpec::Source {
                        id: req_u64(&args, "source")?,
                        shard: req(&args, "shard")?.to_string(),
                        schema: opt_string(&args, "schema"),
                        upper: req_u64(&args, "upper")?,
                    });
                }
            }
            "build" => {
                ensure!(!sub_body.is_empty(), "`build` needs a MIR body");
                builds.push(BuildSpec {
                    id: req_u64(&args, "id")?,
                    expr: body_text(sub_body),
                });
            }
            "export" => exports.push(parse_export(&args)?),
            other => bail!("unknown `create-dataflow` sub-command `{other}`"),
        }
    }
    Ok(Command::CreateDataflow {
        name,
        imports,
        builds,
        exports,
        as_of,
        optimize,
    })
}

/// Parse one command block (directive line plus indentation-structured body).
fn parse_command(input: &str) -> anyhow::Result<Command> {
    let lines = lex(input);
    let (header, body) = lines.split_first().context("empty command")?;
    ensure!(header.indent == 0, "directive must not be indented");
    let (verb, args, flags) = parse_header(&header.text)?;
    let command = match verb.as_str() {
        "define-schema" => Command::DefineSchema {
            name: req(&args, "name")?.to_string(),
            columns: columns_from_body(body)?,
        },
        "write-single-ts" => Command::WriteSingleTs {
            shard: req(&args, "shard")?.to_string(),
            schema: opt_string(&args, "schema"),
            ts: req_u64(&args, "ts")?,
            count: req_u64(&args, "count")?,
            start: opt_u64(&args, "start")?.unwrap_or(0),
            row_bytes: opt_usize(&args, "row-bytes")?,
        },
        "write-spread" => Command::WriteSpread {
            shard: req(&args, "shard")?.to_string(),
            schema: opt_string(&args, "schema"),
            count: req_u64(&args, "count")?,
            n_ts: req_u64(&args, "n-ts")?,
            start: opt_u64(&args, "start")?.unwrap_or(0),
            row_bytes: opt_usize(&args, "row-bytes")?,
        },
        "write-rows" => Command::WriteRows {
            shard: req(&args, "shard")?.to_string(),
            schema: opt_string(&args, "schema"),
            ts: req_u64(&args, "ts")?,
            rows: rows_from_body(body)?,
        },
        "define-index" => Command::DefineIndex {
            source_id: req_u64(&args, "source")?,
            index_id: req_u64(&args, "index")?,
            shard: req(&args, "shard")?.to_string(),
            schema: opt_string(&args, "schema"),
            key: parse_usize_list(req(&args, "key")?)?,
            as_of: req_u64(&args, "as-of")?,
            upper: req_u64(&args, "upper")?,
        },
        "schedule" => Command::Schedule {
            id: req_u64(&args, "id")?,
        },
        "allow-compaction" => Command::AllowCompaction {
            id: req_u64(&args, "id")?,
            frontier: req_u64(&args, "frontier")?,
        },
        "allow-writes" => Command::AllowWrites {
            id: req_u64(&args, "id")?,
        },
        "await-frontier" => Command::AwaitFrontier {
            id: req_u64(&args, "id")?,
            ts: req_u64(&args, "ts")?,
            timeout_secs: opt_u64(&args, "timeout-secs")?,
            allow_timeout: flags.iter().any(|f| f == "allow-timeout"),
        },
        "count" => Command::Count {
            id: req_u64(&args, "id")?,
            ts: req_u64(&args, "ts")?,
        },
        "peek" => Command::Peek {
            id: req_u64(&args, "id")?,
            schema: opt_string(&args, "schema"),
            ts: req_u64(&args, "ts")?,
        },
        "await-subscribe" => Command::AwaitSubscribe {
            id: req_u64(&args, "id")?,
            up_to: req_u64(&args, "up-to")?,
            timeout_secs: opt_u64(&args, "timeout-secs")?,
        },
        "create-dataflow" => parse_create_dataflow(&args, &flags, body)?,
        "create-instance" => Command::CreateInstance {
            expiration_offset: opt_string(&args, "expiration-offset"),
            arrangement_dictionary_compression: args
                .get("arrangement-dictionary-compression")
                .map(|v| v.parse())
                .transpose()
                .context("argument `arrangement-dictionary-compression` is not a bool")?
                .unwrap_or(false),
        },
        "update-configuration" => Command::UpdateConfiguration {
            updates: settings_from_body(body)?,
        },
        "reconnect" => Command::Reconnect,
        "initialization-complete" => Command::InitializationComplete,
        other => bail!("unknown command `{other}`"),
    };
    Ok(command)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A simple command parses its args; absent optionals default.
    #[mz_ore::test]
    fn parses_simple_command() {
        let cmd = parse_command("write-single-ts shard=data ts=0 count=5000").unwrap();
        assert_eq!(
            cmd,
            Command::WriteSingleTs {
                shard: "data".to_string(),
                schema: None,
                ts: 0,
                count: 5000,
                start: 0,
                row_bytes: None,
            }
        );
    }

    /// A flag and a list argument parse.
    #[mz_ore::test]
    fn parses_flag_and_list() {
        let cmd =
            parse_command("await-frontier id=1001 ts=1 timeout-secs=3 allow-timeout").unwrap();
        assert_eq!(
            cmd,
            Command::AwaitFrontier {
                id: 1001,
                ts: 1,
                timeout_secs: Some(3),
                allow_timeout: true,
            }
        );
        let cmd =
            parse_command("define-index source=1000 index=1001 shard=d key=[0] as-of=0 upper=1")
                .unwrap();
        assert_eq!(
            cmd,
            Command::DefineIndex {
                source_id: 1000,
                index_id: 1001,
                shard: "d".to_string(),
                schema: None,
                key: vec![0],
                as_of: 0,
                upper: 1,
            }
        );
    }

    /// `define-schema` and `write-rows` parse their indented bodies, typing values.
    #[mz_ore::test]
    fn parses_bodies() {
        let cmd = parse_command(
            "define-schema name=events\n  key bigint\n  flag boolean\n  label text nullable",
        )
        .unwrap();
        assert_eq!(
            cmd,
            Command::DefineSchema {
                name: "events".to_string(),
                columns: vec![
                    ColumnSpec {
                        name: "key".to_string(),
                        ty: "bigint".to_string(),
                        nullable: false
                    },
                    ColumnSpec {
                        name: "flag".to_string(),
                        ty: "boolean".to_string(),
                        nullable: false
                    },
                    ColumnSpec {
                        name: "label".to_string(),
                        ty: "text".to_string(),
                        nullable: true
                    },
                ],
            }
        );

        let cmd = parse_command(
            "write-rows shard=ev schema=events ts=1\n  1000 true alpha\n  1001 false null",
        )
        .unwrap();
        assert_eq!(
            cmd,
            Command::WriteRows {
                shard: "ev".to_string(),
                schema: Some("events".to_string()),
                ts: 1,
                rows: vec![
                    vec!["1000".to_string(), "true".to_string(), "alpha".to_string()],
                    vec!["1001".to_string(), "false".to_string(), "null".to_string()],
                ],
            }
        );
    }

    /// `create-dataflow` parses its sub-commands, keeping a `build`'s MIR (its
    /// deeper body) with relative indentation preserved; the export kind defaults
    /// to `index`.
    #[mz_ore::test]
    fn parses_create_dataflow_with_mir() {
        let input = "create-dataflow name=count as-of=0\n  import index=1001\n  build id=2000\n    Reduce aggregates=[count(*)]\n      Get u1000\n  export index=2001 on=2000 key=[0]";
        let cmd = parse_command(input).unwrap();
        assert_eq!(
            cmd,
            Command::CreateDataflow {
                name: Some("count".to_string()),
                imports: vec![ImportSpec::Index { index_id: 1001 }],
                builds: vec![BuildSpec {
                    id: 2000,
                    expr: "Reduce aggregates=[count(*)]\n  Get u1000".to_string(),
                }],
                exports: vec![ExportSpec::Index {
                    index_id: 2001,
                    on_id: 2000,
                    key: vec![0]
                }],
                as_of: 0,
                optimize: false,
            }
        );

        // The `optimize` flag on the directive line is picked up.
        let optimized = parse_command(
            "create-dataflow name=j as-of=0 optimize\n  import source=1000 shard=l upper=1\n  build id=2000\n    Get u1000\n  export index=2001 on=2000 key=[0]",
        )
        .unwrap();
        assert!(matches!(
            optimized,
            Command::CreateDataflow { optimize: true, .. }
        ));
    }

    /// `create-dataflow` parses the sink export kinds: a materialized-view sink with
    /// a target shard, and a subscribe sink. The `copy-to` kind is rejected.
    #[mz_ore::test]
    fn parses_sink_export_kinds() {
        let mv = "create-dataflow name=mv as-of=0\n  import source=1000 shard=r upper=1\n  build id=2000\n    Get u1000\n  export kind=materialized-view sink=2001 on=2000 shard=out schema=kv";
        let Command::CreateDataflow { exports, .. } = parse_command(mv).unwrap() else {
            panic!("expected create-dataflow");
        };
        assert_eq!(
            exports,
            vec![ExportSpec::MaterializedView {
                sink_id: 2001,
                on_id: 2000,
                shard: "out".to_string(),
                schema: Some("kv".to_string()),
            }]
        );

        let sub = "create-dataflow name=sub as-of=0\n  import source=1000 shard=s upper=2\n  build id=2000\n    Get u1000\n  export kind=subscribe sink=2001 on=2000 up-to=2";
        let Command::CreateDataflow { exports, .. } = parse_command(sub).unwrap() else {
            panic!("expected create-dataflow");
        };
        assert_eq!(
            exports,
            vec![ExportSpec::Subscribe {
                sink_id: 2001,
                on_id: 2000,
                schema: None,
                up_to: Some(2),
            }]
        );

        // copy-to is named but not implemented.
        let copy = "create-dataflow name=c as-of=0\n  import source=1000 shard=s upper=1\n  build id=2000\n    Get u1000\n  export kind=copy-to sink=2001 on=2000";
        assert!(parse_command(copy).is_err());
    }

    /// `create-instance` parses its optional knobs (defaulting), and
    /// `update-configuration` parses a `name type value` table (empty when bodyless).
    #[mz_ore::test]
    fn parses_handshake_config() {
        assert_eq!(
            parse_command("create-instance").unwrap(),
            Command::CreateInstance {
                expiration_offset: None,
                arrangement_dictionary_compression: false,
            }
        );
        assert_eq!(
            parse_command(
                "create-instance expiration-offset=30s arrangement-dictionary-compression=true"
            )
            .unwrap(),
            Command::CreateInstance {
                expiration_offset: Some("30s".to_string()),
                arrangement_dictionary_compression: true,
            }
        );

        assert_eq!(
            parse_command("update-configuration").unwrap(),
            Command::UpdateConfiguration { updates: vec![] }
        );
        assert_eq!(
            parse_command("update-configuration\n  enable_my_flag bool true\n  my_dur duration 1s")
                .unwrap(),
            Command::UpdateConfiguration {
                updates: vec![
                    ConfigSetting {
                        name: "enable_my_flag".to_string(),
                        ty: "bool".to_string(),
                        value: "true".to_string(),
                    },
                    ConfigSetting {
                        name: "my_dur".to_string(),
                        ty: "duration".to_string(),
                        value: "1s".to_string(),
                    },
                ],
            }
        );
    }

    /// `await-subscribe` parses its arguments.
    #[mz_ore::test]
    fn parses_await_subscribe() {
        let cmd = parse_command("await-subscribe id=2001 up-to=2 timeout-secs=5").unwrap();
        assert_eq!(
            cmd,
            Command::AwaitSubscribe {
                id: 2001,
                up_to: 2,
                timeout_secs: Some(5),
            }
        );
    }

    /// A file splits into stanzas, preserving comments and blanks, and round-trips
    /// through a rewrite when the actual output equals the expected.
    #[mz_ore::test]
    fn parses_and_rewrites_file() {
        let content =
            "# a comment\nschedule id=1001\n----\nok\n\ncount id=1001 ts=5\n----\n10000\n";
        let items = parse_file(content).unwrap();
        let stanzas: Vec<_> = items
            .iter()
            .filter_map(|i| match i {
                Item::Stanza(s) => Some(s),
                Item::Verbatim(_) => None,
            })
            .collect();
        assert_eq!(stanzas.len(), 2);
        assert_eq!(stanzas[0].command, Command::Schedule { id: 1001 });
        assert_eq!(stanzas[0].expected, "ok");
        assert_eq!(stanzas[1].expected, "10000");

        // Rewriting with the same outputs reproduces the file.
        let actuals = vec!["ok".to_string(), "10000".to_string()];
        assert_eq!(rewrite(&items, &actuals), content);
    }
}
