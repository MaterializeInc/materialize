// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Mcp command - proxy stdio JSON-RPC to Materialize's developer MCP server.
//!
//! Materialize exposes a developer MCP server at `POST /api/mcp/developer`
//! over HTTP with HTTP Basic auth. Most MCP clients (Claude Desktop, Claude
//! Code, Cursor) launch their MCP servers as subprocesses speaking JSON-RPC
//! over stdio. This command bridges the two: read newline-delimited JSON-RPC
//! from stdin, POST each message to the developer endpoint with the active
//! profile's credentials, and write each response back to stdout.

use std::path::Path;

use reqwest::Client;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use url::Url;

use crate::cli::CliError;
use crate::client::is_loopback_host;
use crate::config::{Profile, ProfilesConfig, read_mzprofile};

/// Path of the developer MCP endpoint on the Materialize HTTP listener.
const MCP_DEVELOPER_PATH: &str = "/api/mcp/developer";

/// Default HTTP port for a local Materialize.
const LOCAL_HTTP_PORT: u16 = 6876;

/// Proxy stdio JSON-RPC to the developer MCP HTTP endpoint.
///
/// Unlike most subcommands, `mcp` does not require a `project.toml` — MCP
/// clients launch this binary from their own working directory, not from a
/// deploy project root. The active profile is resolved from `--profile` /
/// `MZ_DEPLOY_PROFILE` (passed in via `cli_profile`), falling back to the
/// `.mzprofile` file in `directory` if one happens to exist.
pub async fn run(
    directory: &Path,
    cli_profile: Option<&str>,
    profiles_dir: Option<&Path>,
) -> Result<(), CliError> {
    let profile = resolve_profile(directory, cli_profile, profiles_dir)?;
    let url = developer_url(profile.require_http_host()?)?;
    let client = Client::new();

    let stdin = BufReader::new(tokio::io::stdin());
    let mut lines = stdin.lines();
    let mut stdout = tokio::io::stdout();

    while let Some(line) = lines
        .next_line()
        .await
        .map_err(|e| CliError::Message(format!("failed to read from stdin: {e}")))?
    {
        if line.trim().is_empty() {
            continue;
        }

        // Only attach Basic auth when the profile carries a non-empty
        // password. The MCP endpoint doesn't mandate auth — let the server
        // return 401 if it requires credentials we don't have, rather than
        // sending a header with an empty password.
        let mut req = client
            .post(&url)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(line.clone());
        if let Some(password) = profile.password.as_deref().filter(|p| !p.is_empty()) {
            req = req.basic_auth(&profile.username, Some(password));
        }
        let response = req.send().await;

        let mut response_body = match response {
            Ok(resp) => {
                let status = resp.status();
                match resp.text().await {
                    Ok(body) if !body.is_empty() => body,
                    Ok(_) => synthesize_error(
                        &line,
                        status.as_u16().into(),
                        format!("HTTP {status} with empty body"),
                    ),
                    Err(e) => synthesize_error(
                        &line,
                        -32603,
                        format!("failed to read HTTP response body: {e}"),
                    ),
                }
            }
            Err(e) => synthesize_error(&line, -32603, format!("HTTP request to {url} failed: {e}")),
        };

        // MCP stdio framing: exactly one trailing newline per message.
        if !response_body.ends_with('\n') {
            response_body.push('\n');
        }
        stdout
            .write_all(response_body.as_bytes())
            .await
            .map_err(|e| CliError::Message(format!("failed to write to stdout: {e}")))?;
        stdout
            .flush()
            .await
            .map_err(|e| CliError::Message(format!("failed to flush stdout: {e}")))?;
    }

    Ok(())
}

/// Resolve the active profile without going through `Settings::load`, so the
/// MCP proxy works outside an mz-deploy project directory.
fn resolve_profile(
    directory: &Path,
    cli_profile: Option<&str>,
    profiles_dir: Option<&Path>,
) -> Result<Profile, CliError> {
    let name = match cli_profile {
        Some(p) => p.to_string(),
        None => read_mzprofile(directory)?.ok_or_else(|| {
            CliError::Message(
                "no profile selected: pass --profile <name>, set MZ_DEPLOY_PROFILE, \
                 or run from a directory with a .mzprofile file"
                    .to_string(),
            )
        })?,
    };
    Ok(ProfilesConfig::resolve_profile(profiles_dir, &name)?)
}

/// Build the developer MCP URL from the profile's `http_host`.
///
/// `http_host` is flexible:
/// - Bare hostname (`console.foo.cloud`) → infer `https://`.
/// - Loopback (`localhost`, `127.0.0.0/8`, `::1`) → infer `http://` and, if
///   no port was given, default to `LOCAL_HTTP_PORT`.
/// - `host:port` or `[ipv6]:port` → keep the port; infer scheme as above.
/// - `http://...` / `https://...` → use the URL verbatim. The user picked
///   the scheme and any port; no defaults are applied.
///
/// IPv6 hosts are bracketed automatically by `url::Url` regardless of how
/// the user wrote them.
fn developer_url(http_host: &str) -> Result<String, CliError> {
    // `Url::parse` returns `RelativeUrlWithoutBase` when no scheme is
    // present, so a successful parse means the user wrote one. We filter to
    // the schemes we accept; `Url::parse("localhost:8000")` succeeds with
    // `scheme="localhost"`, which would otherwise be a false positive.
    let (mut url, user_supplied_scheme) = match Url::parse(http_host) {
        Ok(u) if matches!(u.scheme(), "http" | "https") => (u, true),
        _ => {
            // `Url::parse` requires IPv6 hosts to be bracketed. Bracket bare
            // IPv6 input so `::1` parses the same as `[::1]`.
            let host_part = match http_host.parse::<std::net::IpAddr>() {
                Ok(std::net::IpAddr::V6(_)) => format!("[{http_host}]"),
                _ => http_host.to_owned(),
            };
            let parsed = Url::parse(&format!("http://{host_part}"))
                .map_err(|e| CliError::Message(format!("invalid http_host {http_host:?}: {e}")))?;
            (parsed, false)
        }
    };

    // For http/https URLs the host is always present and the scheme is
    // changeable, so the `Url` setters that return `Result` cannot fail.
    if !user_supplied_scheme {
        let host = url.host_str().expect("http URL has a host").to_owned();
        if is_loopback_host(&host) {
            if url.port().is_none() {
                url.set_port(Some(LOCAL_HTTP_PORT))
                    .expect("http URL accepts an explicit port");
            }
        } else {
            url.set_scheme("https")
                .expect("http→https swap is always allowed");
        }
    }

    url.set_path(MCP_DEVELOPER_PATH);
    Ok(url.to_string())
}

/// Build a JSON-RPC error response so the MCP client gets a structured error
/// instead of a hang when the HTTP layer fails before the server can respond.
fn synthesize_error(request_line: &str, code: i64, message: String) -> String {
    let id = serde_json::from_str::<serde_json::Value>(request_line)
        .ok()
        .and_then(|v| v.get("id").cloned())
        .unwrap_or(serde_json::Value::Null);

    let response = serde_json::json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": { "code": code, "message": message },
    });
    response.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[track_caller]
    fn build(input: &str) -> String {
        developer_url(input).expect("developer_url should accept valid input")
    }

    #[mz_ore::test]
    fn loopback_hostname_gets_default_http_port() {
        assert_eq!(
            build("localhost"),
            "http://localhost:6876/api/mcp/developer"
        );
        assert_eq!(
            build("127.0.0.1"),
            "http://127.0.0.1:6876/api/mcp/developer"
        );
    }

    #[mz_ore::test]
    fn loopback_with_explicit_port_keeps_it() {
        assert_eq!(
            build("localhost:8000"),
            "http://localhost:8000/api/mcp/developer"
        );
    }

    #[mz_ore::test]
    fn ipv6_loopback_is_bracketed_correctly() {
        // Both bare and bracketed forms must produce a parseable URL.
        assert_eq!(build("::1"), "http://[::1]:6876/api/mcp/developer");
        assert_eq!(build("[::1]"), "http://[::1]:6876/api/mcp/developer");
        assert_eq!(build("[::1]:9000"), "http://[::1]:9000/api/mcp/developer");
    }

    #[mz_ore::test]
    fn non_loopback_defaults_to_https_no_port() {
        assert_eq!(
            build("console.foo.cloud"),
            "https://console.foo.cloud/api/mcp/developer"
        );
        assert_eq!(
            build("console.foo.cloud:8443"),
            "https://console.foo.cloud:8443/api/mcp/developer"
        );
    }

    #[mz_ore::test]
    fn explicit_scheme_is_preserved() {
        assert_eq!(
            build("http://localhost:8000"),
            "http://localhost:8000/api/mcp/developer"
        );
        assert_eq!(
            build("https://console.foo.cloud"),
            "https://console.foo.cloud/api/mcp/developer"
        );
    }

    #[mz_ore::test]
    fn explicit_scheme_is_case_insensitive() {
        // url::Url normalizes the scheme to lowercase.
        assert_eq!(
            build("HTTPS://console.foo.cloud"),
            "https://console.foo.cloud/api/mcp/developer"
        );
    }

    #[mz_ore::test]
    fn trailing_slashes_collapse() {
        assert_eq!(
            build("localhost/"),
            "http://localhost:6876/api/mcp/developer"
        );
        assert_eq!(
            build("https://console.foo.cloud/"),
            "https://console.foo.cloud/api/mcp/developer"
        );
    }
}
