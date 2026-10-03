// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Comparison of this build's version against the server's.
//!
//! Materialize announces its build as an `mz_version` ParameterStatus during
//! the pgwire handshake, so the comparison costs no round trip. A mismatch is
//! reported as a notice and never changes the exit code: the CLI and the server
//! are versioned together but deploy separately, so drift is expected and only
//! sometimes matters.

use std::cmp::Ordering;
use std::sync::Once;

use semver::Version;

use crate::BUILD_INFO;
use crate::cli::progress;

/// The ParameterStatus the server announces its build version under.
pub(super) const MZ_VERSION_PARAMETER: &str = "mz_version";

/// Extracts the semver from an `mz_version` ParameterStatus value.
///
/// The value is [`mz_build_info::BuildInfo::human_version`] output: a leading
/// `v`, the semver, then parenthesized build detail, for example
/// `v26.44.0 (abc123def)` or `v26.44.0 (abc123def, helm chart: 25.1.0)`.
/// Returns `None` for anything that is not a Materialize version.
fn parse_server_version(raw: &str) -> Option<Version> {
    let token = raw.split_whitespace().next()?;
    Version::parse(token.strip_prefix('v').unwrap_or(token)).ok()
}

/// Builds the skew notice, or `None` when the two versions are compatible.
///
/// Compares `(major, minor)` only. Patch releases do not change the surface the
/// CLI drives, and dropping the pre-release keeps a dev build silent against the
/// release it was cut from.
///
/// Split out from [`report`] so it is testable without a connection or the
/// process-global `Once`.
fn notice(client: &Version, raw: Option<&str>) -> Option<String> {
    let server = parse_server_version(raw?)?;
    let (direction, owner) = match (client.major, client.minor).cmp(&(server.major, server.minor)) {
        Ordering::Less => ("older", "server"),
        Ordering::Greater => ("newer", "mz-deploy"),
        Ordering::Equal => return None,
    };
    Some(format!(
        "mz-deploy v{client} is {direction} than the server (v{server}). \
         Newer {owner} features may be unsupported, and some features may not \
         behave as expected."
    ))
}

/// Emits at most one version skew notice per process.
///
/// Stays silent when the peer announced no parseable `mz_version`. Reaching
/// here means the handshake already succeeded, and a proxy or a non-Materialize
/// peer that drops the parameter is not worth a warning on every command.
pub(super) fn report(raw: Option<&str>) {
    static REPORTED: Once = Once::new();

    // The `Once` is claimed only once a notice is warranted, so a matching first
    // connection cannot suppress the notice for a later mismatched one.
    if let Some(message) = notice(&BUILD_INFO.semver_version(), raw) {
        REPORTED.call_once(|| progress::warn(&message));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn check(client: &str, raw: Option<&str>) -> Option<String> {
        notice(&Version::parse(client).unwrap(), raw)
    }

    #[mz_ore::test]
    fn patch_difference_is_silent() {
        assert_eq!(check("26.44.3", Some("v26.44.0 (abc123def)")), None);
        assert_eq!(check("26.44.0", Some("v26.44.9 (abc123def)")), None);
    }

    #[mz_ore::test]
    fn prerelease_matches_its_release() {
        assert_eq!(check("26.44.0-dev.0", Some("v26.44.0 (abc123def)")), None);
        assert_eq!(check("26.44.0", Some("v26.44.0-dev (937dfde5e)")), None);
        assert_eq!(check("26.44.0-rc.1", Some("v26.44.0 (abc123def)")), None);
    }

    #[mz_ore::test]
    fn minor_difference_reports_direction() {
        let older = check("26.40.0", Some("v26.44.0 (abc123def)")).unwrap();
        assert!(older.contains("is older than the server"), "{older}");
        assert!(older.contains("Newer server features"), "{older}");

        let newer = check("26.44.0", Some("v26.40.0 (abc123def)")).unwrap();
        assert!(newer.contains("is newer than the server"), "{newer}");
        assert!(newer.contains("Newer mz-deploy features"), "{newer}");
    }

    #[mz_ore::test]
    fn major_difference_reports_direction() {
        let notice = check("25.1.0", Some("v26.44.0 (abc123def)")).unwrap();
        assert!(notice.contains("is older than the server"), "{notice}");
    }

    #[mz_ore::test]
    fn helm_chart_suffix_does_not_shadow_the_build_version() {
        let notice = check("26.44.0", Some("v26.40.0 (abc123def, helm chart: 25.1.0)")).unwrap();
        assert!(notice.contains("is newer than the server"), "{notice}");
        assert!(notice.contains("v26.40.0"), "{notice}");
        assert!(!notice.contains("25.1.0"), "{notice}");
    }

    #[mz_ore::test]
    fn bare_semver_parses() {
        let notice = check("26.44.0", Some("26.40.0")).unwrap();
        assert!(notice.contains("is newer than the server"), "{notice}");
    }

    #[mz_ore::test]
    fn unannounced_or_unparseable_version_is_silent() {
        assert_eq!(check("26.44.0", None), None);
        assert_eq!(check("26.44.0", Some("")), None);
        assert_eq!(check("26.44.0", Some("PostgreSQL 16.2")), None);
        assert_eq!(check("26.44.0", Some("v (abc123def)")), None);
    }
}
