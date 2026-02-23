// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { parse as semverParse, SemVer } from "semver";

export interface DbVersion {
  crateVersion: SemVer;
  sha: string;
  helmChartVersion?: SemVer;
}

const parseDbVersionInner = (versionStr: string): DbVersion | null => {
  const parsed = versionStr.match(
    /*
    Matches version strings like "v0.1.2 (abc123)" or "v0.1.2 (abc123, helm chart: 1.2.3)"
    ^v                     - Start of string, must start with 'v'
    (?<crateVersion>      - Named capture group for the crate version
      [^() ]*)            - Any chars except parens or space, zero or more times
    \(
      (?<sha>[0-9a-fA-F]*) - Named capture group for the git SHA
      (?:                  - Non-capturing group for optional helm chart version
        , helm chart:      - Literal text
        (?<helmChartVersion>[^() ]*) - Named capture group for helm chart version
      )?                   - End optional group
    \)$
    */
    /^v(?<crateVersion>[^() ]*) \((?<sha>[0-9a-fA-F]*)(?:, helm chart: (?<helmChartVersion>[^() ]*))?\)$/,
  );

  if (parsed?.groups) {
    const { crateVersion, sha, helmChartVersion } = parsed.groups;

    const crateVersionParsed = semverParse(crateVersion);

    const helmChartVersionParsed = helmChartVersion
      ? semverParse(helmChartVersion)
      : undefined;

    if (!crateVersionParsed) {
      return null;
    }
    return {
      crateVersion: crateVersionParsed,
      sha,
      helmChartVersion: helmChartVersionParsed ?? undefined,
    };
  }
  return null;
};

/** Parses a string returned by `mz_version()`,
 * which is of the form "v<crate-version> (<sha>)`,
 * where <crate-version> is syntactically a semver object.
 * Throws an error if the version string could not be parsed.
 */
export const parseDbVersion = (versionStr: string): DbVersion => {
  const parsed = parseDbVersionInner(versionStr);
  if (!parsed) {
    throw new Error(`Failed to parse database version: ${versionStr}`);
  }
  return parsed;
};
