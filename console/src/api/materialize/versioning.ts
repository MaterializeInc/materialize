// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { SelectQueryBuilder } from "kysely";
import {
  gte as semverGte,
  parse as semverParse,
  rcompare as semverRcompare,
  valid as semverValid,
} from "semver";
import { DB } from "types/materialize";

/**
 * Parses two semantic version strings and returns true if the first version
 * is greater than or equal to the second.
 *
 * @param version The version string to check.
 * @param targetVersion The version string to compare against.
 * @returns boolean if version is greater than or equal to targetVersion
 */
export function semverParseGte(
  version: string,
  targetVersion: string,
): boolean {
  const parsedVersion = semverParse(version);
  const parsedTargetVersion = semverParse(targetVersion);

  if (!parsedVersion || !parsedTargetVersion) {
    return false;
  }

  return semverGte(parsedVersion, parsedTargetVersion);
}

// A generic type for a function that takes a single options object `TArgs` and returns a Kysely SelectQueryBuilder
export type QueryBuilderFactory<TArgs extends object, TReturn> = (
  args: TArgs,
) => SelectQueryBuilder<DB, any, TReturn>;

// A map where each entry is a QueryBuilderFactory. All functions in this map
// are enforced to accept the same options object `TArgs` and return the same shape
export type VersionMap<TArgs extends object, TReturn> = {
  [version: string]: QueryBuilderFactory<TArgs, TReturn>;
};

/**
 * Selects a query builder factory from a version map based on the environment version.
 * It picks the factory for the highest version that is less than or equal to the environment version.
 * If no environment version is provided, it uses the factory for the latest version available in the map.
 * For handling versions older than any specific version in the map fallback would be to a version "0.0.0" to indicate the oldest version.
 *
 * @param environmentVersion The environment version.
 * @param versionMap A map of version strings to query builder factories.
 * @returns The selected query builder factory.
 */
export function getQueryBuilderForVersion<TArgs extends object, TReturn>(
  environmentVersion: string | undefined,
  versionMap: VersionMap<TArgs, TReturn>,
): QueryBuilderFactory<TArgs, TReturn> {
  const sortedVersions = Object.keys(versionMap)
    .filter((v) => semverValid(v))
    .sort(semverRcompare); // Sorts descending

  const getLatest = (): QueryBuilderFactory<TArgs, TReturn> => {
    if (sortedVersions.length > 0) {
      return versionMap[sortedVersions[0]];
    }
    return versionMap["0.0.0"];
  };

  const getFallback = (): QueryBuilderFactory<TArgs, TReturn> =>
    versionMap["0.0.0"];

  if (!environmentVersion) {
    return getLatest();
  }

  const parsedVersion = semverParse(environmentVersion);
  if (!parsedVersion) {
    return getLatest();
  }

  // Find the highest version in the map that is less than or equal to the environment version
  for (const v of sortedVersions) {
    if (semverGte(parsedVersion, v)) {
      return versionMap[v];
    }
  }

  // The environment version is older than any in the map, so use the fallback.
  return getFallback();
}
