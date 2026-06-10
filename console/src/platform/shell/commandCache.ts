// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import storageAvailable from "~/utils/storageAvailable";

export const MAX_CACHED_COMMANDS = 100;

export type CommandCache = {
  version: number;
  commands: string[];
};

function buildCacheKey({
  organizationId,
  regionId,
}: {
  organizationId?: string;
  regionId: string;
}): string {
  return `mz-shell-command-cache|${organizationId}|${regionId}`;
}

export function getCache({
  organizationId,
  regionId,
}: {
  organizationId?: string;
  regionId: string;
}): CommandCache {
  const cacheString = storageAvailable("localStorage")
    ? window.localStorage.getItem(buildCacheKey({ organizationId, regionId }))
    : null;
  if (cacheString === null) {
    // The version number may only be incremented if a migration exists.
    return { version: 1, commands: [] };
  }
  const cache: CommandCache = JSON.parse(cacheString);
  // TODO: when we have a version that is not v1, migrations should be
  // performed here.
  return cache;
}

export function appendToCache({
  organizationId,
  regionId,
  command,
}: {
  organizationId?: string;
  regionId: string;
  command: string;
}) {
  if (storageAvailable("localStorage")) {
    const cache = getCache({ organizationId, regionId });
    cache.commands.push(command);
    if (cache.commands.length > MAX_CACHED_COMMANDS) {
      cache.commands.splice(0, cache.commands.length - MAX_CACHED_COMMANDS);
    }
    window.localStorage.setItem(
      buildCacheKey({ organizationId, regionId }),
      JSON.stringify(cache),
    );
  }
}
