// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { appendToCache, getCache, MAX_CACHED_COMMANDS } from "./commandCache";

const ORG_ID = "test_org_id";
const REGION_ID = "local/test";

describe("commandCache", () => {
  beforeEach(() => {
    window.localStorage.clear();
  });

  it("is of a migratable version", () => {
    const cache = getCache({ organizationId: ORG_ID, regionId: REGION_ID });
    // If you increment this version, you _must_ have a migration.
    expect(cache.version).toStrictEqual(1);
  });

  it("returns an empty state on first load", () => {
    const cache = getCache({ organizationId: ORG_ID, regionId: REGION_ID });
    expect(cache.commands).toHaveLength(0);
  });

  it("appended data can be read", () => {
    let cache = getCache({ organizationId: ORG_ID, regionId: REGION_ID });
    expect(cache.commands).toHaveLength(0);
    appendToCache({
      organizationId: ORG_ID,
      regionId: REGION_ID,
      command: "UNIT TEST",
    });
    cache = getCache({ organizationId: ORG_ID, regionId: REGION_ID });
    expect(cache.commands).toHaveLength(1);
    expect(cache.commands[0]).toEqual("UNIT TEST");
  });

  it("separate caches are maintained per organization and region", () => {
    let o1t1_cache = getCache({
      organizationId: "TEST_ORG_1",
      regionId: "local/test1",
    });
    let o1t2_cache = getCache({
      organizationId: "TEST_ORG_1",
      regionId: "local/test2",
    });
    let o2t1_cache = getCache({
      organizationId: "TEST_ORG_2",
      regionId: "local/test1",
    });
    appendToCache({
      organizationId: "TEST_ORG_1",
      regionId: "local/test1",
      command: "test1",
    });
    appendToCache({
      organizationId: "TEST_ORG_1",
      regionId: "local/test2",
      command: "test2",
    });
    appendToCache({
      organizationId: "TEST_ORG_2",
      regionId: "local/test1",
      command: "test3",
    });
    o1t1_cache = getCache({
      organizationId: "TEST_ORG_1",
      regionId: "local/test1",
    });
    o1t2_cache = getCache({
      organizationId: "TEST_ORG_1",
      regionId: "local/test2",
    });
    o2t1_cache = getCache({
      organizationId: "TEST_ORG_2",
      regionId: "local/test1",
    });
    expect(o1t1_cache.commands).toHaveLength(1);
    expect(o1t1_cache.commands[0]).toEqual("test1");
    expect(o1t2_cache.commands).toHaveLength(1);
    expect(o1t2_cache.commands[0]).toEqual("test2");
    expect(o2t1_cache.commands).toHaveLength(1);
    expect(o2t1_cache.commands[0]).toEqual("test3");
  });

  it("truncates the cache", () => {
    const commands = [];
    for (let x = 1; x <= MAX_CACHED_COMMANDS + 1; x++) {
      commands.push(`SELECT ${x};`);
    }
    expect(commands).toHaveLength(MAX_CACHED_COMMANDS + 1);
    for (const command of commands) {
      appendToCache({ organizationId: ORG_ID, regionId: REGION_ID, command });
    }
    const cache = getCache({ organizationId: ORG_ID, regionId: REGION_ID });
    expect(cache.commands).toHaveLength(MAX_CACHED_COMMANDS);
    expect(cache.commands[0]).toEqual(commands[1]);
    expect(cache.commands[MAX_CACHED_COMMANDS - 1]).toEqual(
      commands[MAX_CACHED_COMMANDS],
    );
  });
});
