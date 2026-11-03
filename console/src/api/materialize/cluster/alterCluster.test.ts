// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { queryBuilder } from "..";
import {
  alterClusterNameStatement,
  alterClusterSettingsStatement,
  alterClusterStatements,
  lingerInterval,
} from "./alterCluster";

const SETTINGS = { clusterName: "prod", size: "400cc", replicas: 1 };
const RENAME = { clusterName: "prod", name: "prod" };

const statements = (
  autoScalingPolicy: Parameters<
    typeof alterClusterStatements
  >[0]["autoScalingPolicy"],
) =>
  alterClusterStatements({ ...SETTINGS, autoScalingPolicy }, RENAME).map(
    ({ sql, parameters }) => {
      expect(parameters).toEqual([]);
      return sql.trim();
    },
  );

describe("queries", () => {
  it("alterClusterSettingsStatement produces the expected query", () => {
    const { sql, parameters } = alterClusterSettingsStatement({
      clusterName: "prod_sources",
      size: "2xlarge",
      replicas: 2,
    })!.compile(queryBuilder);
    expect({ sql, parameters }).toMatchSnapshot();
  });

  it("alterClusterNameStatement produces the expected query", () => {
    const { sql, parameters } = alterClusterNameStatement({
      clusterName: "old_name",
      name: "prod_sources",
    }).compile(queryBuilder);
    expect({ sql, parameters }).toMatchSnapshot();
  });
});

describe("alterClusterStatements", () => {
  it("sets the policy in the same SET as the size and replication factor", () => {
    expect(statements({ hydrationSize: "800cc", lingerMs: 15_000 })).toEqual([
      [
        `ALTER CLUSTER "prod" SET (`,
        "  SIZE = '400cc',",
        "  REPLICATION FACTOR = 1,",
        "  AUTO SCALING STRATEGY = (",
        "    ON HYDRATION (HYDRATION SIZE = '800cc', LINGER DURATION = '15s')",
        "  )",
        ")",
      ].join("\n"),
    ]);
  });

  it("leaves LINGER DURATION out for the default", () => {
    expect(
      statements({ hydrationSize: "800cc", lingerMs: undefined })[0],
    ).toContain("ON HYDRATION (HYDRATION SIZE = '800cc')\n");
  });

  it("removes the policy in the same SET as a resize", () => {
    expect(statements(null)).toEqual([
      [
        `ALTER CLUSTER "prod" SET (`,
        "  SIZE = '400cc',",
        "  REPLICATION FACTOR = 1,",
        "  AUTO SCALING STRATEGY = ()",
        ")",
      ].join("\n"),
    ]);
  });

  it("removes the policy alone with a RESET", () => {
    expect(
      alterClusterStatements(
        { clusterName: "prod", autoScalingPolicy: null },
        RENAME,
      ).map(({ sql }) => sql.trim()),
    ).toEqual([`ALTER CLUSTER "prod" RESET (AUTO SCALING STRATEGY)`]);
  });

  it("sends nothing for an unchanged policy", () => {
    expect(statements(undefined)).toEqual([
      `ALTER CLUSTER "prod" SET (\n  SIZE = '400cc',\n  REPLICATION FACTOR = 1\n)`,
    ]);
  });

  it("names only the settings that change", () => {
    const only = (settings: Partial<typeof SETTINGS>) =>
      alterClusterStatements({ clusterName: "prod", ...settings }, RENAME).map(
        ({ sql }) => sql.trim(),
      );
    expect(only({ size: "800cc" })).toEqual([
      `ALTER CLUSTER "prod" SET (\n  SIZE = '800cc'\n)`,
    ]);
    expect(only({ replicas: 0 })).toEqual([
      `ALTER CLUSTER "prod" SET (\n  REPLICATION FACTOR = 0\n)`,
    ]);
    expect(
      alterClusterStatements(
        {
          clusterName: "prod",
          autoScalingPolicy: { hydrationSize: "800cc", lingerMs: undefined },
        },
        RENAME,
      ).map(({ sql }) => sql.trim()),
    ).toEqual([
      [
        `ALTER CLUSTER "prod" SET (`,
        "  AUTO SCALING STRATEGY = (",
        "    ON HYDRATION (HYDRATION SIZE = '800cc')",
        "  )",
        ")",
      ].join("\n"),
    ]);
  });

  it("renames only to a new name, and sends nothing when nothing changes", () => {
    const renameTo = (name: string) =>
      alterClusterStatements(
        { clusterName: "prod" },
        { clusterName: "prod", name },
      ).map(({ sql }) => sql.trim());
    expect(renameTo("prod")).toEqual([]);
    expect(renameTo("renamed")).toEqual([
      `ALTER CLUSTER "prod" RENAME TO "renamed"`,
    ]);
    expect(
      alterClusterStatements(
        { ...SETTINGS, autoScalingPolicy: undefined },
        { clusterName: "prod", name: "renamed" },
      ).map(({ sql }) => sql.trim()),
    ).toEqual([
      `ALTER CLUSTER "prod" SET (\n  SIZE = '400cc',\n  REPLICATION FACTOR = 1\n)`,
      `ALTER CLUSTER "prod" RENAME TO "renamed"`,
    ]);
    expect(alterClusterSettingsStatement({ clusterName: "prod" })).toBe(
      undefined,
    );
  });

  it("quotes the cluster name and escapes the sizes", () => {
    const [reset] = alterClusterStatements(
      { clusterName: `my "cluster"`, autoScalingPolicy: null },
      { clusterName: `my "cluster"`, name: "renamed" },
    );
    expect(reset.sql).toBe(
      `ALTER CLUSTER "my ""cluster""" RESET (AUTO SCALING STRATEGY)`,
    );
    expect(
      statements({ hydrationSize: "it's", lingerMs: undefined })[0],
    ).toContain("HYDRATION SIZE = 'it''s'");
  });
});

describe("lingerInterval", () => {
  it("writes whole seconds in seconds, and keeps zero and sub-seconds exact", () => {
    expect(lingerInterval(0)).toBe("0s");
    expect(lingerInterval(120_000)).toBe("120s");
    expect(lingerInterval(1_500)).toBe("1500ms");
    expect(lingerInterval(250)).toBe("250ms");
    // `{ secs: 1, nanos: 32_053_000 }`, which parses to 1032.0529999999999.
    expect(lingerInterval(1_000 + 32_053_000 / 1e6)).toBe("1032.053ms");
  });
});
