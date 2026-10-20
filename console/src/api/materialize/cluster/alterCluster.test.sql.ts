// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { parseAutoScaling } from "~/platform/clusters/ClusterPage/Configuration/clusterConfigurationModel";
import {
  executeSqlHttp,
  getMaterializeClient,
} from "~/test/sql/materializeSqlClient";
import { testdrive } from "~/test/sql/mzcompose";

import {
  AlterClusterSettingsParams,
  alterClusterStatements,
} from "./alterCluster";
import {
  buildClusterAutoScalingQuery,
  buildClusterResizeTargetQuery,
} from "./clusterConfiguration";

const SMALL = "scale=1,workers=1";
const LARGE = "scale=1,workers=2";
const BURST = "scale=2,workers=2";

async function clusterIdByName(name: string) {
  const client = await getMaterializeClient();
  const {
    rows: [cluster],
  } = await client.query("SELECT id FROM mz_clusters WHERE name = $1", [name]);
  return cluster.id as string;
}

async function policyOf(name: string) {
  const id = await clusterIdByName(name);
  const { rows } = await executeSqlHttp(
    buildClusterAutoScalingQuery(id).compile(),
  );
  return parseAutoScaling(rows.at(0) ?? null).policy;
}

async function resizeTargetOf(name: string) {
  const { rows } = await executeSqlHttp(
    buildClusterResizeTargetQuery(await clusterIdByName(name)).compile(),
  );
  return rows.at(0)?.size;
}

// Runs the statements in order and stops at the first error, as the form's
// request does.
async function alter(
  settings: AlterClusterSettingsParams,
  name = settings.clusterName,
) {
  for (const statement of alterClusterStatements(settings, {
    clusterName: settings.clusterName,
    name,
  })) {
    await executeSqlHttp(statement);
  }
}

describe("alter cluster statements", () => {
  it("sets, changes and removes an autoscaling policy", async () => {
    await testdrive(`
      $ postgres-execute connection=postgres://mz_system@\${testdrive.materialize-internal-sql-addr}/materialize
      ALTER SYSTEM SET enable_auto_scaling_strategy = true

      > CREATE CLUSTER bursting (SIZE '${SMALL}');
    `);
    const settings = { clusterName: "bursting", size: SMALL, replicas: 1 };

    await alter({
      ...settings,
      autoScalingPolicy: { hydrationSize: LARGE, lingerMs: 1500 },
    });
    expect(await policyOf("bursting")).toEqual({
      hydrationSize: LARGE,
      lingerMs: 1500,
    });

    // A policy without a linger replaces the whole strategy, so the linger
    // goes back to the server's default.
    await alter({
      ...settings,
      autoScalingPolicy: { hydrationSize: LARGE, lingerMs: undefined },
    });
    expect(await policyOf("bursting")).toEqual({
      hydrationSize: LARGE,
      lingerMs: undefined,
    });

    // Undefined leaves the policy in place.
    await alter(settings);
    expect(await policyOf("bursting")).toEqual({
      hydrationSize: LARGE,
      lingerMs: undefined,
    });
  });

  it("removes a policy and resizes to its burst size in one statement, or does neither", async () => {
    await testdrive(`
      $ postgres-execute connection=postgres://mz_system@\${testdrive.materialize-internal-sql-addr}/materialize
      ALTER SYSTEM SET enable_auto_scaling_strategy = true

      > CREATE CLUSTER resized (SIZE '${SMALL}', AUTO SCALING STRATEGY = (ON HYDRATION (HYDRATION SIZE = '${LARGE}')));
    `);
    const [removeAndResize] = alterClusterStatements(
      { clusterName: "resized", size: LARGE, autoScalingPolicy: null },
      { clusterName: "resized", name: "resized" },
    );

    // A resize the server rejects keeps the policy too.
    await expect(
      alter({
        clusterName: "resized",
        size: "not-a-size",
        autoScalingPolicy: null,
      }),
    ).rejects.toThrow(/unknown cluster replica size/);
    expect(await policyOf("resized")).toEqual({
      hydrationSize: LARGE,
      lingerMs: undefined,
    });

    await executeSqlHttp(removeAndResize);
    expect(await policyOf("resized")).toBeNull();
    // The resize completes in the background; testdrive retries until it has.
    await testdrive(
      `> SELECT size FROM mz_clusters WHERE name = 'resized'
      ${LARGE}`,
      { noReset: true, timeoutSeconds: 30 },
    );
  });

  it("leaves an in-flight resize running when the policy or name changes, and reads its target", async () => {
    // The view hydrates for 15s, which holds the resize in flight that long.
    await testdrive(`
      $ postgres-execute connection=postgres://mz_system@\${testdrive.materialize-internal-sql-addr}/materialize
      ALTER SYSTEM SET enable_auto_scaling_strategy = true
      ALTER SYSTEM SET unsafe_enable_unstable_dependencies = true

      > CREATE CLUSTER resizing (SIZE '${SMALL}');
      > CREATE TABLE resizing_t (id int);
      > INSERT INTO resizing_t VALUES (1);
      > CREATE MATERIALIZED VIEW resizing_slow IN CLUSTER resizing AS
        SELECT mz_unsafe.mz_sleep(id * 15) AS s FROM resizing_t;
      > ALTER CLUSTER resizing SET (SIZE '${LARGE}');
    `);
    const id = await clusterIdByName("resizing");
    // The resize's status, and the size the cluster runs at.
    const expectResize = (row: string, timeoutSeconds = 1) =>
      testdrive(
        `> SELECT r.status, c.size FROM mz_internal.mz_cluster_reconfigurations r JOIN mz_clusters c ON c.id = r.cluster_id WHERE c.id = '${id}'
        ${row}`,
        { noReset: true, timeoutSeconds },
      );
    await expectResize(`in-progress ${SMALL}`);
    expect(await resizeTargetOf("resizing")).toBe(LARGE);

    // Why the form keeps the target out of the burst size.
    await expect(
      alter({
        clusterName: "resizing",
        autoScalingPolicy: { hydrationSize: LARGE, lingerMs: undefined },
      }),
    ).rejects.toThrow(
      /HYDRATION SIZE must differ from the target SIZE .* of the in-progress cluster resize/,
    );

    // What the form sends to enable a policy, remove it again, then rename,
    // without touching the size or replicas it opened with.
    await alter({
      clusterName: "resizing",
      autoScalingPolicy: { hydrationSize: BURST, lingerMs: undefined },
    });
    expect(await policyOf("resizing")).toEqual({
      hydrationSize: BURST,
      lingerMs: undefined,
    });
    await alter({ clusterName: "resizing", autoScalingPolicy: null });
    expect(await policyOf("resizing")).toBeNull();
    await alter({ clusterName: "resizing" }, "resizing_renamed");

    await expectResize(`in-progress ${SMALL}`);
    await expectResize(`finalized ${LARGE}`, 40);
  });
});
