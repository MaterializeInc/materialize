// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { renderHook, waitFor } from "@testing-library/react";

import { DatabaseObject } from "~/api/materialize/objects";
import { createProviderWrapper } from "~/test/utils";

import {
  allObjectsCollection,
  useClusterObjectsLive,
} from "./allObjectsCollection";

const MAINTAINED = ["index", "materialized-view"];

const object = (
  id: string,
  clusterId: string,
  name: string,
  objectType = "index",
): DatabaseObject => ({
  databaseName: "materialize",
  databaseId: "u1",
  name,
  schemaName: "public",
  schemaId: "u2",
  id,
  objectType,
  sourceType: null,
  isWebhookTable: null,
  clusterId,
  clusterName: clusterId,
});

const seed = (objects: DatabaseObject[]) =>
  allObjectsCollection.applySnapshot({
    data: objects,
    snapshotComplete: true,
    error: undefined,
  });

const renderBothClusters = async () => {
  const ProviderWrapper = await createProviderWrapper();
  return renderHook(
    () => ({
      mine: useClusterObjectsLive("u9", MAINTAINED),
      other: useClusterObjectsLive("u10", MAINTAINED),
    }),
    { wrapper: ProviderWrapper },
  );
};

describe("useClusterObjectsLive", () => {
  it("returns the cluster's objects of the given types", async () => {
    seed([
      object("u100", "u9", "orders_idx"),
      object("u101", "u9", "orders_view", "view"),
      object("u200", "u10", "other_idx"),
    ]);
    const { result } = await renderBothClusters();

    await waitFor(() =>
      expect(result.current.mine.data.map(({ id }) => id)).toEqual(["u100"]),
    );
  });

  it("keeps its result through changes on other clusters", async () => {
    seed([object("u100", "u9", "orders_idx"), object("u200", "u10", "a_idx")]);
    const { result } = await renderBothClusters();
    await waitFor(() => expect(result.current.mine.data).toHaveLength(1));
    const before = result.current.mine.data;

    seed([object("u100", "u9", "orders_idx"), object("u200", "u10", "b_idx")]);
    await waitFor(() =>
      expect(result.current.other.data[0]?.name).toEqual("b_idx"),
    );

    expect(result.current.mine.data).toBe(before);
  });

  it("updates when one of the cluster's objects changes", async () => {
    seed([object("u100", "u9", "orders_idx")]);
    const { result } = await renderBothClusters();
    await waitFor(() => expect(result.current.mine.data).toHaveLength(1));

    seed([object("u100", "u9", "orders_by_day_idx")]);

    await waitFor(() =>
      expect(result.current.mine.data[0]?.name).toEqual("orders_by_day_idx"),
    );
  });
});
