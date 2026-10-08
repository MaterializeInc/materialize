// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { renderHook, waitFor } from "@testing-library/react";

import { ReplicaObjectSize } from "~/api/materialize/cluster/largestMaintainedQueries";
import { DatabaseObject } from "~/api/materialize/objects";
import { createSubscribeCollection } from "~/api/materialize/subscribeCollection";
import { allObjectsCollection } from "~/store/allObjectsCollection";
import { createProviderWrapper } from "~/test/utils";

import { useLargestObjects } from "./useLiveLargestObjects";

const MIB = 2 ** 20;
const HEAP_LIMIT = 1000 * MIB;

const catalogObject = (id: string, name: string): DatabaseObject => ({
  databaseName: "materialize",
  databaseId: "u1",
  name,
  schemaName: "public",
  schemaId: "u2",
  id,
  objectType: "index",
  sourceType: null,
  isWebhookTable: null,
  clusterId: "u9",
  clusterName: "prod",
});

const renderLargestObjects = async (sizes: ReplicaObjectSize[]) => {
  const collection = createSubscribeCollection<ReplicaObjectSize>({
    id: `test-sizes-${Math.random()}`,
    getKey: (row) => row.objectId,
  });
  collection.applySnapshot({
    data: sizes,
    snapshotComplete: true,
    error: undefined,
  });
  const ProviderWrapper = await createProviderWrapper();
  return renderHook(() => useLargestObjects(collection, HEAP_LIMIT), {
    wrapper: ProviderWrapper,
  });
};

describe("useLargestObjects", () => {
  beforeEach(() => {
    allObjectsCollection.applySnapshot({
      data: [
        catalogObject("u1", "orders_idx"),
        catalogObject("u2", "items_idx"),
      ],
      snapshotComplete: true,
      error: undefined,
    });
  });

  it("names the largest objects from the catalog, largest first", async () => {
    const { result } = await renderLargestObjects([
      { objectId: "u1", size: 100 * MIB },
      { objectId: "u2", size: 300 * MIB },
      { objectId: "u3", size: null },
    ]);

    await waitFor(() =>
      expect(
        result.current.objects.map(({ name, memoryPercentage }) => [
          name,
          memoryPercentage,
        ]),
      ).toEqual([
        ["items_idx", 30],
        ["orders_idx", 10],
        ["u3", null],
      ]),
    );
  });

  it("marks an object missing from the catalog as an orphaned dataflow", async () => {
    const { result } = await renderLargestObjects([
      { objectId: "u404", size: 50 * MIB },
    ]);

    await waitFor(() =>
      expect(result.current.objects[0]).toMatchObject({
        id: "u404",
        name: "u404",
        isOrphanedDataflow: true,
      }),
    );
  });

  it("keeps only the ten largest", async () => {
    const { result } = await renderLargestObjects(
      Array.from({ length: 12 }, (_, index) => ({
        objectId: `u${100 + index}`,
        size: (index + 1) * MIB,
      })),
    );

    await waitFor(() => expect(result.current.objects).toHaveLength(10));
    expect(result.current.objects.at(-1)?.id).toEqual("u102");
  });
});
