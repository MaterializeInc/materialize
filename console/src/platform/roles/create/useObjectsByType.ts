// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { useMemo } from "react";

import { isSystemId } from "~/api/materialize";
import { useAllClusters } from "~/store/allClusters";
import { useAllObjects } from "~/store/allObjects";
import { useAllSchemas } from "~/store/allSchemas";

import { ObjectTypeId } from "./constants";

export type ObjectOption = {
  id: string;
  name: string;
  databaseName?: string;
  schemaName?: string;
};

/**
 * Hook that returns available objects filtered by the selected object type.
 * Combines data from clusters, schemas, and allObjects subscriptions.
 */
export function useObjectsByType(objectTypeId: ObjectTypeId | undefined) {
  const { data: clusters, snapshotComplete: clustersReady } = useAllClusters();
  const { data: schemas, snapshotComplete: schemasReady } = useAllSchemas({
    includeSystemSchemas: false,
  });
  const { data: allObjects, snapshotComplete: objectsReady } = useAllObjects();

  const objects = useMemo((): ObjectOption[] => {
    if (!objectTypeId) return [];

    if (objectTypeId === "system") {
      return [{ id: "SYSTEM", name: "SYSTEM" }];
    }

    if (objectTypeId === "cluster") {
      return clusters
        .filter((c) => !isSystemId(c.id))
        .map((c) => ({ id: c.id, name: c.name }));
    }

    if (objectTypeId === "database") {
      const seen = new Map<string, ObjectOption>();
      for (const s of schemas) {
        if (s.databaseId && s.databaseName && !isSystemId(s.databaseId)) {
          seen.set(s.databaseId, { id: s.databaseId, name: s.databaseName });
        }
      }
      return Array.from(seen.values());
    }

    if (objectTypeId === "schema") {
      return schemas.map((s) => ({
        id: s.id,
        name: s.name,
        databaseName: s.databaseName ?? undefined,
      }));
    }

    // Table, view, materialized-view, source, connection, secret, type
    return allObjects
      .filter((obj) => obj.objectType === objectTypeId && !isSystemId(obj.id))
      .map((obj) => ({
        id: obj.id,
        name: obj.name,
        databaseName: obj.databaseName ?? undefined,
        schemaName: obj.schemaName,
      }));
  }, [objectTypeId, clusters, schemas, allObjects]);

  const isLoading = (() => {
    if (!objectTypeId || objectTypeId === "system") return false;
    if (objectTypeId === "cluster") return !clustersReady;
    if (objectTypeId === "database" || objectTypeId === "schema")
      return !schemasReady;
    return !objectsReady;
  })();

  return { objects, isLoading };
}
