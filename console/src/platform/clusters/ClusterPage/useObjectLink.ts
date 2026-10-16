// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { useFlags } from "~/hooks/useFlags";
import {
  absoluteMaintainedObjectPath,
  useBuildWorkflowGraphPath,
} from "~/platform/routeHelpers";
import { useRegionSlug } from "~/store/environments";

export interface LinkableObject {
  id: string | null;
  name: string | null;
  databaseName: string | null;
  schemaName: string | null;
  type: string | null;
}

/**
 * Builds an object's link: its Maintained Objects drawer, or its workflow
 * graph while that page is off. Undefined for an object missing a catalog
 * field, such as a dropped object's dataflow.
 */
export function useObjectLink() {
  // The maintained objects page only has routes while its flag is on.
  const hasMaintainedObjectsPage = Boolean(
    useFlags()["maintained-objects-ui-50"],
  );
  const regionSlug = useRegionSlug();
  const workflowGraphPath = useBuildWorkflowGraphPath();
  return ({ id, name, databaseName, schemaName, type }: LinkableObject) => {
    if (!id || !name || !schemaName || !type) return undefined;
    return hasMaintainedObjectsPage
      ? absoluteMaintainedObjectPath(regionSlug, id)
      : workflowGraphPath({
          databaseObject: { id, name, databaseName, schemaName },
          type,
        });
  };
}
