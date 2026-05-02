// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { sql } from "kysely";
import React from "react";

import { queryBuilder, useSqlTyped } from "~/api/materialize";
import { hasSuperUserPrivileges } from "~/api/materialize/expressionBuilders";

export const CAN_CREATE_CLUSTERS_QUERY_KEY = "canCreateCluster";

/**
 * Checks system privileges to see if a user can create clusters
 */
export default function useCanCreateCluster() {
  const query = React.useMemo(() => {
    return sql<{
      canCreateCluster: boolean;
    }>`SELECT (${hasSuperUserPrivileges()}) OR has_system_privilege(current_user, 'CREATECLUSTER') as "canCreateCluster";`.compile(
      queryBuilder,
    );
  }, []);
  const response = useSqlTyped(query, {
    queryKey: CAN_CREATE_CLUSTERS_QUERY_KEY,
  });

  let canCreateCluster = false;

  if (response.results && response.results[0]) {
    canCreateCluster = response.results[0].canCreateCluster;
  }

  return { ...response, results: canCreateCluster };
}
