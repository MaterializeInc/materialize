// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import React from "react";

import { AllowedObjects } from "~/api/materialize/workflowGraph";
import { WorkflowGraphNode } from "~/api/materialize/workflowGraphNodes";
import IndexIcon from "~/svg/IndexIcon";
import MaterializedViewIcon from "~/svg/MaterializedViewIcon";
import { DataIcon } from "~/svg/nav/DataIcon";
import SinkIcon from "~/svg/SinkIcon";
import TableIcon from "~/svg/TableIcon";

export function nodeIcon(node: WorkflowGraphNode) {
  switch (node.type as AllowedObjects) {
    case "index":
      return <IndexIcon />;
    case "materialized-view":
      return <MaterializedViewIcon />;
    case "sink":
      return <SinkIcon />;
    case "source":
      return sourceIcon(node);
    case "table":
      return <TableIcon />;
  }
}

/**
 * The source icon is different for subsource nodes.
 */
function sourceIcon(node: WorkflowGraphNode) {
  if (node.sourceType === "subsource") {
    return <TableIcon />;
  }
  return <DataIcon />;
}
