// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import React from "react";

import { SchemaIcon } from "~/icons";
import ConnectionIcon from "~/svg/ConnectionIcon";
import IndexIcon from "~/svg/IndexIcon";
import MaterializedViewIcon from "~/svg/MaterializedViewIcon";
import { DataIcon } from "~/svg/nav/DataIcon";
import { SecretIcon } from "~/svg/SecretIcon";
import SinkIcon from "~/svg/SinkIcon";
import TableIcon from "~/svg/TableIcon";
import ViewIcon from "~/svg/ViewIcon";

import { ObjectExplorerNodeType } from "./ObjectExplorerNode";

export function objectIcon(type: ObjectExplorerNodeType, sourceType?: string) {
  switch (type) {
    case "connection":
      return <ConnectionIcon />;
    case "database":
      return <DataIcon />;
    case "index":
      return <IndexIcon />;
    case "materialized-view":
      return <MaterializedViewIcon />;
    case "secret":
      return <SecretIcon />;
    case "schema":
      return <SchemaIcon />;
    case "sink":
      return <SinkIcon />;
    case "source": {
      if (sourceType === "subsource") {
        return <TableIcon />;
      }
      return <DataIcon />;
    }
    case "table":
      return <TableIcon />;
    case "view":
      return <ViewIcon />;
  }
}
