// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { ConnectorStatus } from "~/api/materialize/types";
import { handleSortOrder, SortOrder } from "~/utils/sort";

export type Connector = {
  databaseName: string;
  schemaName: string;
  name: string;
  status: string;
  type: string;
};

type ConnectorStatusOrderMapping = {
  [k in ConnectorStatus]: number;
};
const connectorStatusSortOrder: ConnectorStatusOrderMapping = {
  stalled: 1,
  starting: 2,
  failed: 3,
  dropped: 4,
  paused: 5,
  created: 6,
  running: 7,
};

function statusSort(connector: Connector) {
  return connectorStatusSortOrder[connector.status as ConnectorStatus];
}

export function sortConnectors<T extends Connector>(
  objects: T[],
  selectedSortColumn: keyof Connector,
  selectedSortOrder: SortOrder,
): T[] {
  return objects.sort(
    handleSortOrder(selectedSortOrder, (a, b) => {
      if (selectedSortColumn === "name") {
        return (
          a["databaseName"].localeCompare(b["databaseName"]) ||
          a["schemaName"].localeCompare(b["schemaName"]) ||
          a["name"].localeCompare(b["name"])
        );
      }
      if (selectedSortColumn === "status") {
        return statusSort(a) - statusSort(b);
      }
      return a[selectedSortColumn].localeCompare(b[selectedSortColumn]);
    }),
  );
}
