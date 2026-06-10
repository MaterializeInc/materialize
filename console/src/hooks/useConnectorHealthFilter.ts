// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { HealthStatus } from "~/platform/connectors/utils";
import { useQueryStringState } from "~/useQueryString";

export const useConnectorHealthFilter = () => {
  const [selectedHealthStatus, setSelectedHealthStatus] =
    useQueryStringState("health");
  const defaultValue = "all";

  return {
    selected: (selectedHealthStatus as HealthStatus) ?? defaultValue,
    setSelected: setSelectedHealthStatus,
  };
};

export type UseConnectorHealthFilter = ReturnType<
  typeof useConnectorHealthFilter
>;
