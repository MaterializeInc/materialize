// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { queryBuilder } from "..";
import {
  alterClusterNameStatement,
  alterClusterSettingsStatement,
} from "./alterCluster";

describe("queries", () => {
  it("alterClusterSettingsStatement produces the expected query", () => {
    const { sql, parameters } = alterClusterSettingsStatement({
      clusterName: "prod_sources",
      size: "2xlarge",
      replicas: 2,
    }).compile(queryBuilder);
    expect({ sql, parameters }).toMatchSnapshot();
  });

  it("alterClusterNameStatement produces the expected query", () => {
    const { sql, parameters } = alterClusterNameStatement({
      clusterName: "old_name",
      name: "prod_sources",
    }).compile(queryBuilder);
    expect({ sql, parameters }).toMatchSnapshot();
  });
});
