// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import IndexList from "~/platform/clusters/IndexList";
import MaterializedViewsList from "~/platform/clusters/MaterializedViewsList";
import Sinks from "~/platform/clusters/Sinks";
import Sources from "~/platform/clusters/Sources";

/** Object types listed on the Objects tab, keyed by their URL segment. */
export const CLUSTER_OBJECT_TYPES = [
  {
    path: "materialized-views",
    label: "Materialized Views",
    Component: MaterializedViewsList,
  },
  { path: "indexes", label: "Indexes", Component: IndexList },
  { path: "sources", label: "Sources", Component: Sources },
  { path: "sinks", label: "Sinks", Component: Sinks },
] as const;
