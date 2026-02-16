// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { SchemaObject } from "~/api/materialize";
import {
  relativeDatabasePath,
  relativeObjectPath,
} from "~/platform/object-explorer/routerHelpers";
import { useRegionSlug } from "~/store/environments";

export type RoutableObjectType =
  | "source"
  | "sink"
  | "index"
  | "materialized-view";

export const regionPath = (regionSlug: string) => {
  return `/regions/${regionSlug}`;
};

export type ClusterPathParams = {
  id: string;
  name: string;
};

export const absoluteClusterPath = (
  regionSlug: string,
  cluster: ClusterPathParams,
) => `${regionPath(regionSlug)}/clusters/${relativeClusterPath(cluster)}`;

export const relativeClusterPath = (cluster: ClusterPathParams) =>
  `${cluster.id}/${encodeURIComponent(cluster.name)}`;

/**
 * Function to switch to another cluster, while maintaining the rest of the route.
 */
export function replaceClusterIdAndName({
  pathname,
  currentClusterId,
  currentClusterName,
  targetCluster,
}: {
  pathname: string;
  currentClusterId: string;
  currentClusterName: string;
  targetCluster: ClusterPathParams;
}) {
  const toReplace = `${currentClusterId}/${currentClusterName}`;
  const replacement = `${targetCluster.id}/${targetCluster.name}`;
  return pathname.replace(toReplace, replacement);
}

export const ENVIRONMENT_NOT_READY_SLUG = "environment-not-ready";

export const environmentNotReadyPath = `/${ENVIRONMENT_NOT_READY_SLUG}`;

export const SHELL_SLUG = "shell";

export const shellPath = (regionSlug: string) => {
  return `${regionPath(regionSlug)}/${SHELL_SLUG}`;
};

export { shellPath as homePagePath };

export interface ObjectPathParams {
  databaseName: string | null;
  schemaName: string;
  objectType: string;
  objectName: string;
  id: string;
}

export function databasePath(regionSlug: string, databaseName: string) {
  return `${regionPath(regionSlug)}/objects/${relativeDatabasePath({ databaseName })}`;
}
export function objectExplorerObjectPath(
  regionSlug: string,
  params: ObjectPathParams,
) {
  return `${regionPath(regionSlug)}/objects/${relativeObjectPath(params)}`;
}

export function useBuildObjectPath() {
  const regionSlug = useRegionSlug();
  return (params: ObjectPathParams, _basePath?: string) =>
    objectExplorerObjectPath(regionSlug, params);
}

export interface ObjectWithoutTypePathParams {
  databaseName: string | null;
  schemaName: string;
  name: string;
  id: string;
}

export function useBuildMaterializedViewPath() {
  const regionSlug = useRegionSlug();
  return (params: ObjectWithoutTypePathParams) =>
    `${regionPath(regionSlug)}/objects/${relativeObjectPath({
      ...params,
      objectName: params.name,
      objectType: "materialized-view",
    })}`;
}

export function useBuildIndexPath() {
  const regionSlug = useRegionSlug();
  return (params: ObjectWithoutTypePathParams) =>
    objectExplorerObjectPath(regionSlug, {
      ...params,
      objectName: params.name,
      objectType: "index",
    });
}

export function useBuildSinkPath() {
  const regionSlug = useRegionSlug();
  const build = useBuildObjectPath();

  return (params: ObjectWithoutTypePathParams) =>
    build(
      { ...params, objectName: params.name, objectType: "sink" },
      `${regionPath(regionSlug)}/sinks/`,
    );
}

export function useBuildSourcePath() {
  const regionSlug = useRegionSlug();
  const build = useBuildObjectPath();

  return (params: ObjectWithoutTypePathParams) =>
    build(
      { ...params, objectName: params.name, objectType: "source" },
      `${regionPath(regionSlug)}/sources/`,
    );
}

export interface WorkflowGraphPathParams {
  type: string;
  databaseObject: SchemaObject;
  clusterId?: string | null;
  clusterName?: string | null;
}

export function useBuildWorkflowGraphPath() {
  const regionSlug = useRegionSlug();
  return (params: WorkflowGraphPathParams) =>
    `${objectExplorerObjectPath(regionSlug, {
      ...params.databaseObject,
      objectName: params.databaseObject.name,
      objectType: params.type,
    })}/workflow`;
}

export const relativeQueryHistoryPath = (executionId: string) =>
  `${executionId}`;

export const newConnectionPath = (regionSlug: string) => {
  return `${regionPath(regionSlug)}/sources/new/connection`;
};
