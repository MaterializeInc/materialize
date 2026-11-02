// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { HStack, IconButton, Tag } from "@chakra-ui/react";
import React from "react";
import { Navigate, Route, useParams, useSearchParams } from "react-router-dom";

import { SettingsIcon } from "~/icons";
import { PageHeading, PageTabStrip, Tab } from "~/layouts/BaseLayout";
import { ClusterParams } from "~/platform/clusters/ClusterRoutes";
import { SentryRoutes } from "~/sentry";
import { useAllClusters } from "~/store/allClusters";
import { useSubscribeToAllObjectsCollection } from "~/store/allObjectsCollection";
import { assert, pluralize } from "~/util";

import { ClusterMetrics } from "./ClusterMetrics";
import { ClusterObjects } from "./ClusterObjects";
import { CLUSTER_OBJECT_TYPES } from "./clusterObjectTypes";
import { ClusterWorkers } from "./ClusterWorkers";
import { ClusterConfigurationDrawer } from "./Configuration/ClusterConfigurationDrawer";
import {
  CONFIGURATION_SECTIONS,
  ConfigurationSection,
} from "./Configuration/clusterConfigurationModel";
import { replicaSearch, WORKERS_TAB_PATH } from "./tabLinks";
import { REPLICA_SEARCH_PARAM } from "./useSelectedReplica";

// The Metrics and Workers tabs share a replica choice, so their links carry it.
const tabs = (replicaName: string | undefined): Tab[] => [
  { label: "Metrics", href: `..${replicaSearch(replicaName)}`, end: true },
  {
    label: "Workers",
    href: `../${WORKERS_TAB_PATH}${replicaSearch(replicaName)}`,
  },
  { label: "Objects", href: "../objects" },
];

// The configuration drawer's open section, in the URL so it survives a reload
// and the classic Replicas tab's links can open it.
const CONFIGURE_PARAM = "configure";

const replicaCountLabel = (count: number) =>
  count === 0
    ? "No replicas"
    : `${count} ${pluralize(count, "replica", "replicas")}`;

interface ClusterTitleProps {
  name: string;
  clusterId: string;
  onConfigure: () => void;
}

// The all-clusters subscribe updates on any cluster's replica status change.
// Looking the cluster up here keeps those updates from re-rendering the page.
const ClusterTitle = ({ name, clusterId, onConfigure }: ClusterTitleProps) => {
  const { getClusterById } = useAllClusters();
  const cluster = getClusterById(clusterId);
  return (
    <HStack px={7} pt={4} pb={1} spacing={3}>
      <PageHeading as="h1">{name}</PageHeading>
      {cluster && (
        <HStack spacing={2}>
          {cluster.managed && cluster.size && (
            <Tag size="sm">{cluster.size}</Tag>
          )}
          <Tag size="sm">{replicaCountLabel(cluster.replicas.length)}</Tag>
          {!cluster.managed && <Tag size="sm">Unmanaged</Tag>}
          <IconButton
            aria-label="Configure cluster"
            title="Configure cluster"
            icon={<SettingsIcon w="4" h="4" />}
            size="sm"
            variant="ghost"
            onClick={onConfigure}
          />
        </HStack>
      )}
    </HStack>
  );
};

const isSection = (value: string | null): value is ConfigurationSection =>
  CONFIGURATION_SECTIONS.includes(value as ConfigurationSection);

const useConfigurationSection = () => {
  const [searchParams, setSearchParams] = useSearchParams();
  const setSection = (next: ConfigurationSection | undefined) =>
    setSearchParams(
      (params) => {
        if (next) params.set(CONFIGURE_PARAM, next);
        else params.delete(CONFIGURE_PARAM);
        return params;
      },
      { replace: true },
    );
  return [searchParams.get(CONFIGURE_PARAM), setSection] as const;
};

interface ConfigurationDrawerProps {
  clusterId: string;
  section: string | null;
  onSectionChange: (section: ConfigurationSection | undefined) => void;
}

// Looks the cluster up itself, as the title does, for the same reason.
const ConfigurationDrawer = ({
  clusterId,
  section,
  onSectionChange,
}: ConfigurationDrawerProps) => {
  const { getClusterById } = useAllClusters();
  const cluster = getClusterById(clusterId);
  if (!cluster) return null;
  return (
    <ClusterConfigurationDrawer
      isOpen={isSection(section)}
      onClose={() => onSectionChange(undefined)}
      cluster={cluster}
      section={isSection(section) ? section : "overview"}
      onSectionChange={onSectionChange}
    />
  );
};

/** The redesigned page's part of the shared header: its title row and tabs. */
export const ClusterPageHeader = () => {
  const { clusterId, clusterName } = useParams<ClusterParams>();
  assert(clusterId);
  assert(clusterName);
  const [, setSection] = useConfigurationSection();
  const [searchParams] = useSearchParams();
  return (
    <>
      <ClusterTitle
        name={clusterName}
        clusterId={clusterId}
        onConfigure={() => setSection("overview")}
      />
      <PageTabStrip
        tabData={tabs(searchParams.get(REPLICA_SEARCH_PARAM) ?? undefined)}
      />
    </>
  );
};

const ClusterPage = () => {
  const { clusterId, clusterName } = useParams<ClusterParams>();
  assert(clusterId);
  assert(clusterName);
  useSubscribeToAllObjectsCollection();
  const [section, setSection] = useConfigurationSection();

  // Keying route elements on the cluster name avoids stale state when the
  // breadcrumb menu switches clusters.
  return (
    <>
      <SentryRoutes>
        <Route index element={<ClusterMetrics key={clusterName} />} />
        <Route
          path={WORKERS_TAB_PATH}
          element={<ClusterWorkers key={clusterName} />}
        />
        <Route
          path="objects"
          element={<Navigate to={CLUSTER_OBJECT_TYPES[0].path} replace />}
        />
        <Route
          path="objects/:objectType"
          element={<ClusterObjects key={clusterName} />}
        />
        <Route
          path="replicas"
          element={
            <Navigate
              to={{ pathname: "..", search: `?${CONFIGURE_PARAM}=replicas` }}
              replace
            />
          }
        />
        {/* The classic page's tab URLs, kept working for bookmarks and shared links. */}
        {CLUSTER_OBJECT_TYPES.map(({ path }) => (
          <Route
            key={path}
            path={path}
            element={<Navigate to={`../objects/${path}`} replace />}
          />
        ))}
      </SentryRoutes>
      <ConfigurationDrawer
        clusterId={clusterId}
        section={section}
        onSectionChange={setSection}
      />
    </>
  );
};

export default ClusterPage;
