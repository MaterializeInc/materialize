// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { HStack, Text, useTheme, VStack } from "@chakra-ui/react";
import React from "react";
import { Link } from "react-router-dom";

import TextLink from "~/components/TextLink";
import { ClockIcon, ClustersIcon, ProfileIcon } from "~/icons";
import { absoluteClusterPath } from "~/platform/routeHelpers";
import { useRegionSlug } from "~/store/environments";
import { MaterializeTheme } from "~/theme";
import { kebabToTitleCase } from "~/util";
import { formatDate, FRIENDLY_DATETIME_FORMAT } from "~/utils/dateFormat";

import { objectIcon } from "./icons";
import type { ObjectExplorerNodeType } from "./ObjectExplorerNode";

export const ObjectDetailItem = (props: {
  icon: React.ReactNode;
  label?: string;
  value: React.ReactNode;
}) => {
  const { colors } = useTheme<MaterializeTheme>();

  return (
    <HStack spacing="2" maxWidth="100%">
      <HStack spacing="1" color={colors.foreground.secondary}>
        {props.icon}
        {props.label && (
          <Text textStyle="text-small" noOfLines={1}>
            {props.label}
          </Text>
        )}
      </HStack>
      <Text textStyle="text-small" noOfLines={1}>
        {props.value}
      </Text>
    </HStack>
  );
};

export const ObjectDetailsContainer = (props: React.PropsWithChildren) => {
  return (
    <VStack alignItems="flex-start" spacing="6" width="100%">
      {props.children}
    </VStack>
  );
};

export const ObjectDetailsStrip = ({
  name,
  type,
  owner,
  createdAt,
  sourceType,
  isSourceTable,
  clusterId,
  clusterName,
}: {
  name: string;
  type: string;
  owner: string;
  createdAt: Date | null;
  sourceType?: string | null;
  isSourceTable?: boolean;
  clusterId?: string | null;
  clusterName?: string | null;
}) => {
  const regionSlug = useRegionSlug();

  return (
    <VStack alignItems="flex-start" spacing="0" width="100%">
      <Text textStyle="heading-md">{name}</Text>
      <HStack spacing="6" my="2" flexWrap="wrap" width="100%">
        <ObjectType
          type={type}
          sourceType={sourceType ?? undefined}
          isSourceTable={isSourceTable}
        />
        <ObjectOwner owner={owner} />
        <ObjectCreatedAt createdAt={createdAt} />
        <ObjectCluster
          regionSlug={regionSlug}
          clusterId={clusterId}
          clusterName={clusterName}
        />
      </HStack>
    </VStack>
  );
};

function formatObjectType(
  objectType: string,
  sourceType: string | undefined,
  isSourceTable?: boolean,
) {
  if (isSourceTable) {
    return "Subsource";
  }
  if (sourceType) {
    return kebabToTitleCase(sourceType) + " Source";
  }
  return kebabToTitleCase(objectType);
}

export const ObjectType = (props: {
  type: string;
  sourceType?: string;
  isSourceTable?: boolean;
}) => {
  return (
    <ObjectDetailItem
      icon={objectIcon(props.type as ObjectExplorerNodeType, props.sourceType)}
      value={formatObjectType(
        props.type,
        props.sourceType,
        props.isSourceTable,
      )}
    />
  );
};

export const ObjectOwner = ({ owner }: { owner: string }) => {
  return (
    <ObjectDetailItem icon={<ProfileIcon />} label="Owner" value={owner} />
  );
};

export const ObjectCreatedAt = ({ createdAt }: { createdAt: Date | null }) => {
  if (!createdAt) return null;

  return (
    <ObjectDetailItem
      icon={<ClockIcon />}
      label="Created at"
      value={formatDate(createdAt, FRIENDLY_DATETIME_FORMAT)}
    />
  );
};

export const ObjectCluster = ({
  regionSlug,
  clusterId,
  clusterName,
}: {
  regionSlug: string;
  clusterId?: string | null;
  clusterName?: string | null;
}) => {
  if (!clusterId || !clusterName) return null;

  return (
    <ObjectDetailItem
      icon={<ClustersIcon />}
      label="Cluster"
      value={
        <TextLink
          as={Link}
          to={absoluteClusterPath(regionSlug, {
            id: clusterId,
            name: clusterName,
          })}
        >
          {clusterName}
        </TextLink>
      }
    />
  );
};
