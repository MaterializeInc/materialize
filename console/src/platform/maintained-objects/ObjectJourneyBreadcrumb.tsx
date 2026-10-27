// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { HStack, Text, useTheme } from "@chakra-ui/react";
import React from "react";
import { Link as RouterLink, useLocation } from "react-router-dom";

import TextLink from "~/components/TextLink";
import { ChevronRightIcon } from "~/icons";
import { useAllObjects } from "~/store/allObjects";
import { MaterializeTheme } from "~/theme";

export interface ObjectJourneyBreadcrumbProps {
  currentId: string;
  currentName: string;
}

export const ObjectJourneyBreadcrumb = ({
  currentId,
  currentName,
}: ObjectJourneyBreadcrumbProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  const { state } = useLocation();
  const journey: string[] = state?.journey ?? [];
  const visibleJourney = journey.filter((id) => id !== currentId);

  const { data: allObjects } = useAllObjects();
  const nameById = React.useMemo(
    () => new Map(allObjects.map((o) => [o.id, o.name])),
    [allObjects],
  );

  return (
    <HStack spacing={1} minWidth={0}>
      {visibleJourney.map((id, i) => (
        <React.Fragment key={`${id}-${i}`}>
          <TextLink
            as={RouterLink}
            to={`../${id}`}
            relative="path"
            state={{ journey: visibleJourney.slice(0, i) }}
            textStyle="heading-sm"
            noOfLines={1}
          >
            {nameById.get(id) ?? id}
          </TextLink>
          <ChevronRightIcon
            boxSize="4"
            color={colors.foreground.secondary}
            aria-hidden
          />
        </React.Fragment>
      ))}
      <Text textStyle="heading-sm" noOfLines={1} title={currentName}>
        {currentName}
      </Text>
    </HStack>
  );
};
