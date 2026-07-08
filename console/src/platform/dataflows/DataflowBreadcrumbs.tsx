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

import { ChevronRightIcon } from "~/icons";
import { MaterializeTheme } from "~/theme";

import {
  type DataflowNode,
  type DataflowStructure,
  type NodeId,
} from "./dataflowGraph";

export const DataflowBreadcrumbs = ({
  structure,
  focusedScope,
  onNavigate,
}: {
  structure: DataflowStructure;
  focusedScope: NodeId;
  onNavigate: (scope: NodeId) => void;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  const path: DataflowNode[] = [];
  for (let id: NodeId | null = focusedScope; id; ) {
    const node: DataflowNode = structure.nodes.get(id)!;
    path.unshift(node);
    id = node.parent;
  }
  return (
    <HStack spacing={1} minWidth={0}>
      {path.map((node, i) =>
        i === path.length - 1 ? (
          <Text key={node.id} fontSize="sm" fontWeight="600" noOfLines={1}>
            {node.name}
          </Text>
        ) : (
          <React.Fragment key={node.id}>
            <Text
              as="button"
              fontSize="sm"
              color={colors.foreground.secondary}
              noOfLines={1}
              onClick={() => onNavigate(node.id)}
            >
              {node.name}
            </Text>
            <ChevronRightIcon
              boxSize="4"
              color={colors.foreground.secondary}
              aria-hidden
            />
          </React.Fragment>
        ),
      )}
    </HStack>
  );
};
