// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { VStack } from "@chakra-ui/react";
import React from "react";

import { CommandBlockOutputContainer } from "./CommandBlockOutputContainer";
import SqlSelectTable from "./SqlSelectTable";

const LocalCommandOutput = ({
  command,
  commandResults,
}: {
  command: string;
  commandResults: string[][];
}) => {
  return (
    <VStack spacing="3" alignItems="flex-start" minWidth="0">
      <CommandBlockOutputContainer command={command}>
        <SqlSelectTable
          colNames={["Command", "Description"]}
          paginatedRows={commandResults}
        />
      </CommandBlockOutputContainer>
    </VStack>
  );
};

export default LocalCommandOutput;
