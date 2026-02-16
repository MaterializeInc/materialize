// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { StackProps, VStack } from "@chakra-ui/react";
import { useAtom } from "jotai";
import React, { forwardRef } from "react";

import { CommandBlockOutputContainer } from "./CommandBlockOutputContainer";
import CommandResult from "./CommandResult";
import LocalCommandOutput from "./LocalCommandOutput";
import NoticeOutput from "./NoticeOutput";
import {
  CommandOutput as CommandOutputType,
  CommandResult as CommandResultType,
  HistoryId,
  historyItemAtom,
  historyItemCommandResultsSelector,
} from "./store/shell";

type HistoryOutputProps = {
  historyId?: HistoryId;
} & StackProps;

const CommandOutput = ({
  commandOutput,
  commandResults,
}: {
  commandOutput: CommandOutputType;
  commandResults: CommandResultType[];
}) => {
  const error = commandOutput.error;

  return (
    <CommandBlockOutputContainer
      commandBlockContainerProps={{
        width: "100%",
        overflow: "auto",
      }}
      command={commandOutput.command}
    >
      <>
        {commandResults.map((commandResult, commandResultIdx) => {
          return (
            <CommandResult
              key={commandResultIdx}
              commandResultIndex={commandResultIdx}
              commandResult={commandResult}
              commandOutput={commandOutput}
              error={
                commandResultIdx === commandResults.length - 1
                  ? error
                  : undefined
              }
            />
          );
        })}
      </>
    </CommandBlockOutputContainer>
  );
};

export const HistoryOutput = forwardRef<HTMLDivElement, HistoryOutputProps>(
  (props, ref) => {
    const { historyId, ...rest } = props;
    const [historyOutput] = useAtom(historyItemAtom(historyId ?? ""));
    const [commandResults] = useAtom(
      historyItemCommandResultsSelector(historyId ?? ""),
    );
    if (!historyOutput) return;

    return (
      <VStack
        alignItems="flex-start"
        width="100%"
        p="3"
        spacing={0}
        ref={ref}
        {...rest}
      >
        {historyOutput.kind === "notice" ? (
          <NoticeOutput notice={historyOutput} />
        ) : historyOutput.kind === "localCommand" ? (
          <LocalCommandOutput
            command={historyOutput.command}
            commandResults={historyOutput.commandResults}
          />
        ) : (
          <CommandOutput
            commandOutput={historyOutput}
            commandResults={commandResults ?? []}
          />
        )}
      </VStack>
    );
  },
);

export default HistoryOutput;
