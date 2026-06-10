// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import React from "react";

import { Notice } from "~/api/materialize/types";

import NoticeOutput from "./NoticeOutput";
import PlanInsightsNotice, {
  PLAN_INSIGHTS_NOTICE_CODE,
} from "./plan-insights/PlanInsightsNotice";
import { CommandOutput } from "./store/shell";

const CommandResultNotice = ({
  notice,
  commandOutput,
  commandResultIndex,
}: {
  notice: Notice;
  commandOutput: CommandOutput;
  commandResultIndex: number;
}) => {
  if (notice.code === PLAN_INSIGHTS_NOTICE_CODE) {
    return (
      <PlanInsightsNotice
        notice={notice}
        commandOutput={commandOutput}
        commandResultIndex={commandResultIndex}
      />
    );
  }
  return <NoticeOutput notice={notice} />;
};
export default CommandResultNotice;
