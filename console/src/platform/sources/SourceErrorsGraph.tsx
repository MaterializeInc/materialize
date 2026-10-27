// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import React from "react";

import { ConnectorErrorGraph } from "~/components/ConnectorErrorsGraph";

import { useBucketedSourceErrors } from "./queries";

export interface Props {
  sourceId: string;
  timePeriodMinutes: number;
}

const SourceErrorsGraph = ({ sourceId, timePeriodMinutes }: Props) => {
  const bucketSizeSeconds = React.useMemo(() => {
    return (timePeriodMinutes / 15) * 60;
  }, [timePeriodMinutes]);
  const { data } = useBucketedSourceErrors({
    sourceId,
    bucketSizeSeconds,
    timePeriodMinutes,
  });

  return (
    <ConnectorErrorGraph
      startTime={data.startTime}
      endTime={data.endTime}
      connectorStatuses={data.rows}
      bucketSizeMs={bucketSizeSeconds * 1000}
      timePeriodMinutes={timePeriodMinutes}
    />
  );
};

export default SourceErrorsGraph;
