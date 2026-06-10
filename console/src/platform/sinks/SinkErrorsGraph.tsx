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

import { useBucketedSinkErrors } from "./queries";

export interface Props {
  sinkId: string;
  timePeriodMinutes: number;
}

const SinkErrorsGraph = ({ sinkId, timePeriodMinutes }: Props) => {
  const bucketSizeSeconds = React.useMemo(() => {
    return (timePeriodMinutes / 15) * 60;
  }, [timePeriodMinutes]);
  const { data } = useBucketedSinkErrors({
    sinkId,
    timePeriodMinutes,
    bucketSizeSeconds,
  });

  const bucketSizeMs = bucketSizeSeconds * 1000;

  return (
    <ConnectorErrorGraph
      startTime={data.startTime}
      endTime={data.endTime}
      connectorStatuses={data.rows}
      bucketSizeMs={bucketSizeMs}
      timePeriodMinutes={timePeriodMinutes}
    />
  );
};

export default SinkErrorsGraph;
