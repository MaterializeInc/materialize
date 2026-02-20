// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import mitt from "mitt";
import React from "react";

import { EventEmitterEvents } from "~/types/charts";

import { EventEmitterContext } from "./Context";

export const EventEmitterProvider = ({
  children,
}: {
  children: React.ReactNode;
}) => {
  const emitter = React.useMemo(() => mitt<EventEmitterEvents>(), []);
  return (
    <EventEmitterContext.Provider value={emitter}>
      {children}
    </EventEmitterContext.Provider>
  );
};
