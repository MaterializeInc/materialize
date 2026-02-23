// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import React from "react";

import { EventEmitterContextType } from "~/types/charts";

// This file was copied from the visx XYchart:
// https://github.com/airbnb/visx/blob/dee0ac097cc3f53f4d81d1d99b736bf0e3c7ac1c/packages/visx-xychart/src/context/EventEmitterContext.tsx

export const EventEmitterContext =
  React.createContext<EventEmitterContextType | null>(null);
