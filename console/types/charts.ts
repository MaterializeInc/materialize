// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Emitter } from "mitt";
import { FocusEvent, PointerEvent } from "react";

// This code was copied from the visx XYchart:
// https://github.com/airbnb/visx/tree/dee0ac097cc3f53f4d81d1d99b736bf0e3c7ac1c/packages/visx-xychart/src/types

export type ChartEvent<
  Type extends PointerEvent | FocusEvent = PointerEvent | FocusEvent,
> = {
  /** The react PointerEvent or FocusEvent. */
  event: Type;
  /** The source of the event. This can be anything, but for this package is the name of the component which emitted the event. */
  source?: string;
};

export type EventEmitterEvents = {
  pointermove: ChartEvent<PointerEvent>;
  pointerout: ChartEvent<PointerEvent>;
};

export type EventType = keyof EventEmitterEvents;

export type EmitterEventType = keyof EventEmitterEvents;

export type EventEmitterContextType = Emitter<EventEmitterEvents>;

export type Handler = (
  event: ChartEvent<React.FocusEvent | React.PointerEvent>,
) => void;
