// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import React from "react";

import { EventEmitterContext } from "~/components/EventEmitter";
import { EventType, Handler } from "~/types/charts";

// This code was copied from the visx XYChart and then simplified:
// https://github.com/airbnb/visx/blob/dee0ac097cc3f53f4d81d1d99b736bf0e3c7ac1c/packages/visx-xychart/src/hooks/useEventEmitter.ts#L61

/**
 * Hook for subscribing to a specified EventType.
 */
export function useHandleEmittedEvent(
  /** Type of event to subscribe to. */
  eventType: EventType,
  /** Handler invoked on emission of EventType event.  */
  handler: Handler,
  /** Optional valid sources for EventType subscription. */
  allowedSources?: string[],
) {
  const emitter = React.useContext(EventEmitterContext);
  const allowedSourcesRef = React.useRef<string[] | undefined>();

  React.useEffect(() => {
    // Use a ref so allowedSources can change without recreating handlers
    allowedSourcesRef.current = allowedSources;
  }, [allowedSources]);

  React.useEffect(() => {
    if (emitter) {
      // register handler, with source filtering as needed
      const handlerWithSourceFilter: Handler = (event) => {
        if (
          !allowedSourcesRef.current ||
          (event?.source && allowedSourcesRef.current?.includes(event.source))
        ) {
          handler(event);
        }
      };
      emitter.on(eventType, handlerWithSourceFilter);
      return () => emitter?.off(eventType, handlerWithSourceFilter);
    }
    return undefined;
  }, [emitter, eventType, handler]);

  return emitter;
}
