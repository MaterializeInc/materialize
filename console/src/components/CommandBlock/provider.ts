// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  createContext,
  useView as useCodeMirrorView,
  useViewEffect as useCodeMirrorViewEffect,
  ViewEffectSetup,
} from "@codemirror-toolkit/react";

const { Provider, useCodeMirror } = createContext();

const useGetView = () => useCodeMirror().getView;

const useView = () => useCodeMirrorView(useCodeMirror());

/**
 * Runs `setup` against each view the provider creates.
 *
 * `setup` must have a stable identity, otherwise the effect is torn down and
 * re-run on every render.
 */
const useViewEffect = (setup: ViewEffectSetup) =>
  useCodeMirrorViewEffect(useCodeMirror(), setup);

export {
  Provider as _Provider,
  useCodeMirror,
  useGetView,
  useView,
  useViewEffect,
};
