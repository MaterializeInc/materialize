// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { createCodeMirrorWithContext } from "@codemirror-toolkit/react";

const {
  Provider,
  useGetView,
  useView,
  useViewDispatch,
  useViewEffect,
  useContainerRef,
} = createCodeMirrorWithContext<HTMLDivElement>("CodeMirrorContext");

export {
  Provider as _Provider,
  useContainerRef,
  useGetView,
  useView,
  useViewDispatch,
  useViewEffect,
};
