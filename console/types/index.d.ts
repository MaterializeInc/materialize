// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

declare module "*.svg?react" {
  import { ComponentWithAs, IconProps } from "@chakra-ui/react";

  const Icon: ComponentWithAs<"svg", IconProps>;

  export default Icon;
}

declare module "*.png" {
  const src: string;
  export default src;
}

declare module "*.svg" {
  import * as React from "react";

  export const ReactComponent: React.FunctionComponent<
    React.SVGProps<SVGSVGElement> & { title?: string }
  >;

  const src: string;
  export default src;
}

declare type Optional<T, K extends keyof T> = Pick<Partial<T>, K> & Omit<T, K>;
