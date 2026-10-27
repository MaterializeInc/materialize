// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import React from "react";

export type ErrorBoundaryProps = {
  children?: React.ReactNode;
  fallback: React.ReactElement;
  onError?: (error: unknown, errorInfo: React.ErrorInfo) => void;
};

export class ExpectedErrorBoundary extends React.Component<
  ErrorBoundaryProps,
  { error: null | unknown }
> {
  constructor(props: ErrorBoundaryProps) {
    super(props);
    this.state = { error: null };
  }

  static getDerivedStateFromError(error: unknown) {
    return { error };
  }

  public componentDidCatch(error: unknown, errorInfo: React.ErrorInfo): void {
    if (this.props.onError) {
      this.props.onError(error, errorInfo);
    }
  }

  render() {
    if (this.state.error) {
      return this.props.fallback;
    }
    return this.props.children;
  }
}
