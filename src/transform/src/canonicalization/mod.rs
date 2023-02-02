// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Transformations that bring relation expressions to their canonical form.
//!
//! This is achieved  by:
//! 1. Bringing enclosed scalar expressions to a canonical form,
//! 2. Converting / peeling off part of the enclosing relation expression into
//!    another relation expression that can represent the same concept.
