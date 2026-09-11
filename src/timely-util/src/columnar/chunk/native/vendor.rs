// Copyright (c) 2015 Frank McSherry
// SPDX-License-Identifier: MIT
// See vendor/LICENSE.

//! Differential spine scheduling with resumable maintenance.
//!
//! The spine is based on `TimelyDataflow/differential-dataflow` at `626c80ce`,
//! with local scheduling changes from `339f01522823171f2a8c710967502f3e1d09360b`.
//! It owns fuel grants, layer promotion, and atomic publication across suspension.
//! Batch storage, chunk operations, and polling contracts come from the
//! `differential-dataflow-next` dependency.

#![allow(dead_code)]

pub mod spine;

#[cfg(test)]
mod tests;
