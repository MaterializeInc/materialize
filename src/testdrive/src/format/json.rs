// Copyright 2018 Flavien Raynaud
// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

pub fn parse_many(input: &str) -> Result<Vec<serde_json::Value>, serde_json::Error> {
    serde_json::Deserializer::from_str(input)
        .into_iter()
        .collect()
}
