// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file at the
// root of this repository, or online at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Command-line parsing utilities.

use clap::Parser;

/// A help template for use with clap that does not include the name of the
/// binary or the version in the help output.
const NO_VERSION_HELP_TEMPLATE: &str = "{about}

USAGE:
    {usage}

{all-args}";

/// Parses command-line arguments according to a `StructOpt` parser after
/// applying Materialize-specific customizations.
pub fn parse_args<O>() -> O
where
    O: Parser,
{
    let clap = O::command()
        .disable_version_flag(true)
        .args_override_self(true)
        .help_template(NO_VERSION_HELP_TEMPLATE);
    O::from_arg_matches(&clap.get_matches()).unwrap()
}
