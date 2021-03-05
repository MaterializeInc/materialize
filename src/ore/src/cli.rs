// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Apache license, Version 2.0

//! Command-line parsing utilities.

use structopt::clap::AppSettings;
use structopt::StructOpt;

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
    O: StructOpt,
{
    let clap = O::clap()
        .global_setting(AppSettings::DisableVersion)
        .global_setting(AppSettings::UnifiedHelpMessage)
        .template(NO_VERSION_HELP_TEMPLATE);
    O::from_clap(&clap.get_matches())
}
