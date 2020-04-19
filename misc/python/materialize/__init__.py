# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Package `materialize` is the top-level Python package for Materialize, Inc.

While the primary product, [materialized], is written in Rust, various demos and
build tools are written in Python. This package enables the sharing of code
between those scripts.

Consider writing additional Python code when:

  * You are writing scripts intended to be run by users of Materialize, not
    developers, e.g. automation for a complicated demo. We can reasonably ask
    users to install Python 3.6+. A Rust toolchain is a much bigger ask.

  * You are about to write a large Bash script.

  * You've already written a Bash script whose complexity and maintainability
    has deteriorated as the script's responsibilities have grown.

[materialized]: https://mtrlz.dev/api/materialized/index.html
"""
