# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# This file is needed as otherwise pytest --doctest-module can not
# disambiguate between the two instances of `scalability` in misc/python/materialize

# The pass is needed to keep pydoc happy, as it errors out on empty files
pass
