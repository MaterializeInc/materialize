# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import glob
from importlib import import_module
from os.path import basename, dirname, isfile, join

# Automatically find all python files in this directory and import them when
# using "from materialize.checks.all_checks import *"
for f in glob.glob(join(dirname(__file__), "*.py")):
    if isfile(f) and basename(f) != "__init__.py":
        import_module(f".{basename(f).removesuffix('.py')}", __package__)
