# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
#
# crates.jq — helper to parse and render crate metadata for bin/doc.

.packages
  | sort_by(.name)
  | .[]
  | select(.manifest_path | startswith($pwd))
  | "<tr class='module-item'><td><a href='\(.name | @uri)/index.html' class='mod'>\(.name | @html)</a></td><td>\(.description | @html)</td></tr>"
