# Copyright 2019 Timely Data, Inc. All rights reserved.
#
# This file is part of Materialize. Materialize may not be used or
# distributed without the express permission of Timely Data, Inc.
#
# crates.jq — helper to parse and render crate metadata for bin/doc.

.packages
  | sort_by(.name)
  | .[]
  | select(.manifest_path | startswith($pwd))
  | "<tr class='module-item'><td><a href='\(.name | @uri)/index.html' class='mod'>\(.name | @html)</a></td><td>\(.description | @html)</td></tr>"
