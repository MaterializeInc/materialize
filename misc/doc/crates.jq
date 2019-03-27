.packages
  | sort_by(.name)
  | .[]
  | select(.manifest_path | startswith($pwd))
  | "<tr class='module-item'><td><a href='\(.name | @uri)/index.html' class='mod'>\(.name | @html)</a></td><td>\(.description | @html)</td></tr>"