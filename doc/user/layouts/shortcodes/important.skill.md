{{- /* Skill output: render important as markdown blockquote */ -}}
> **Important:** {{ .Inner | replaceRE "^\\s+" "" | replaceRE "\\s+$" "" | replaceRE "\\n\\s*\\n+" "\n" | replaceRE "\\n" "\n> " | replaceRE "\\n> \\s*$" "" }}
