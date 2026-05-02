{{- /* Skill output: render warning as markdown blockquote */ -}}
> **Warning:** {{ .Inner | replaceRE "^\\s+" "" | replaceRE "\\s+$" "" | replaceRE "\\n\\s*\\n+" "\n" | replaceRE "\\n" "\n> " | replaceRE "\\n> \\s*$" "" }}
