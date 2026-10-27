{{- /* Skill output: render note as markdown blockquote */ -}}
> **Note:** {{ .Inner | replaceRE "^\\s+" "" | replaceRE "\\s+$" "" | replaceRE "\\n\\s*\\n+" "\n" | replaceRE "\\n" "\n> " | replaceRE "\\n> \\s*$" "" }}
