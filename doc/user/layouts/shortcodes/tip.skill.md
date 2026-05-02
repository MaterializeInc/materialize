{{- /* Skill output: render tip as markdown blockquote */ -}}
> **Tip:** {{ .Inner | replaceRE "^\\s+" "" | replaceRE "\\s+$" "" | replaceRE "\\n\\s*\\n+" "\n" | replaceRE "\\n" "\n> " | replaceRE "\\n> \\s*$" "" }}
