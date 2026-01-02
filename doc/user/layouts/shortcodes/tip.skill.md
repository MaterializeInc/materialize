{{- /* Skill output: render tip as markdown blockquote */ -}}
> **Tip:** {{ .Inner | replaceRE "^\\s+" "" | replaceRE "\\n" "\n> " }}
