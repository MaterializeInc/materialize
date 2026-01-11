{{- /* Skill output: render warning as markdown blockquote */ -}}
> **Warning:** {{ .Inner | replaceRE "^\\s+" "" | replaceRE "\\n" "\n> " }}
