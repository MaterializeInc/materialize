{{- /* Skill output: render important as markdown blockquote */ -}}
> **Important:** {{ .Inner | replaceRE "^\\s+" "" | replaceRE "\\n" "\n> " }}
