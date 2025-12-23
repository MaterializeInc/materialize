{{- /* Skill output: render note as markdown blockquote */ -}}
> **Note:** {{ .Inner | replaceRE "^\\s+" "" | replaceRE "\\n" "\n> " }}
