{{- /* Skill output: render annotation as blockquote */ -}}
{{- $type := .Get "type" | default "Note" -}}
> **{{ $type }}:** {{ .Inner | replaceRE "^\\s+" "" | replaceRE "\\n\\s*" " " }}
