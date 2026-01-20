{{- /* Skill output: include-syntax renders code block and markdown table */ -}}
{{- $pathArray := split (lower (.Get "file")) "/" -}}
{{- $data := .Site.Data -}}
{{- range $pathArray }}
  {{- $data = index $data . -}}
{{- end }}
{{- $example := .Get "example" -}}
{{- $indent := .Get "indent" -}}
{{- range $data }}
{{- if eq .name $example -}}
{{- if .description }}
{{ .description }}
{{ end -}}
{{- if .code -}}
{{- $code := .code -}}
{{- if $indent }}
{{- $code = replaceRE "(?m)^" "   " $code -}}
{{- end }}

{{ if $indent }}   {{ end }}```mzsql
{{ $code | safeHTML }}
{{ if $indent }}   {{ end }}```
{{- end -}}
{{- if .syntax_elements }}

{{ partial "yaml-tables/generic-table.skill.md" (dict
  "columns" (slice
    (dict "column" "name" "header" "Syntax element")
    (dict "column" "description" "header" "Description")
  )
  "rows" .syntax_elements
) }}
{{- end -}}
{{- if .addenda }}

{{ .addenda }}
{{- end -}}
{{- end -}}
{{- end -}}
