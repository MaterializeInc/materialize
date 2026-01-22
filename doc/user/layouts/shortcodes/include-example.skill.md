{{- /* Skill output: include-example renders code examples from YAML */ -}}
{{- $pathArray := split (lower (.Get "file")) "/" -}}
{{- $data := .Site.Data -}}
{{- range $pathArray }}
  {{- $data = index $data . -}}
{{- end }}
{{- $example := .Get "example" -}}
{{- $indent := .Get "indent" -}}
{{- range $data }}
{{- if eq .name $example -}}
{{- .description -}}
{{- if .code -}}
{{- $code := .code -}}
{{- if $indent }}
{{- $code = replaceRE "(?m)^" "   " $code -}}
{{- end }}

{{ if $indent }}   {{ end }}```mzsql
{{ $code | safeHTML }}
{{ if $indent }}   {{ end }}```
{{- end -}}
{{- if .results }}

{{- if $indent }}   {{ end }}{{ .results -}}
{{- end -}}
{{- end -}}
{{- end -}}
