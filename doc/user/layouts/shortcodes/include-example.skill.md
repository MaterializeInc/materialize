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
{{- /*
  A description can end without a newline, as a "|-" YAML block does, and a
  fence at the end of a line of text is text, not an opener. Separate the
  description, the code and the results with blank lines.
*/ -}}
{{- if .description -}}
{{ trim .description "\n" }}{{ if .code }}{{ "\n\n" }}{{ end }}
{{- end -}}
{{- if .code -}}
{{- $code := .code -}}
{{- if $indent }}
{{- $code = replaceRE "(?m)^" "   " $code -}}
{{- end }}
{{- /* Trim trailing whitespace from each line */ -}}
{{- $code = replaceRE "(?m)[ \t]+$" "" $code -}}
{{ if $indent }}   {{ end }}```mzsql
{{ $code | safeHTML }}
{{ if $indent }}   {{ end }}```
{{- end -}}
{{- if .results }}

{{ if $indent }}   {{ end }}{{ .results -}}
{{- end -}}
{{- end -}}
{{- end -}}
