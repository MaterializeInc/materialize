{{- /* Skill output: yaml-list renders as markdown list */ -}}
{{- $column := .Get "column" -}}
{{- $label := .Get "label" -}}
{{- $pathArray := split (lower (.Get "data")) "/" -}}
{{- $data := $.Site.Data -}}
{{- range $pathArray -}}
  {{- $data = index $data . -}}
{{- end -}}
{{ if $label }}
{{- $filteredRows := slice -}}
{{- range $data.rows -}}
  {{- if in .labels $label -}}
    {{- $filteredRows = $filteredRows | append . -}}
  {{- end -}}
{{- end -}}
{{ range $filteredRows }}
- {{ index . "command" }}
{{ end -}}
{{ else }}
{{ range $data.rows -}}
- {{ index . $column }}
{{ end -}}
{{ end -}}
