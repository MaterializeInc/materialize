{{- /* Single page template for Claude skill output */ -}}
# {{ .Title }}
{{ if .Description }}
{{ .Description }}
{{ end }}

{{ .RawContent }}
