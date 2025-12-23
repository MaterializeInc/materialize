{{- /* List/section template for Claude skill output */ -}}
# {{ .Title }}
{{ if .Description }}
{{ .Description }}
{{ end }}

{{ .RawContent }}

{{ range .Pages }}
{{ if not (in (slice "releases" "self-managed") .Section) }}
---

## {{ .Title }}

{{ .RawContent }}
{{ end }}
{{ end }}
