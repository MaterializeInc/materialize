{{- /* List/section template for Claude skill output */ -}}
{{- $excludedSections := .Site.Params.excludeFromSkill | default slice -}}
{{- if not (in $excludedSections .Section) -}}
# {{ .Title }}
{{ if .Description }}
{{ .Description }}
{{ end }}

{{ .Content }}

{{ range .Pages }}
{{- if not (in $excludedSections .Section) }}
---

## {{ .Title }}

{{ .Content }}
{{ end -}}
{{ end }}
{{- end -}}
