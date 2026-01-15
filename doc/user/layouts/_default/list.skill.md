{{- /* List/section template for Claude skill output */ -}}
{{- $excludedSections := .Site.Params.excludeFromSkill | default slice -}}
{{- if not (in $excludedSections .Section) -}}
# {{ .Title }}
{{ if .Description }}
{{ .Description }}
{{ end }}

{{ .RenderShortcodes }}

{{ range .Pages }}
{{- if not (in $excludedSections .Section) }}
---

## {{ .Title }}

{{ .RenderShortcodes }}
{{ end -}}
{{ end }}
{{- end -}}
