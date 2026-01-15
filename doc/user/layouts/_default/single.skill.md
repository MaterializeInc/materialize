{{- /* Single page template for Claude skill output */ -}}
{{- $excludedSections := .Site.Params.excludeFromSkill | default slice -}}
{{- if not (in $excludedSections .Section) -}}
# {{ .Title }}
{{ if .Description }}
{{ .Description }}
{{ end }}

{{ .RenderShortcodes }}
{{- end -}}
