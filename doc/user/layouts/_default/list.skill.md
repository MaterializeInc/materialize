{{- /* List/section template for Claude skill output */ -}}
{{- $excludedSections := .Site.Params.excludeFromSkill | default slice -}}
{{- if not (in $excludedSections .Section) -}}
# {{ .Title }}
{{ if .Description }}
{{ .Description }}
{{ end }}

{{ .RenderShortcodes }}

{{ with .Pages }}
## In this section
{{ range . }}
- [{{ .Title }}]({{ .RelPermalink }})
{{- end }}
{{ end }}
{{- end -}}
