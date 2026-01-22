{{- /* List/section template for Claude skill output */ -}}
{{- $excludedSections := .Site.Params.excludeFromSkill | default slice -}}
{{- if not (in $excludedSections .Section) -}}
{{- if not (.Params.disable_h1) -}}
# {{ .Title }}
{{- end -}}

{{ .RenderShortcodes }}

{{- if not (.Params.disable_list) -}}
{{- range .Pages.ByWeight -}}
{{- if not (in $excludedSections .Section) -}}
- [{{ .Title }}]({{ .RelPermalink }})
{{- end -}}
{{- end -}}
{{- end -}}
{{- end -}}
