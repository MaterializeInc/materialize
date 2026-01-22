{{- /* Skill output: include-from-yaml renders content from YAML data */ -}}
{{- $pathArray := split (lower (.Get "data")) "/" -}}
{{- $data := $.Site.Data -}}
{{- range $pathArray }}
  {{- $data = index $data . -}}
{{- end }}
{{- $name := .Get "name" -}}
{{- range $data -}}
{{- if eq .name $name -}}
{{- .content | $.Page.RenderString -}}
{{- end -}}
{{- end -}}
