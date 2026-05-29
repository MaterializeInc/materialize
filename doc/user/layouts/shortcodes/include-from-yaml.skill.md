{{- /* Skill output: include-from-yaml renders content from YAML data */ -}}
{{- $pathArray := split (lower (.Get "data")) "/" -}}
{{- $data := hugo.Data -}}
{{- range $pathArray }}
  {{- $data = index $data . -}}
{{- end }}
{{- $name := .Get "name" -}}
{{- $field := .Get "field" | default "content" -}}

{{- $rows := $data -}}
{{- if reflect.IsMap $data -}}
  {{- $rows = $data.rows -}}
{{- end -}}

{{- range $rows -}}
{{- if eq .name $name -}}
{{- index . $field | $.Page.RenderString -}}
{{- end -}}
{{- end -}}
