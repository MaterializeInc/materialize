{{- /* Skill output: include-from-yaml renders content from YAML data */ -}}
{{- $pathArray := split (lower (.Get "data")) "/" -}}
{{- $data := $.Site.Data -}}
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
{{- $content := index . $field -}}
{{- /* If the snippet contains nested shortcodes, render it (skill shortcode
       variants resolve, but output is HTML). Otherwise emit the raw markdown so
       skill output stays clean markdown. */ -}}
{{- if findRE "{{[<%]" $content -}}
{{- $content | $.Page.RenderString -}}
{{- else -}}
{{- $content -}}
{{- end -}}
{{- end -}}
{{- end -}}
