{{- /* Markdown table output for skill format */ -}}
{{- $fields := slice -}}
{{- $headers := slice -}}
{{- $separators := slice -}}
{{- range .columns -}}
  {{- $headers = $headers | append (.header | default .column) -}}
  {{- $fields = $fields | append (dict "field" .column) -}}
  {{- $separators = $separators | append "---" -}}
{{- end -}}
| {{ delimit $headers " | " }} |
| {{ delimit $separators " | " }} |
{{- range .rows }}
{{- $row := . -}}
{{- $cells := slice -}}
{{- range $fields -}}
  {{- $field := .field -}}
  {{- $value := index $row $field | default "" -}}
  {{- $cells = $cells | append ($value | replaceRE "\\|" "\\|" | replaceRE "\n" " ") -}}
{{- end }}
| {{ delimit $cells " | " }} |
{{- end }}
