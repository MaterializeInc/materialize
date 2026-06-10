{{- /* Skill output: yaml-table renders as markdown table with shortcode processing */ -}}
{{- $pathArray := split (lower (.Get "data")) "/" -}}
{{- $noHeader := .Get "noHeader" -}}
{{- $columnsParam := .Get "columns" -}}
{{- $data := $.Site.Data -}}
{{- range $pathArray }}
  {{- $data = index $data . -}}
{{- end }}

{{- $columns := $data.columns -}}
{{- if $columnsParam -}}
  {{- $wanted := split $columnsParam "," -}}
  {{- $filtered := slice -}}
  {{- range $wanted -}}
    {{- $name := strings.TrimSpace . -}}
    {{- range $data.columns -}}
      {{- if eq .column $name -}}
        {{- $filtered = $filtered | append . -}}
      {{- end -}}
    {{- end -}}
  {{- end -}}
  {{- $columns = $filtered -}}
{{- end -}}

{{- $fields := slice -}}
{{- $headers := slice -}}
{{- $separators := slice -}}
{{- range $columns -}}
  {{- $headers = $headers | append (.header | default .column) -}}
  {{- $fields = $fields | append (dict "field" .column) -}}
  {{- $separators = $separators | append "---" -}}
{{- end -}}
{{- if not $noHeader }}
| {{ delimit $headers " | " }} |
| {{ delimit $separators " | " }} |
{{- end }}
{{- range $data.rows }}
{{- $row := . -}}
{{- $cells := slice -}}
{{- range $fields -}}
  {{- $field := .field -}}
  {{- $value := index $row $field | default "" -}}
  {{- $rendered := $value | $.Page.RenderString -}}
  {{- $cells = $cells | append ($rendered | replaceRE "\\|" "\\|" | replaceRE "\n" " ") -}}
{{- end }}
| {{ delimit $cells " | " }} |
{{- end }}
