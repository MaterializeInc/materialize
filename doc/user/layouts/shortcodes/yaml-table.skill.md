{{- /* Skill output: yaml-table renders as markdown table */ -}}
{{- $pathArray := split (lower (.Get "data")) "/" -}}
{{- $data := $.Site.Data -}}
{{- range $pathArray }}
  {{- $data = index $data . -}}
{{- end }}
{{ partial "yaml-tables/generic-table.skill.md" (dict "rows" $data.rows "columns" $data.columns) }}
