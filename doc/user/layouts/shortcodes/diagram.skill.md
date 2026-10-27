{{- /* Skill output: diagrams are visual, describe or omit */ -}}
{{- $diagramName := .Get 0 | replaceRE "\\.svg$" "" -}}
_See syntax diagram: {{ $diagramName }}_
