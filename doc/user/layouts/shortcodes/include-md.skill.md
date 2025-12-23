{{- /* Skill output: inline the included markdown content */ -}}
{{- $path := .Get "file" -}}
{{- readFile $path -}}
