{{- /* Skill output: inline the included markdown content with shortcode processing */ -}}
{{- $path := .Get "file" -}}
{{- $content := readFile $path -}}
{{- $content | $.Page.RenderString -}}
