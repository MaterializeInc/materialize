{{- /* List/section template for Claude skill output */ -}}
{{- $excludedSections := .Site.Params.excludeFromSkill | default slice -}}
{{- if not (in $excludedSections .Section) -}}
# {{ .Title }}
{{ if .Description }}
{{ .Description }}
{{ end }}

{{ .RenderShortcodes }}

{{- /*
  List the pages below this one rather than embedding their content. Each of
  them is published as its own Markdown file, and embedding them made section
  pages such as sql/index.md several hundred KB, too long for an agent to read.
*/ -}}
{{- $children := slice -}}
{{- range .Pages -}}
  {{- if and .RelPermalink (not (in $excludedSections .Section)) -}}
    {{- $children = $children | append . -}}
  {{- end -}}
{{- end -}}
{{- with $children }}

## Pages in this section

{{ range . }}- [{{ .Title }}]({{ partial "markdown-link-url.html" (dict "page" .) }}){{ with .Description }}{{ $d := trim (replaceRE `\s+` " " .) " " }}{{ if $d }}: {{ $d }}{{ end }}{{ end }}
{{ end }}
{{- end }}
{{- end -}}
