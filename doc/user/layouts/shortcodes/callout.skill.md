{{- /* Skill output: render callout as markdown blockquote */ -}}
> {{ .Inner | replaceRE "^\\s+" "" | replaceRE "\\s+$" "" | replaceRE "\\n\\s*\\n+" "\n" | replaceRE "\\n" "\n> " | replaceRE "\\n> \\s*$" "" }}
{{ if and ($.Params) (isset $.Params "primary_text") }}
>
> **{{ .Get "primary_text" }}**: {{ .Get "primary_url" }}
{{ end }}
{{ if and ($.Params) (isset $.Params "secondary_text") }}
> **{{ .Get "secondary_text" }}**: {{ .Get "secondary_url" }}
{{ end }}
