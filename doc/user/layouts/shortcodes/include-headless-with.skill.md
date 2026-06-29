{{- /* Skill output: inline the included markdown content, substituting named params */ -}}
{{- $file := .Get "file" -}}
{{- with $.Page.GetPage $file -}}
  {{- $out := .RenderShortcodes -}}
  {{- range $k, $v := $.Params -}}
    {{- if ne $k "file" -}}
      {{- $out = replace $out (printf "__%s__" (upper $k)) $v -}}
    {{- end -}}
  {{- end -}}
  {{- $out = replaceRE `{{__hugo_ctx[^}]*}}` "" $out -}}
  {{- $out = replace $out "{{__hugo_ctx/}}" "" -}}
  {{- $out = replaceRE "^\\s+" "" $out -}}
  {{- $out = replaceRE "\\s+$" "" $out -}}
  {{- $out -}}
{{- else -}}
  {{- errorf "The %q shortcode was unable to find %q. See %s" $.Name $file $.Position -}}
{{- end -}}
