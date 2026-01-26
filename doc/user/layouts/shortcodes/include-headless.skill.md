{{- with .Get 0 -}}
  {{- with $.Page.GetPage . -}}
    {{- $out := .RenderShortcodes -}}
    {{- $out = replaceRE `{{__hugo_ctx[^}]*}}` "" $out -}}
    {{- $out = replace $out "{{__hugo_ctx/}}" "" -}}
    {{- $out = replaceRE "^\\s+" "" $out -}}
    {{- $out = replaceRE "\\s+$" "" $out -}}
    {{- $out -}}
  {{- else -}}
    {{- errorf "The %q shortcode was unable to find %q. See %s" $.Name . $.Position -}}
  {{- end -}}
{{- else -}}
  {{- errorf "The %q shortcode requires a positional parameter indicating the logical path of the file to include. See %s" .Name .Position -}}
{{- end -}}
