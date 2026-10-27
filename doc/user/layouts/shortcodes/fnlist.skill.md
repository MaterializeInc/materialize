{{- /* Skill output: converts data/sql_funcs.yml into markdown list */ -}}
{{- $parentPath := partial "relative-link.html" . -}}
{{- $releasedVersions := dict -}}
{{- range (where $.Site.RegularPages "Section" "releases") -}}
  {{- $releasedVersions = merge $releasedVersions (dict .File.ContentBaseName .) -}}
{{- end -}}

{{- range $.Site.Data.sql_funcs -}}

{{- if not (isset $.Params 0) -}}

### {{ .type }} functions

{{- if .description -}}
{{ .description | $.Page.RenderString }}
{{- end -}}

{{- end -}}

{{- if or (eq ($.Get 0) .type) (not (isset $.Params 0)) -}}

{{- range .functions -}}
#### `{{ .signature }}`

{{ .description | $.Page.RenderString }}{{ if .url }} [(docs)]({{ $parentPath }}{{ .url | relURL }}){{ end }}{{ if .unmaterializable }}

**Note:** This function is [unmaterializable](#unmaterializable-functions).{{ end }}{{ if .unmaterializable_unless_temporal_filter }}

**Note:** This function is [unmaterializable](#unmaterializable-functions), but can be used in limited contexts in materialized views as a [temporal filter]({{ $parentPath }}{{ "/transform-data/patterns/temporal-filters/" | relURL }}).{{ end }}{{ if .known_time_zone_limitation_cast }}

**Known limitation:** You must explicitly cast the type for the time zone.{{ end }}{{ if .side_effecting }}

**Note:** This function is [side-effecting](#side-effecting-functions).{{ end }}{{ $versionAdded := index . "version-added" }}{{ if $versionAdded }}{{ $releasePage := index $releasedVersions $versionAdded }}{{ if not $releasePage.Params.released }}

**Unreleased:** This function will be released in [**{{ $versionAdded }}**]({{ printf "%s/releases#release-notes" $parentPath | relURL }}). It may not be available in your region yet. The release is scheduled to complete by **{{ dateFormat "January 2, 2006" $releasePage.Params.date }}**.{{ end }}{{ end }}

{{- end -}}

{{- end -}}

{{- end -}}
