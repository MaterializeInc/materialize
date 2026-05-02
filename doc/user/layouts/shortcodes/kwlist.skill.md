{{- /* Skill output: list SQL keywords from keywords.txt */ -}}
{{- $keywords := slice -}}
{{- range $line := split (readFile "sql-grammar/keywords.txt") "\n" -}}
  {{- if and (not (hasPrefix $line "#")) $line -}}
    {{- $keywords = $keywords | append ($line | upper) -}}
  {{- end -}}
{{- end -}}

{{- $columnCount := 4 -}}
{{- $totalKeywords := len $keywords -}}
{{- $numRows := div $totalKeywords $columnCount -}}
{{- if gt (mod $totalKeywords $columnCount) 0 -}}
  {{- $numRows = add $numRows 1 -}}
{{- end -}}

| | | | |
|--|--|--|--|
{{- range $row := seq $numRows -}}
{{- $rowIndex := sub $row 1 -}}
| {{- range $col := seq $columnCount -}}
  {{- $index := add (mul $rowIndex $columnCount) (sub $col 1) -}}
  {{- if and (ge $index 0) (lt $index $totalKeywords) -}}
`{{ index $keywords $index }}`
  {{- else -}}
&nbsp;
  {{- end -}}
  {{- if lt $col $columnCount }} |{{ end -}}
{{- end -}} |
{{- end -}}
