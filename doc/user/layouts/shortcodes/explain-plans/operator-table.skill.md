{{- /* Skill output: operator-table renders as markdown table */ -}}
{{- $dataFile := .Get "data" -}}
{{- $planType := .Get "planType" -}}

{{- if not $dataFile -}}
  {{- errorf "operator-table shortcode requires a 'data' parameter" -}}
{{- end -}}
{{- if not $planType -}}
  {{- errorf "operator-table shortcode requires a 'planType' parameter" -}}
{{- end -}}

{{- $data := index $.Site.Data $dataFile -}}

{{- $filteredOperators := slice -}}
{{- range $data.operators -}}
  {{- if in .plan_types $planType -}}
    {{- $filteredOperators = $filteredOperators | append . -}}
  {{- end -}}
{{- end -}}

{{- if gt (len $filteredOperators) 0 -}}
The following table lists the operators that are available in the {{ $planType }} plan.

- For those operators that require memory to maintain intermediate state, **Uses memory** is marked with **Yes**.
- For those operators that expand the data size (either rows or columns), **Can increase data size** is marked with **Yes**.

{{- $rows := slice -}}
{{- range $filteredOperators -}}
  {{- $description := .description | markdownify -}}
  {{- $expansiveText := "No" -}}
  {{- if .expansive -}}
    {{- $expansiveText = .expansive_details | markdownify -}}
  {{- end -}}
  {{- $memoryText := "No" -}}
  {{- if .uses_memory -}}
    {{- $memoryText = printf "✅ %s" (.memory_details | markdownify) -}}
  {{- end -}}
  {{- $fullDescription := printf "%s\n\n**Can increase data size:** %s\n**Uses memory:** %s" $description $expansiveText $memoryText -}}
  {{- $row := dict "Operator" (printf "**%s**" .operator) "Description" $fullDescription "Example" (.example | markdownify) -}}
  {{- $rows = $rows | append $row -}}
{{- end -}}

{{- $columns := slice -}}
{{- $columns = $columns | append (dict "column" "Operator") -}}
{{- $columns = $columns | append (dict "column" "Description") -}}
{{- $columns = $columns | append (dict "column" "Example") -}}

{{- partial "yaml-tables/generic-table.skill.md" (dict "rows" $rows "columns" $columns) -}}

**Notes:**
- **Can increase data size:** Specifies whether the operator can increase the data size (can be the number of rows or the number of columns).
- **Uses memory:** Specifies whether the operator use memory to maintain state for its inputs.
{{- else -}}
*No operators found for plan type "{{ $planType }}".*
{{- end -}}
