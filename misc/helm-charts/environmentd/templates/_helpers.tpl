{{- /*
Copyright Materialize, Inc. and contributors. All rights reserved.

Use of this software is governed by the Business Source License
included in the LICENSE file at the root of this repository.

As of the Change Date specified in that file, in accordance with
the Business Source License, use of this software will be governed
by the Apache License, Version 2.0.
*/ -}}

{{/*
Expand the name of the chart.
*/}}
{{- define "materialize-environmentd.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "materialize-environmentd.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "materialize-environmentd.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "materialize-environmentd.labels" -}}
helm.sh/chart: {{ include "materialize-environmentd.chart" . }}
{{ include "materialize-environmentd.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "materialize-environmentd.selectorLabels" -}}
app.kubernetes.io/name: {{ include "materialize-environmentd.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the namespace to use
*/}}
{{- define "materialize-environmentd.namespaceName" -}}
{{- default "materialize-environment" .Values.namespace.name }}
{{- end }}

{{/*
Create the secret name for an environment
*/}}
{{- define "materialize-environmentd.secretName" -}}
{{- printf "materialize-backend-%s" . }}
{{- end }}
