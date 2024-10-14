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
{{- define "materialize-operator.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "materialize-operator.fullname" -}}
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
{{- define "materialize-operator.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "materialize-operator.labels" -}}
helm.sh/chart: {{ include "materialize-operator.chart" . }}
{{ include "materialize-operator.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "materialize-operator.selectorLabels" -}}
app.kubernetes.io/name: {{ include "materialize-operator.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "materialize-operator.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "materialize-operator.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Create the name of the cluster role to use
*/}}
{{- define "materialize-operator.clusterRoleName" -}}
{{- if .Values.rbac.clusterRole.create }}
{{- default (include "materialize-operator.fullname" .) .Values.rbac.clusterRole.name }}
{{- else }}
{{- default "default" .Values.rbac.clusterRole.name }}
{{- end }}
{{- end }}

{{/*
Create the name of the cluster role binding to use
*/}}
{{- define "materialize-operator.clusterRoleBindingName" -}}
{{- if .Values.rbac.clusterRoleBinding.create }}
{{- default (include "materialize-operator.fullname" .) .Values.rbac.clusterRoleBinding.name }}
{{- else }}
{{- default "default" .Values.rbac.clusterRoleBinding.name }}
{{- end }}
{{- end }}

{{/*
Create the CockroachDB connection string
*/}}
{{- define "materialize-operator.cockroachdbConnectionString" -}}
postgresql://{{ .Values.cockroachdb.username }}:{{ .Values.cockroachdb.password }}@{{ .Values.cockroachdb.endpoint }}:{{ .Values.cockroachdb.port }}/defaultdb?sslmode=verify-full&sslrootcert=/cockroach/ca.crt
{{- end }}
