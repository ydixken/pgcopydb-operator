{{/*
Chart name, truncated to the 63-char label limit.
*/}}
{{- define "pgcopydb-operator.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Fully qualified app name. Skips the duplicated chart name when the release
name already contains it.
*/}}
{{- define "pgcopydb-operator.fullname" -}}
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
Chart name and version for the helm.sh/chart label.
*/}}
{{- define "pgcopydb-operator.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels.
*/}}
{{- define "pgcopydb-operator.labels" -}}
helm.sh/chart: {{ include "pgcopydb-operator.chart" . }}
{{ include "pgcopydb-operator.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels: the immutable subset used by the Deployment selector.
*/}}
{{- define "pgcopydb-operator.selectorLabels" -}}
app.kubernetes.io/name: {{ include "pgcopydb-operator.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
ServiceAccount name: created name, explicit name, or "default".
*/}}
{{- define "pgcopydb-operator.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "pgcopydb-operator.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}
