{{/* Chart name. */}}
{{- define "splitter.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/* Fully qualified app name (used as the StatefulSet name and pod name prefix). */}}
{{- define "splitter.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name (include "splitter.name" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{/* Headless service name — provides stable per-pod DNS for Raft. */}}
{{- define "splitter.headlessName" -}}
{{- printf "%s-headless" (include "splitter.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "splitter.labels" -}}
app.kubernetes.io/name: {{ include "splitter.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" }}
{{- end -}}

{{- define "splitter.selectorLabels" -}}
app.kubernetes.io/name: {{ include "splitter.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{/*
Comma-separated Raft join-peer list. Points at the INTERNAL gRPC port (Notify is a gRPC
call), not the raft transport port. Format: <pod>.<headless>:<internalPort>.
*/}}
{{- define "splitter.joinPeers" -}}
{{- $peers := list -}}
{{- $full := include "splitter.fullname" . -}}
{{- $headless := include "splitter.headlessName" . -}}
{{- $port := int .Values.ports.internal -}}
{{- range $i := until (int .Values.replicas) -}}
{{- $peers = append $peers (printf "%s-%d.%s:%d" $full $i $headless $port) -}}
{{- end -}}
{{- join "," $peers -}}
{{- end -}}
