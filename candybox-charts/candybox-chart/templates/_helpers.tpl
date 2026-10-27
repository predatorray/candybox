{{- define "candybox.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 40 | trimSuffix "-" -}}
{{- end -}}
{{- define "candybox.fullname" -}}
{{- default (printf "%s-%s" .Release.Name (include "candybox.name" .)) .Values.fullnameOverride | trunc 40 | trimSuffix "-" -}}
{{- end -}}
{{- define "candybox.labels" -}}
app.kubernetes.io/name: {{ include "candybox.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | quote }}
{{- end -}}
{{- define "candybox.selector" -}}
app.kubernetes.io/name: {{ include "candybox.name" .root }}
app.kubernetes.io/instance: {{ .root.Release.Name }}
app.kubernetes.io/component: {{ .component }}
{{- end -}}
{{- define "candybox.image" -}}
{{- if .Values.image.digest -}}
{{ printf "%s@%s" .Values.image.repository .Values.image.digest }}
{{- else -}}
{{ printf "%s:%s" .Values.image.repository (default .Chart.AppVersion .Values.image.tag) }}
{{- end -}}
{{- end -}}
{{- define "candybox.zk" -}}
{{- if .Values.zookeeper.enabled -}}
{{- $zk := .Subcharts.zookeeper -}}
{{ printf "%s.%s.svc:%v" (include "zookeeper.fullname" $zk) (include "cloudpirates.namespace" $zk) $zk.Values.service.ports.client }}
{{- else -}}
{{ required "externalZookeeper.servers is required when zookeeper.enabled=false" .Values.externalZookeeper.servers }}
{{- end -}}
{{- end -}}
{{- define "candybox.bkZk" -}}
{{ default (include "candybox.zk" .) .Values.bookkeeper.zookeeperServers }}
{{- end -}}
{{- define "candybox.metadata" -}}
{{- if .Values.bookkeeper.enabled -}}
{{ printf "zk://%s%s" (include "candybox.bkZk" .) .Values.bookkeeper.ledgerRootPath }}
{{- else -}}
{{ required "externalBookkeeper.metadataServiceUri is required when bookkeeper.enabled=false" .Values.externalBookkeeper.metadataServiceUri }}
{{- end -}}
{{- end -}}
{{- define "candybox.scheduling" -}}
{{- with .nodeSelector }}
nodeSelector: {{ toYaml . | nindent 2 }}
{{- end }}
{{- with .tolerations }}
tolerations: {{ toYaml . | nindent 2 }}
{{- end }}
{{- with .affinity }}
affinity: {{ toYaml . | nindent 2 }}
{{- end }}
{{- with .topologySpreadConstraints }}
topologySpreadConstraints: {{ toYaml . | nindent 2 }}
{{- end }}
{{- end -}}
{{- define "candybox.bkEnv" -}}
- name: BK_zkServers
  value: {{ include "candybox.bkZk" . | quote }}
- name: BK_metadataServiceUri
  value: {{ include "candybox.metadata" . | quote }}
- name: BK_zkLedgersRootPath
  value: {{ .Values.bookkeeper.ledgerRootPath | quote }}
- name: BK_journalDirectory
  value: /data/journal
- name: BK_ledgerDirectories
  value: /data/ledgers
- name: BK_httpServerEnabled
  value: "true"
- name: BK_useHostNameAsBookieID
  value: "true"
- name: BOOKIE_LOG_DIR
  value: /tmp/logs
- name: BOOKIE_CONF
  value: /tmp/bk_server.conf
{{- range $key, $value := .Values.bookkeeper.config }}
- name: {{ printf "BK_%s" $key }}
  value: {{ $value | quote }}
{{- end }}
{{- with .Values.bookkeeper.env }}
{{ toYaml . }}
{{- end }}
{{- end -}}
