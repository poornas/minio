// Copyright (c) 2015-2024 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package cmd

import (
	"net/http"
	"sync"

	"github.com/minio/minio/internal/logger"
	"github.com/minio/minio/internal/mcontext"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/common/expfmt"
)

const (
	poolIndex                                    = "poolIndex"
	apiMetricNamespace           MetricNamespace = "minio_api"
	apiObjectMetricsNamespace    MetricNamespace = "minio_obj"
	processMetricsNamespace      MetricNamespace = "minio_process"
	replicationV3MetricNamespace MetricNamespace = "minio_replication"
)

type clusterMetricsOpts struct {
	IsClusterMetrics     bool
	IsConfigMetrics      bool
	IsAuditMetrics       bool
	IsHealMetrics        bool
	IsESETMetrics        bool
	IsReplicationMetrics bool
	IsILMMetrics         bool
	IsWebhookMetrics     bool
	IsIAMMetrics         bool
	IsScannerMetrics     bool
	IsNotifyMetrics      bool
}

func (c clusterMetricsOpts) Empty() bool {
	return c == clusterMetricsOpts{}
}

var (
	// processCollector *minioProcessCollector
	//
	//	processMetricsGroup   []*MetricsGroup
	nodeReplicationMetricsGroupV3 []*MetricsGroup
	clusterReplV3MetricsGroup     []*MetricsGroup
	clusterV3MetricsGroup         []*MetricsGroup
	clusterV3ConfigMetricsGroup   []*MetricsGroup
	clusterV3AuditMetricsGroup    []*MetricsGroup
	clusterV3HealMetricsGroup     []*MetricsGroup
	clusterV3ESETMetricsGroup     []*MetricsGroup
	clusterV3IAMMetricsGroup      []*MetricsGroup
	clusterV3WebhookMetricsGroup  []*MetricsGroup
	clusterV3ILMMetricsGroup      []*MetricsGroup
	clusterV3ScannerMetricsGroup  []*MetricsGroup
	clusterV3NotifyMetricsGroup   []*MetricsGroup
	nodeReplCollectorV3           *minioNodeReplCollectorV3
	clusterCollectorV3            *minioClusterCollectorV3
	clusterReplCollectorV3        *minioClusterCollectorV3
	clusterConfigCollectorV3      *minioClusterCollectorV3
	clusterAuditCollectorV3       *minioClusterCollectorV3
	clusterHealCollectorV3        *minioClusterCollectorV3
	clusterESETCollectorV3        *minioClusterCollectorV3
	clusterIAMCollectorV3         *minioClusterCollectorV3
	clusterILMCollectorV3         *minioClusterCollectorV3
	clusterWebhookCollectorV3     *minioClusterCollectorV3
	clusterScannerCollectorV3     *minioClusterCollectorV3
	clusterNotifyCollectorV3      *minioClusterCollectorV3
)

func init() {
	nodeReplicationMetricsGroupV3 = []*MetricsGroup{
		getReplicationNodeMetrics(MetricsGroupOpts{dependGlobalObjectAPI: true, dependBucketTargetSys: true}, replicationV3MetricNamespace),
	}
	clusterReplV3MetricsGroup = []*MetricsGroup{
		getReplicationSiteMetrics(MetricsGroupOpts{dependGlobalSiteReplicationSys: true}, minioMetricNamespace),
	}
	clusterV3MetricsGroup = []*MetricsGroup{
		getClusterStorageMetrics(MetricsGroupOpts{dependGlobalObjectAPI: true}),
		getClusterHealthMetrics(MetricsGroupOpts{dependGlobalObjectAPI: true, v3Opts: clusterMetricsOpts{IsClusterMetrics: true}}, clusterMetricNamespace, healthSubsys),
	}
	clusterV3ConfigMetricsGroup = []*MetricsGroup{
		//TODO: need to collect config metrics
		getClusterHealthMetrics(MetricsGroupOpts{dependGlobalObjectAPI: true, v3Opts: clusterMetricsOpts{IsConfigMetrics: true}}, minioMetricNamespace, "cfg"),
	}
	clusterV3AuditMetricsGroup = []*MetricsGroup{
		getNotificationMetrics(MetricsGroupOpts{dependGlobalNotificationSys: true, v3Opts: clusterMetricsOpts{IsAuditMetrics: true}}),
	}
	clusterV3NotifyMetricsGroup = []*MetricsGroup{
		getNotificationMetrics(MetricsGroupOpts{dependGlobalNotificationSys: true, v3Opts: clusterMetricsOpts{IsNotifyMetrics: true}}),
	}
	clusterV3WebhookMetricsGroup = []*MetricsGroup{
		getWebhookMetrics(),
	}

	clusterV3HealMetricsGroup = []*MetricsGroup{
		getMinioHealingMetrics(MetricsGroupOpts{dependGlobalBackgroundHealState: true}),
	}
	clusterV3ESETMetricsGroup = []*MetricsGroup{
		getClusterHealthMetrics(MetricsGroupOpts{dependGlobalObjectAPI: true, v3Opts: clusterMetricsOpts{IsESETMetrics: true}}, minioMetricNamespace, ""),
	}
	clusterV3IAMMetricsGroup = []*MetricsGroup{
		getIAMNodeMetrics(MetricsGroupOpts{dependGlobalAuthNPlugin: true, dependGlobalIAMSys: true}, minioMetricNamespace),
	}
	clusterV3ILMMetricsGroup = []*MetricsGroup{
		getTierMetrics(),
		getILMNodeMetrics(minioMetricNamespace),
	}
	clusterV3ScannerMetricsGroup = []*MetricsGroup{
		getScannerNodeMetrics(minioMetricNamespace),
	}
	nodeReplCollectorV3 = newMinioReplCollectorNodeV3(nodeReplicationMetricsGroupV3)
	clusterCollectorV3 = newMinioClusterCollectorV3(clusterV3MetricsGroup, clusterMetricNameCluster, prometheus.NewDesc("minio_stats", "Statistics exposed by MinIO server per cluster", nil, nil))
	clusterReplCollectorV3 = newMinioClusterCollectorV3(clusterReplV3MetricsGroup, clusterMetricNameReplication, prometheus.NewDesc("minio_replication_stats", "Replication statistics exposed by MinIO server per cluster", nil, nil))
	clusterConfigCollectorV3 = newMinioClusterCollectorV3(clusterV3ConfigMetricsGroup, clusterMetricNameConfig, prometheus.NewDesc("minio_config_stats", "Config statistics exposed by MinIO server per cluster", nil, nil))
	clusterAuditCollectorV3 = newMinioClusterCollectorV3(clusterV3AuditMetricsGroup, clusterMetricNameAudit, prometheus.NewDesc("minio_audit_stats", "Audit statistics exposed by MinIO server per cluster", nil, nil))
	clusterHealCollectorV3 = newMinioClusterCollectorV3(clusterV3HealMetricsGroup, clusterMetricNameHeal, prometheus.NewDesc("minio_heal_stats", "Heal statistics exposed by MinIO server per cluster", nil, nil))
	clusterESETCollectorV3 = newMinioClusterCollectorV3(clusterV3ESETMetricsGroup, clusterMetricNameESET, prometheus.NewDesc("minio_eset_stats", "ESET statistics exposed by MinIO server per cluster", nil, nil))
	clusterIAMCollectorV3 = newMinioClusterCollectorV3(clusterV3IAMMetricsGroup, clusterMetricNameIAM, prometheus.NewDesc("minio_iam_stats", "IAM statistics exposed by MinIO server per cluster", nil, nil))
	clusterILMCollectorV3 = newMinioClusterCollectorV3(clusterV3ILMMetricsGroup, clusterMetricNameILM, prometheus.NewDesc("minio_ilm_stats", "ILM statistics exposed by MinIO server per cluster", nil, nil))
	clusterWebhookCollectorV3 = newMinioClusterCollectorV3(clusterV3WebhookMetricsGroup, clusterMetricNameWebhook, prometheus.NewDesc("minio_webhook_stats", "Webhook statistics exposed by MinIO server per cluster", nil, nil))
	clusterScannerCollectorV3 = newMinioClusterCollectorV3(clusterV3ScannerMetricsGroup, clusterMetricNameScanner, prometheus.NewDesc("minio_scanner_stats", "Scanner statistics exposed by MinIO server per cluster", nil, nil))
	clusterNotifyCollectorV3 = newMinioClusterCollectorV3(clusterV3NotifyMetricsGroup, clusterMetricNameNotify, prometheus.NewDesc("minio_notify_stats", "Notify statistics exposed by MinIO server per cluster", nil, nil))
}

// metricsV3DebugHandler is the prometheus handler for debug metrics
func metricsV3DebugHandler() http.Handler {
	registry := prometheus.NewRegistry()

	if err := registry.Register(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{
		Namespace:    minioNamespace,
		ReportErrors: true,
	})); err != nil {
		logger.CriticalIf(GlobalContext, err)
	}
	if err := registry.Register(collectors.NewGoCollector()); err != nil {
		logger.CriticalIf(GlobalContext, err)
	}
	gatherers := prometheus.Gatherers{
		registry,
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tc, ok := r.Context().Value(mcontext.ContextTraceKey).(*mcontext.TraceCtxt)
		if ok {
			tc.FuncName = "handler.MetricsV3Debug"
			tc.ResponseRecorder.LogErrBody = true
		}

		mfs, err := gatherers.Gather()
		if err != nil {
			if len(mfs) == 0 {
				writeErrorResponseJSON(r.Context(), w, toAdminAPIErr(r.Context(), err), r.URL)
				return
			}
		}

		contentType := expfmt.Negotiate(r.Header)
		w.Header().Set("Content-Type", string(contentType))

		enc := expfmt.NewEncoder(w, contentType)
		for _, mf := range mfs {
			if err := enc.Encode(mf); err != nil {
				logger.LogIf(r.Context(), err)
				return
			}
		}
		if closer, ok := enc.(expfmt.Closer); ok {
			closer.Close()
		}
	})
}

func metricsNodeV3Handler() http.Handler {
	registry := prometheus.NewRegistry()

	logger.CriticalIf(GlobalContext, registry.Register(nodeReplCollectorV3))

	gatherers := prometheus.Gatherers{
		registry,
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tc, ok := r.Context().Value(mcontext.ContextTraceKey).(*mcontext.TraceCtxt)
		if ok {
			tc.FuncName = "handler.MetricsNode"
			tc.ResponseRecorder.LogErrBody = true
		}

		mfs, err := gatherers.Gather()
		if err != nil {
			if len(mfs) == 0 {
				writeErrorResponseJSON(r.Context(), w, toAdminAPIErr(r.Context(), err), r.URL)
				return
			}
		}

		contentType := expfmt.Negotiate(r.Header)
		w.Header().Set("Content-Type", string(contentType))

		enc := expfmt.NewEncoder(w, contentType)
		for _, mf := range mfs {
			if err := enc.Encode(mf); err != nil {
				logger.LogIf(r.Context(), err)
				return
			}
		}
		if closer, ok := enc.(expfmt.Closer); ok {
			closer.Close()
		}
	})
}

// minioNodeReplCollectorV3 is the Custom Collector
type minioNodeReplCollectorV3 struct {
	metricsGroups []*MetricsGroup
	desc          *prometheus.Desc
}

// Describe sends the super-set of all possible descriptors of metrics
func (c *minioNodeReplCollectorV3) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.desc
}

// Collect is called by the Prometheus registry when collecting metrics.
func (c *minioNodeReplCollectorV3) Collect(ch chan<- prometheus.Metric) {
	// Expose MinIO's version information
	minioVersionInfo.WithLabelValues(Version, CommitID).Set(1.0)

	populateAndPublish(c.metricsGroups, func(metric Metric) bool {
		labels, values := getOrderedLabelValueArrays(metric.VariableLabels)
		values = append(values, globalLocalNodeName)
		labels = append(labels, serverName)

		if metric.Description.Type == histogramMetric {
			if metric.Histogram == nil {
				return true
			}
			for k, v := range metric.Histogram {
				labels = append(labels, metric.HistogramBucketLabel)
				values = append(values, k)
				ch <- prometheus.MustNewConstMetric(
					prometheus.NewDesc(
						prometheus.BuildFQName(string(metric.Description.Namespace),
							string(metric.Description.Subsystem),
							string(metric.Description.Name)),
						metric.Description.Help,
						labels,
						metric.StaticLabels,
					),
					prometheus.GaugeValue,
					float64(v),
					values...)
			}
			return true
		}

		metricType := prometheus.GaugeValue
		if metric.Description.Type == counterMetric {
			metricType = prometheus.CounterValue
		}
		ch <- prometheus.MustNewConstMetric(
			prometheus.NewDesc(
				prometheus.BuildFQName(string(metric.Description.Namespace),
					string(metric.Description.Subsystem),
					string(metric.Description.Name)),
				metric.Description.Help,
				labels,
				metric.StaticLabels,
			),
			metricType,
			metric.Value,
			values...)
		return true
	})
}

// newMinioReplCollectorNodeV3 describes the collector
// and returns reference of minioCollector for version 3
// It creates the Prometheus Description which is used
// to define Metric and  help string
func newMinioReplCollectorNodeV3(metricsGroups []*MetricsGroup) *minioNodeReplCollectorV3 {
	return &minioNodeReplCollectorV3{
		metricsGroups: metricsGroups,
		desc:          prometheus.NewDesc("minio_stats", "Statistics exposed by MinIO server per node", nil, nil),
	}
}

type minioClusterCollectorV3 struct {
	metricName    clusterMetricName
	metricsGroups []*MetricsGroup
	desc          *prometheus.Desc
}

func newMinioClusterCollectorV3(metricsGroups []*MetricsGroup, name clusterMetricName, desc *prometheus.Desc) *minioClusterCollectorV3 {
	return &minioClusterCollectorV3{
		metricName:    name,
		metricsGroups: metricsGroups,
		desc:          desc,
	}
}

type clusterMetricName int

const (
	clusterMetricNameCluster clusterMetricName = iota + 1
	clusterMetricNameReplication
	clusterMetricNameAudit
	clusterMetricNameHeal
	clusterMetricNameESET
	clusterMetricNameIAM
	clusterMetricNameILM
	clusterMetricNameWebhook
	clusterMetricNameScanner
	clusterMetricNameNotify
	clusterMetricNameConfig
)

func (c *minioClusterCollectorV3) getOpts() clusterMetricsOpts {
	opts := clusterMetricsOpts{}
	switch c.metricName {
	case clusterMetricNameCluster:
		opts.IsClusterMetrics = true
	case clusterMetricNameReplication:
		opts.IsReplicationMetrics = true
	case clusterMetricNameAudit:
		opts.IsAuditMetrics = true
	case clusterMetricNameHeal:
		opts.IsHealMetrics = true
	case clusterMetricNameESET:
		opts.IsESETMetrics = true
	case clusterMetricNameIAM:
		opts.IsIAMMetrics = true
	case clusterMetricNameILM:
		opts.IsILMMetrics = true
	case clusterMetricNameWebhook:
		opts.IsWebhookMetrics = true
	case clusterMetricNameScanner:
		opts.IsScannerMetrics = true
	case clusterMetricNameNotify:
		opts.IsNotifyMetrics = true
	}
	return opts
}

// Describe sends the super-set of all possible descriptors of metrics
func (c *minioClusterCollectorV3) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.desc
}

// Collect is called by the Prometheus registry when collecting metrics.
func (c *minioClusterCollectorV3) Collect(out chan<- prometheus.Metric) {
	var wg sync.WaitGroup
	publish := func(in <-chan Metric) {
		defer wg.Done()
		for metric := range in {
			labels, values := getOrderedLabelValueArrays(metric.VariableLabels)
			collectMetric(metric, labels, values, "cluster", out)
		}
	}
	opts := c.getOpts()
	// Call peer api to fetch metrics
	wg.Add(1)
	go publish(ReportMetrics(GlobalContext, c.metricsGroups))
	if !opts.IsESETMetrics {
		wg.Add(1)
		go publish(globalNotificationSys.GetClusterMetrics(GlobalContext, opts))
	}
	wg.Wait()
}

func getMetricsGroups(opts clusterMetricsOpts) []*MetricsGroup {
	var metricsGroups []*MetricsGroup
	if opts.IsClusterMetrics {
		metricsGroups = append(metricsGroups, clusterV3MetricsGroup...)
	}
	if opts.IsReplicationMetrics {
		metricsGroups = append(metricsGroups, clusterReplV3MetricsGroup...)
	}

	if opts.IsAuditMetrics {
		metricsGroups = append(metricsGroups, clusterV3AuditMetricsGroup...)
	}
	if opts.IsHealMetrics {
		metricsGroups = append(metricsGroups, clusterV3HealMetricsGroup...)
	}
	if opts.IsESETMetrics {
		metricsGroups = append(metricsGroups, clusterV3ESETMetricsGroup...)
	}
	if opts.IsIAMMetrics {
		metricsGroups = append(metricsGroups, clusterV3IAMMetricsGroup...)
	}
	if opts.IsILMMetrics {
		metricsGroups = append(metricsGroups, clusterV3ILMMetricsGroup...)
	}
	if opts.IsWebhookMetrics {
		metricsGroups = append(metricsGroups, clusterV3WebhookMetricsGroup...)
	}
	if opts.IsScannerMetrics {
		metricsGroups = append(metricsGroups, clusterV3ScannerMetricsGroup...)
	}
	if opts.IsNotifyMetrics {
		metricsGroups = append(metricsGroups, clusterV3NotifyMetricsGroup...)
	}
	if opts.Empty() { //v2 metrics
		metricsGroups = append(metricsGroups, peerMetricsGroups...)
	}

	return metricsGroups

}
func metricsClusterV3Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		registry := prometheus.NewRegistry()
		var c prometheus.Collector
		switch r.URL.Path {
		case minioReservedBucketPath + prometheusMetricsV3ClusterPath:
			c = clusterCollectorV3
		case minioReservedBucketPath + prometheusMetricsV3ClusterReplicationPath:
			c = clusterReplCollectorV3
		case minioReservedBucketPath + prometheusMetricsV3ClusterConfigPath:
			c = clusterConfigCollectorV3
		case minioReservedBucketPath + prometheusMetricsV3ClusterAuditPath:
			c = clusterAuditCollectorV3
		case minioReservedBucketPath + prometheusMetricsV3ClusterILMPath:
			c = clusterILMCollectorV3
		case minioReservedBucketPath + prometheusMetricsV3ClusterIAMPath:
			c = clusterIAMCollectorV3
		case minioReservedBucketPath + prometheusMetricsV3ClusterESETPath:
			c = clusterESETCollectorV3
		case minioReservedBucketPath + prometheusMetricsV3ClusterWebhookPath:
			c = clusterWebhookCollectorV3
		case minioReservedBucketPath + prometheusMetricsV3ClusterScannerPath:
			c = clusterScannerCollectorV3
		case minioReservedBucketPath + prometheusMetricsV3ClusterHealingPath:
			c = clusterHealCollectorV3
		case minioReservedBucketPath + prometheusMetricsV3ClusterNotificationPath:
			c = clusterNotifyCollectorV3
		default:
			writeErrorResponse(r.Context(), w, toAdminAPIErr(r.Context(), AllAccessDisabled{}), r.URL)
			return
		}
		// Report all other metrics
		logger.CriticalIf(GlobalContext, registry.Register(c))
		// DefaultGatherers include golang metrics and process metrics.
		gatherers := prometheus.Gatherers{
			registry,
		}
		tc, ok := r.Context().Value(mcontext.ContextTraceKey).(*mcontext.TraceCtxt)
		if ok {
			tc.FuncName = "handler.MetricsCluster"
			tc.ResponseRecorder.LogErrBody = true
		}

		mfs, err := gatherers.Gather()
		if err != nil && len(mfs) == 0 {
			writeErrorResponseJSON(r.Context(), w, toAdminAPIErr(r.Context(), err), r.URL)
			return
		}

		contentType := expfmt.Negotiate(r.Header)
		w.Header().Set("Content-Type", string(contentType))

		enc := expfmt.NewEncoder(w, contentType)
		for _, mf := range mfs {
			if err := enc.Encode(mf); err != nil {
				// client may disconnect for any reasons
				// we do not have to log this.
				return
			}
		}
		if closer, ok := enc.(expfmt.Closer); ok {
			closer.Close()
		}
	})
}
