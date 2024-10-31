package metalbond

import (
	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
	"sync"
)

// Ensure metrics are registered only once
var registerMetricsOnce sync.Once

// Define metrics globally
var (
	metricTxChanDepth = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "metalbond_tx_chan_depth",
		Help: "Current depth of txChan per peer",
	}, []string{"peer"})

	metricTxChanMaxDepth = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "metalbond_tx_chan_max_depth",
		Help: "Maximum depth of txChan observed per peer",
	}, []string{"peer"})

	metricRxChanHelloMaxDepth = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "metalbond_rx_chan_hello_max_depth",
		Help: "Current depth of rxChan for event HELLO per peer",
	}, []string{"peer"})

	metricRxChanKeepaliveMaxDepth = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "metalbond_rx_chan_keepalive_max_depth",
		Help: "Current depth of rxChan for event Keepalive per peer",
	}, []string{"peer"})

	metricRxChanSubscribeMaxDepth = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "metalbond_rx_chan_subscribe_max_depth",
		Help: "Current depth of rxChan for event Subscribe per peer",
	}, []string{"peer"})

	metricRxChanUnsubscribeMaxDepth = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "metalbond_rx_chan_unsubscribe_max_depth",
		Help: "Current depth of rxChan for event Unsubscribe per peer",
	}, []string{"peer"})

	metricRxChanUpdateMaxDepth = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "metalbond_rx_chan_update_max_depth",
		Help: "Current depth of rxChan for event Update per peer",
	}, []string{"peer"})

	// Metric for tracking subscriptions per peer
	metricSubscriptionCount = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "metalbond_subscription_count",
		Help: "Current number of active subscriptions per peer",
	}, []string{"peer"})
)

// RegisterMetrics initializes Prometheus metrics only once
func RegisterMetrics() {
	registerMetricsOnce.Do(func() {
		metrics.Registry.MustRegister(
			metricTxChanDepth,
			metricTxChanMaxDepth,
			metricRxChanHelloMaxDepth,
			metricRxChanKeepaliveMaxDepth,
			metricRxChanSubscribeMaxDepth,
			metricRxChanUnsubscribeMaxDepth,
			metricRxChanUpdateMaxDepth,
			metricSubscriptionCount,
		)
	})
}
