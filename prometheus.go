package metalbond

import (
	"github.com/prometheus/client_golang/prometheus"
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

	metricRxChanHelloDepth = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "metalbond_rx_chan_hello_depth",
		Help: "Current depth of rxChan for event HELLO per peer",
	}, []string{"peer"})

	metricRxChanKeepaliveDepth = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "metalbond_rx_chan_keepalive_depth",
		Help: "Current depth of rxChan for event Keepalive per peer",
	}, []string{"peer"})

	metricRxChanSubscribeDepth = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "metalbond_rx_chan_subscribe_depth",
		Help: "Current depth of rxChan for event Subscribe per peer",
	}, []string{"peer"})

	metricRxChanUnsubscribeDepth = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "metalbond_rx_chan_unsubscribe_depth",
		Help: "Current depth of rxChan for event Unsubscribe per peer",
	}, []string{"peer"})

	metricRxChanUpdateDepth = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "metalbond_rx_chan_update_depth",
		Help: "Current depth of rxChan for event Update per peer",
	}, []string{"peer"})

	// Metric for tracking subscriptions per peer
	metricSubscriptionCount = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "metalbond_subscription_count",
		Help: "Current number of active subscriptions per peer",
	}, []string{"peer"})

	// Metric for tracking current routes per peer
	metricRouteCount = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "metalbond_route_count",
		Help: "Current number of active routes per peer",
	}, []string{"peer"})
)

// RegisterMetrics initializes Prometheus metrics only once
func RegisterMetrics() {
	registerMetricsOnce.Do(func() {
		prometheus.MustRegister(
			metricTxChanDepth,
			metricTxChanMaxDepth,
			metricRxChanHelloDepth,
			metricRxChanKeepaliveDepth,
			metricRxChanSubscribeDepth,
			metricRxChanUnsubscribeDepth,
			metricRxChanUpdateDepth,
			metricSubscriptionCount,
			metricRouteCount,
		)
	})
}
