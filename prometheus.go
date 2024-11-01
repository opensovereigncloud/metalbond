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
	}, []string{"local", "peer"})

	metricTxChanMaxDepth = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "metalbond_tx_chan_max_depth",
		Help: "Maximum depth of txChan observed per peer",
	}, []string{"local", "peer"})

	metricRxChanHelloMaxDepth = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "metalbond_rx_chan_hello_max_depth",
		Help: "Current depth of rxChan for event HELLO per peer",
	}, []string{"local", "peer"})

	metricRxChanKeepaliveMaxDepth = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "metalbond_rx_chan_keepalive_max_depth",
		Help: "Current depth of rxChan for event Keepalive per peer",
	}, []string{"local", "peer"})

	metricRxChanSubscribeMaxDepth = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "metalbond_rx_chan_subscribe_max_depth",
		Help: "Current depth of rxChan for event Subscribe per peer",
	}, []string{"local", "peer"})

	metricRxChanUnsubscribeMaxDepth = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "metalbond_rx_chan_unsubscribe_max_depth",
		Help: "Current depth of rxChan for event Unsubscribe per peer",
	}, []string{"local", "peer"})

	metricRxChanUpdateMaxDepth = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "metalbond_rx_chan_update_max_depth",
		Help: "Current depth of rxChan for event Update per peer",
	}, []string{"local", "peer"})

	// Metric for tracking subscriptions per peer
	metricSubscriptionCount = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "metalbond_subscription_count",
		Help: "Current number of active subscriptions per peer",
	}, []string{"local", "peer"})
)

// RegisterMetrics initializes Prometheus metrics only once
func RegisterMetrics(registerer prometheus.Registerer) {
	registerMetricsOnce.Do(func() {
		registerer.MustRegister(
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
