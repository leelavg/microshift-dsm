package config

type MultiNodeConfig struct {
	Enabled bool `json:"enabled"`
	// Comma-separated list of control plane node IPs
	Controlplane string `json:"controlplane"`
	// WorkerOnly indicates this node should only run kubelet without control plane components
	WorkerOnly bool `json:"workerOnly"`
	// VIP is the virtual IP for API server HA (auto-calculated if empty)
	VIP string `json:"vip"`
	// ServiceLBRange is the IP range for LoadBalancer services (auto-calculated if empty)
	ServiceLBRange string `json:"serviceLBRange"`
}

// ConfigMultiNode populates multinode configurations to Config.MultiNode
func ConfigMultiNode(c *Config, enabled bool) *Config {
	if !enabled {
		return c
	}
	c.MultiNode.Enabled = enabled
	c.MultiNode.Controlplane = c.Node.NodeIP
	return c
}
