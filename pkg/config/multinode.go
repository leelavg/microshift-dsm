package config

type MultiNodeConfig struct {
	Enabled         bool   `json:"enabled"`
	ControlNodeName string `json:"controlNodeName,omitempty"`
}

// ConfigMultiNode populates multinode configurations to Config.MultiNode
func ConfigMultiNode(c *Config, enabled bool) *Config {
	if !enabled {
		return c
	}
	c.MultiNode.Enabled = enabled
	return c
}
