package components

import (
	"context"
	"fmt"
	"net"
	"strings"

	"github.com/openshift/microshift/pkg/assets"
	"github.com/openshift/microshift/pkg/config"
	"github.com/openshift/microshift/pkg/util"
	"k8s.io/klog/v2"
)

// calculateVIP derives VIP address from node IP by setting last octet to 100
// Example: 10.89.0.11 -> 10.89.0.100
func calculateVIP(nodeIP string) string {
	ip := net.ParseIP(nodeIP)
	if ip == nil {
		klog.Warningf("Invalid node IP for VIP calculation: %s", nodeIP)
		return ""
	}

	ip4 := ip.To4()
	if ip4 == nil {
		klog.Warningf("IPv6 not supported for kube-vip ARP mode: %s", nodeIP)
		return ""
	}

	return fmt.Sprintf("%d.%d.%d.100", ip4[0], ip4[1], ip4[2])
}

// calculateServiceLBRange derives service LoadBalancer IP range from node IP
// Example: 10.89.0.11 -> 10.89.0.101-10.89.0.199
func calculateServiceLBRange(nodeIP string) string {
	ip := net.ParseIP(nodeIP)
	if ip == nil {
		klog.Warningf("Invalid node IP for service LB range calculation: %s", nodeIP)
		return ""
	}

	ip4 := ip.To4()
	if ip4 == nil {
		klog.Warningf("IPv6 not supported for kube-vip: %s", nodeIP)
		return ""
	}

	return fmt.Sprintf("%d.%d.%d.101-%d.%d.%d.199", ip4[0], ip4[1], ip4[2], ip4[0], ip4[1], ip4[2])
}

// shouldDeployKubeVipDaemonSet determines if kube-vip DaemonSet should be deployed
// Deploy VIP when enable-ha marker exists OR when there are multiple control planes
func shouldDeployKubeVipDaemonSet(cfg *config.Config) bool {
	// Skip on worker-only nodes
	if cfg.MultiNode.WorkerOnly {
		return false
	}

	// Only deploy in multinode mode
	if !cfg.MultiNode.Enabled {
		return false
	}

	// Check if enable-ha marker file exists (for single CP HA mode)
	markerFile := "/var/lib/microshift-data/.enable-ha"
	if exists, _ := util.PathExists(markerFile); exists {
		return true
	}

	// Check if this is a multi-CP setup (comma in Controlplane field)
	if strings.Contains(cfg.MultiNode.Controlplane, ",") {
		return true
	}

	// Single CP without enable-ha marker - don't deploy
	klog.V(2).Infof("Single control plane without enable-ha marker, skipping kube-vip DaemonSet")
	return false
}

// shouldDeployKubeVipCloudController determines if kube-vip cloud-controller should be deployed
// Deploy cloud-controller when enable-ha marker exists OR in multinode setups
func shouldDeployKubeVipCloudController(cfg *config.Config) bool {
	// Skip on worker-only nodes
	if cfg.MultiNode.WorkerOnly {
		return false
	}

	// Only deploy in multinode mode
	if !cfg.MultiNode.Enabled {
		return false
	}

	// Check if enable-ha marker file exists (for single CP HA mode)
	markerFile := "/var/lib/microshift-data/.enable-ha"
	if exists, _ := util.PathExists(markerFile); exists {
		return true
	}

	// Check if this is a multi-CP setup (comma in Controlplane field)
	if strings.Contains(cfg.MultiNode.Controlplane, ",") {
		return true
	}

	// Single CP without enable-ha marker - don't deploy
	return false
}

func deployKubeVip(ctx context.Context, cfg *config.Config, kubeconfigPath string) error {
	deployDaemonSet := shouldDeployKubeVipDaemonSet(cfg)
	deployCloudController := shouldDeployKubeVipCloudController(cfg)

	if !deployDaemonSet && !deployCloudController {
		klog.V(2).Infof("kube-vip deployment skipped")
		return nil
	}

	if deployDaemonSet {
		klog.Infof("Deploying kube-vip DaemonSet for HA control plane")
	}
	if deployCloudController {
		klog.Infof("Deploying kube-vip cloud-controller for LoadBalancer services")
	}

	// Calculate VIP if needed (for DaemonSet) and not explicitly configured
	vip := cfg.MultiNode.VIP
	if deployDaemonSet && vip == "" {
		vip = calculateVIP(cfg.Node.NodeIP)
		if vip == "" {
			return fmt.Errorf("failed to calculate VIP from node IP: %s", cfg.Node.NodeIP)
		}
		klog.Infof("Calculated VIP: %s", vip)
	}

	// Calculate service LoadBalancer range if not explicitly configured
	serviceLBRange := cfg.MultiNode.ServiceLBRange
	if serviceLBRange == "" {
		serviceLBRange = calculateServiceLBRange(cfg.Node.NodeIP)
		if serviceLBRange == "" {
			return fmt.Errorf("failed to calculate service LB range from node IP: %s", cfg.Node.NodeIP)
		}
		klog.Infof("Calculated service LoadBalancer range: %s", serviceLBRange)
	}

	renderParams := assets.RenderParams{
		"VIP":            vip,
		"ServiceLBRange": serviceLBRange,
	}

	// Deploy namespace
	ns := []string{"components/kube-vip/namespace.yaml"}
	if err := assets.ApplyNamespaces(ctx, ns, kubeconfigPath); err != nil {
		klog.Warningf("Failed to apply kube-vip namespace: %v", err)
		return err
	}

	// Deploy ServiceAccounts
	sa := []string{}
	if deployDaemonSet {
		sa = append(sa, "components/kube-vip/serviceaccount.yaml")
	}
	if deployCloudController {
		sa = append(sa, "components/kube-vip/cloud-controller-serviceaccount.yaml")
	}
	if len(sa) > 0 {
		if err := assets.ApplyServiceAccounts(ctx, sa, kubeconfigPath); err != nil {
			klog.Warningf("Failed to apply kube-vip service accounts: %v", err)
			return err
		}
	}

	// Deploy SCC (only needed for DaemonSet)
	if deployDaemonSet {
		scc := []string{"components/kube-vip/scc.yaml"}
		if err := assets.ApplySCCs(ctx, scc, renderTemplate, renderParamsFromConfig(cfg, renderParams), kubeconfigPath); err != nil {
			klog.Warningf("Failed to apply kube-vip SCC: %v", err)
			return err
		}
	}

	// Deploy RBAC
	cr := []string{}
	crb := []string{}
	if deployDaemonSet {
		cr = append(cr, "components/kube-vip/clusterrole.yaml")
		crb = append(crb, "components/kube-vip/clusterrolebinding.yaml")
	}
	if deployCloudController {
		cr = append(cr, "components/kube-vip/cloud-controller-clusterrole.yaml")
		crb = append(crb, "components/kube-vip/cloud-controller-clusterrolebinding.yaml")
	}

	if len(cr) > 0 {
		if err := assets.ApplyClusterRoles(ctx, cr, kubeconfigPath); err != nil {
			klog.Warningf("Failed to apply kube-vip cluster roles: %v", err)
			return err
		}
	}

	if len(crb) > 0 {
		if err := assets.ApplyClusterRoleBindings(ctx, crb, kubeconfigPath); err != nil {
			klog.Warningf("Failed to apply kube-vip cluster role bindings: %v", err)
			return err
		}
	}

	// Deploy ConfigMap for cloud controller
	if deployCloudController {
		cm := []string{"components/kube-vip/cloud-controller-configmap.yaml"}
		if err := assets.ApplyConfigMaps(ctx, cm, renderTemplate, renderParamsFromConfig(cfg, renderParams), kubeconfigPath); err != nil {
			klog.Warningf("Failed to apply kube-vip configmap: %v", err)
			return err
		}
	}

	// Deploy DaemonSet
	if deployDaemonSet {
		ds := []string{"components/kube-vip/daemonset.yaml"}
		if err := assets.ApplyDaemonSets(ctx, ds, renderTemplate, renderParamsFromConfig(cfg, renderParams), kubeconfigPath); err != nil {
			klog.Warningf("Failed to apply kube-vip daemonset: %v", err)
			return err
		}
	}

	// Deploy cloud controller Deployment
	if deployCloudController {
		deploy := []string{"components/kube-vip/cloud-controller-deployment.yaml"}
		if err := assets.ApplyDeployments(ctx, deploy, renderTemplate, renderParamsFromConfig(cfg, renderParams), kubeconfigPath); err != nil {
			klog.Warningf("Failed to apply kube-vip cloud controller: %v", err)
			return err
		}
	}

	if deployDaemonSet {
		klog.Infof("kube-vip DaemonSet deployed successfully with VIP %s", vip)
	}
	if deployCloudController {
		klog.Infof("kube-vip cloud-controller deployed successfully")
	}
	return nil
}
