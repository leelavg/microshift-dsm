package main

import (
	"context"
	"fmt"
	"math"
	"net"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	configv1 "github.com/openshift/api/config/v1"
	"github.com/openshift/microshift/etcd/pkg/etcdmembers"
	"github.com/openshift/microshift/pkg/config"
	"github.com/openshift/microshift/pkg/util/cryptomaterial"

	"github.com/spf13/cobra"
	etcd "go.etcd.io/etcd/server/v3/embed"
	"go.etcd.io/etcd/server/v3/storage/backend"
	"k8s.io/klog/v2"
)

const (
	// MaxLearners determines the maximum number of etcd learners in the cluster. This is
	// needed to support topology transitions without losing high availability.
	MaxLearners = 2
)

func NewRunEtcdCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use: "run",
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			cfg, err := config.ActiveConfig()
			if err != nil {
				klog.Fatalf("Error in reading and validating MicroShift config: %v", err)
			}

			e := NewEtcd(cfg)
			return e.Run()
		},
	}

	return cmd
}

type EtcdService struct {
	etcdCfg                 *etcd.Config
	minDefragBytes          int64
	maxFragmentedPercentage float64
	defragCheckFreq         time.Duration
}

func NewEtcd(cfg *config.Config) *EtcdService {
	s := &EtcdService{}
	s.configure(cfg)
	return s
}

func (s *EtcdService) Name() string { return "etcd" }

func (s *EtcdService) configure(cfg *config.Config) {
	s.minDefragBytes = cfg.Etcd.MinDefragBytes
	s.maxFragmentedPercentage = cfg.Etcd.MaxFragmentedPercentage
	s.defragCheckFreq = cfg.Etcd.DefragCheckFreq

	certsDir := cryptomaterial.CertsDirectory(config.DataDir)

	etcdServingCertDir := cryptomaterial.EtcdServingCertDir(certsDir)
	etcdPeerCertDir := cryptomaterial.EtcdPeerCertDir(certsDir)
	etcdSignerCertPath := cryptomaterial.CACertPath(cryptomaterial.EtcdSignerDir(certsDir))
	dataDir := filepath.Join(config.DataDir, s.Name())

	// based on https://github.com/openshift/cluster-etcd-operator/blob/master/bindata/bootkube/bootstrap-manifests/etcd-member-pod.yaml#L19
	s.etcdCfg = etcd.NewConfig()
	s.etcdCfg.ClusterState = "new"
	s.etcdCfg.Logger = "zap"
	s.etcdCfg.Dir = dataDir
	s.etcdCfg.QuotaBackendBytes = cfg.Etcd.QuotaBackendBytes
	s.etcdCfg.AdvertisePeerUrls = setURL([]string{cfg.Node.NodeIP}, "2380")
	s.etcdCfg.ListenPeerUrls = setURL([]string{"0.0.0.0"}, "2380")
	s.etcdCfg.AdvertiseClientUrls = setURL([]string{cfg.Node.NodeIP}, "2379")
	s.etcdCfg.ListenClientUrls = setURL([]string{"0.0.0.0"}, "2379")
	s.etcdCfg.ListenMetricsUrls = setURL([]string{cfg.Node.NodeIP}, "2381")

	s.etcdCfg.Name = cfg.Node.HostnameOverride
	s.etcdCfg.InitialCluster = fmt.Sprintf("%s=https://%s", cfg.Node.HostnameOverride, net.JoinHostPort(cfg.Node.NodeIP, "2380"))

	s.etcdCfg.TlsMinVersion = getTLSMinVersion(cfg.ApiServer.TLS.MinVersion)
	if cfg.ApiServer.TLS.MinVersion != string(configv1.VersionTLS13) {
		s.etcdCfg.CipherSuites = cfg.ApiServer.TLS.CipherSuites
	}
	s.etcdCfg.ClientTLSInfo.CertFile = cryptomaterial.PeerCertPath(etcdServingCertDir)
	s.etcdCfg.ClientTLSInfo.KeyFile = cryptomaterial.PeerKeyPath(etcdServingCertDir)
	s.etcdCfg.ClientTLSInfo.TrustedCAFile = etcdSignerCertPath

	s.etcdCfg.PeerTLSInfo.CertFile = cryptomaterial.PeerCertPath(etcdPeerCertDir)
	s.etcdCfg.PeerTLSInfo.KeyFile = cryptomaterial.PeerKeyPath(etcdPeerCertDir)
	s.etcdCfg.PeerTLSInfo.TrustedCAFile = etcdSignerCertPath

	s.etcdCfg.MaxLearners = MaxLearners

	// Regenerate config files from etcd database if resuming (self-healing)
	// This ensures config files are always in sync with actual cluster membership
	if err := regenerateConfigsFromDB(dataDir); err != nil {
		klog.Warningf("Failed to regenerate configs from etcd DB: %v", err)
	}

	updateConfigFromFile(s.etcdCfg, getConfigFilePath())
}

func (s *EtcdService) Run() error {
	if os.Geteuid() > 0 {
		klog.Fatalf("microshift-etcd must be run privileged")
	}

	versionInfo := EtcdVersionInfo
	klog.InfoS("Version", "microshift-etcd", versionInfo.String(), "etcd-base", versionInfo.EtcdVersion)

	e, err := etcd.StartEtcd(s.etcdCfg)
	if err != nil {
		return fmt.Errorf("microshift-etcd failed to start: %v", err)
	}
	<-e.Server.ReadyNotify()
	
	// Regenerate cluster configs from etcd DB on every startup
	// This ensures .cluster-config exists with enable_ha field for bootstrap and joining nodes
	// Use s.etcdCfg.Dir which is the actual etcd data directory
	if err := regenerateConfigsFromDB(s.etcdCfg.Dir); err != nil {
		klog.Warningf("Failed to regenerate cluster configs: %v", err)
	}
	
	defer func() {
		e.Server.Stop()
		<-e.Server.StopNotify()
	}()

	// Go ahead and do a defragment now.
	if err := e.Server.Backend().Defrag(); err != nil {
		err = fmt.Errorf("initial defragmentation failed: %v", err)
		klog.Error(err)
		return err
	}

	// Start up the defrag controller.
	defragCtx, defragShutdown := context.WithCancel(context.Background())
	go s.defragController(defragCtx, e.Server.Backend())

	// Wait to be stopped.
	sigTerm := make(chan os.Signal, 1)
	signal.Notify(sigTerm, os.Interrupt, syscall.SIGTERM)
	sig := <-sigTerm
	klog.Infof("microshift-etcd received signal %v - stopping", sig)

	// Shutdown the defrag controller.
	defragShutdown()

	return nil
}

func (s *EtcdService) defragController(ctx context.Context, be backend.Backend) {
	// Stop the controller if defrags are disabled.
	if s.defragCheckFreq == 0 {
		klog.Warning("defragmentation has been disabled")
		return
	}

	// This timer will check the fragmented conditions periodically.
	timer := time.NewTimer(s.defragCheckFreq)
	defer func() {
		if !timer.Stop() {
			<-timer.C
		}
	}()
	for {
		select {
		case <-ctx.Done():
			return
		case start := <-timer.C:
			if isBackendFragmented(be, s.maxFragmentedPercentage, s.minDefragBytes) {
				klog.Info("attempting to defragment backend")
				if err := be.Defrag(); err != nil {
					klog.Errorf("defragmentation failed: %v", err)
				} else {
					klog.Infof("defragmentation took %v", time.Since(start))
				}
			}
			timer.Reset(s.defragCheckFreq)
		}
	}
}

func setURL(hostnames []string, port string) []url.URL {
	urls := make([]url.URL, len(hostnames))
	for i, name := range hostnames {
		u, err := url.Parse("https://" + net.JoinHostPort(name, port))
		if err != nil {
			klog.Errorf("failed to parse url: %v", err)
			return []url.URL{}
		}
		urls[i] = *u
	}
	return urls
}

func getTLSMinVersion(minVersion string) string {
	switch minVersion {
	case string(configv1.VersionTLS12):
		return "TLS1.2"
	case string(configv1.VersionTLS13):
		return "TLS1.3"
	}
	return ""
}

// The following 'fragemented' logic is copied from the Openshift Cluster Etcd Operator.
//
//	https://github.com/openshift/cluster-etcd-operator/blob/0584b0d1c8868535baf889d8c199f605aef4a3ae/pkg/operator/defragcontroller/defragcontroller.go#L282
func isBackendFragmented(b backend.Backend, maxFragmentedPercentage float64, minDefragBytes int64) bool {
	fragmentedPercentage := checkFragmentationPercentage(b.Size(), b.SizeInUse())
	if fragmentedPercentage > 0.00 {
		klog.Infof("backend store fragmented: %.2f %%, dbSize: %d", fragmentedPercentage, b.Size())
	}
	return fragmentedPercentage >= maxFragmentedPercentage && b.Size() >= minDefragBytes
}

func checkFragmentationPercentage(ondisk, inuse int64) float64 {
	diff := float64(ondisk - inuse)
	fragmentedPercentage := (diff / float64(ondisk)) * 100
	return math.Round(fragmentedPercentage*100) / 100
}

func getConfigFilePath() string {
	return filepath.Join(config.DataDir, "etcd", "config")
}

func updateConfigFromFile(etcdCfg *etcd.Config, configPath string) {
	data, err := os.ReadFile(configPath)
	if err != nil {
		klog.Errorf("failed to read config file: %v", err)
		return
	}
	lines := strings.Split(string(data), "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		eqIdx := strings.Index(line, "=")
		if eqIdx == -1 {
			continue
		}
		parts := []string{line[:eqIdx], line[eqIdx+1:]}
		key := strings.TrimSpace(parts[0])
		val := strings.TrimSpace(parts[1])
		switch key {
		case "ETCD_INITIAL_CLUSTER":
			etcdCfg.InitialCluster = val
		case "ETCD_INITIAL_CLUSTER_STATE":
			etcdCfg.ClusterState = val
		}
	}
}

// regenerateConfigsFromDB reads etcd member list from the local bbolt database
// and regenerates the config files. This provides self-healing for bootstrap nodes
// and ensures configs stay in sync with actual cluster membership.
func regenerateConfigsFromDB(etcdDataDir string) error {
	members, err := etcdmembers.ReadMembersFromDB(etcdDataDir)
	if err != nil {
		return fmt.Errorf("failed to read members from DB: %w", err)
	}

	// No members means first boot or empty DB - skip regeneration
	if len(members) == 0 {
		klog.V(2).Info("No members found in etcd DB, skipping config regeneration")
		return nil
	}

	// Build ETCD_INITIAL_CLUSTER string and extract control plane IPs
	initialCluster := etcdmembers.BuildInitialCluster(members)
	controlPlaneIPs := etcdmembers.ExtractControlPlaneIPs(members)

	if initialCluster == "" {
		klog.V(2).Info("Empty initial cluster string, skipping config regeneration")
		return nil
	}

	// Write etcd/config
	etcdConfigContent := fmt.Sprintf("ETCD_INITIAL_CLUSTER=%s\nETCD_INITIAL_CLUSTER_STATE=existing\n", initialCluster)
	etcdConfigPath := filepath.Join(etcdDataDir, "config")
	if err := os.WriteFile(etcdConfigPath, []byte(etcdConfigContent), 0600); err != nil {
		return fmt.Errorf("failed to write etcd config: %w", err)
	}

	// Write .cluster-config (used by OVN, kubelet, kube-vip, etc.)
	// Check if enable-ha marker exists to persist HA decision cluster-wide
	enableHA := "false"
	markerFile := filepath.Join(config.DataDir, ".enable-ha")
	if exists, _ := os.Stat(markerFile); exists == nil {
		enableHA = "true"
	}
	
	clusterConfigContent := fmt.Sprintf("# Auto-regenerated from etcd database\ncontrolplane: %s\nenable_ha: %s\n",
		strings.Join(controlPlaneIPs, ","), enableHA)
	clusterConfigPath := filepath.Join(config.DataDir, ".cluster-config")
	if err := os.WriteFile(clusterConfigPath, []byte(clusterConfigContent), 0600); err != nil {
		return fmt.Errorf("failed to write cluster config: %w", err)
	}

	klog.Infof("Regenerated cluster configs from etcd DB: %d members, IPs: %v", len(members), controlPlaneIPs)
	return nil
}
