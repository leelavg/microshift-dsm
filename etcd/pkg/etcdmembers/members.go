// Package etcdmembers provides utilities for reading etcd cluster membership
// from the local bbolt database without requiring a running etcd server.
package etcdmembers

import (
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	bolt "go.etcd.io/bbolt"
)

// Member represents an etcd cluster member as stored in the bbolt database.
type Member struct {
	ID         uint64   `json:"id"`
	Name       string   `json:"name"`
	PeerURLs   []string `json:"peerURLs"`
	ClientURLs []string `json:"clientURLs"`
}

// ReadMembersFromDB reads the etcd member list from the local bbolt database.
// This allows reading cluster membership offline (before etcd starts).
// Returns nil, nil if the database doesn't exist (first boot scenario).
func ReadMembersFromDB(etcdDataDir string) ([]Member, error) {
	dbPath := filepath.Join(etcdDataDir, "member", "snap", "db")

	// Check if database exists
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		return nil, nil // First boot, no database yet
	}

	// Open database in read-only mode to avoid locking issues
	db, err := bolt.Open(dbPath, 0400, &bolt.Options{ReadOnly: true})
	if err != nil {
		return nil, fmt.Errorf("failed to open etcd database: %w", err)
	}
	defer db.Close()

	var members []Member
	err = db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("members"))
		if b == nil {
			// No members bucket yet - might be empty/new database
			return nil
		}

		return b.ForEach(func(k, v []byte) error {
			var m Member
			if err := json.Unmarshal(v, &m); err != nil {
				// Skip malformed entries rather than failing entirely
				return nil
			}
			// Only include members with valid peer URLs
			if len(m.PeerURLs) > 0 && m.Name != "" {
				members = append(members, m)
			}
			return nil
		})
	})

	if err != nil {
		return nil, fmt.Errorf("failed to read members from database: %w", err)
	}

	return members, nil
}

// BuildInitialCluster generates the ETCD_INITIAL_CLUSTER string from member list.
// Format: "name1=https://ip1:2380,name2=https://ip2:2380,..."
func BuildInitialCluster(members []Member) string {
	var parts []string
	for _, m := range members {
		if len(m.PeerURLs) > 0 {
			parts = append(parts, fmt.Sprintf("%s=%s", m.Name, m.PeerURLs[0]))
		}
	}
	return strings.Join(parts, ",")
}

// ExtractControlPlaneIPs extracts IP addresses from member peer URLs.
// Used to generate the .cluster-config file.
func ExtractControlPlaneIPs(members []Member) []string {
	var ips []string
	seen := make(map[string]bool)

	for _, m := range members {
		for _, peerURL := range m.PeerURLs {
			ip := extractIPFromURL(peerURL)
			if ip != "" && !seen[ip] {
				ips = append(ips, ip)
				seen[ip] = true
			}
		}
	}
	return ips
}

// extractIPFromURL extracts the IP address from a URL like "https://10.89.0.11:2380"
func extractIPFromURL(rawURL string) string {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return ""
	}
	host := parsed.Hostname()
	return host
}
