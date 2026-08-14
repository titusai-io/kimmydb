package kimmydb_test

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

// A real node to talk to.
//
// The Go client is tested the same way the Rust and Python ones are: against a
// spawned kimmyd, over a socket. Nothing here mocks a response — a client's
// whole job is to be right about what comes back from a server, and a fake
// server is a statement of what this client already believes.

const (
	rootPassword = "go-client-password"
	jwtSecret    = "a-secret-long-enough-for-the-go-client-tests"
)

type node struct {
	base string
	cmd  *exec.Cmd
	dir  string
	log  *os.File
}

// binaryPath finds the kimmyd to drive: release first, then debug, since
// either is a real node and the fast one keeps the suite quick.
func binaryPath(t *testing.T) string {
	t.Helper()
	if override := os.Getenv("KIMMYD_BINARY"); override != "" {
		return override
	}
	root, err := filepath.Abs(filepath.Join("..", "..", ".."))
	if err != nil {
		t.Fatalf("locating the repository root: %v", err)
	}
	for _, profile := range []string{"release", "debug"} {
		candidate := filepath.Join(root, "target", profile, "kimmyd")
		if _, err := os.Stat(candidate); err == nil {
			return candidate
		}
	}
	t.Skip("no kimmyd binary found; run `cargo build` first, or set KIMMYD_BINARY")
	return ""
}

func freePort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("finding a free port: %v", err)
	}
	defer listener.Close()
	return listener.Addr().(*net.TCPAddr).Port
}

// startNode spawns a node whose tokens last tokenTTL seconds.
func startNode(t *testing.T, tokenTTL int) *node {
	t.Helper()
	binary := binaryPath(t)
	dir := t.TempDir()
	port := freePort(t)

	config := fmt.Sprintf(`
[server]
bind = "127.0.0.1:%d"
mcp = false

[storage]
data_dir = "%s"

[auth]
jwt_secret = "%s"
token_ttl_secs = %d
`, port, filepath.Join(dir, "data"), jwtSecret, tokenTTL)

	configPath := filepath.Join(dir, "kimmy.toml")
	if err := os.WriteFile(configPath, []byte(config), 0o600); err != nil {
		t.Fatalf("writing the config: %v", err)
	}

	logFile, err := os.Create(filepath.Join(dir, "node.log"))
	if err != nil {
		t.Fatalf("creating the log: %v", err)
	}

	cmd := exec.Command(binary, "--config", configPath)
	cmd.Env = append(os.Environ(), "KIMMY_ROOT_PASSWORD="+rootPassword)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	if err := cmd.Start(); err != nil {
		t.Fatalf("starting kimmyd: %v", err)
	}

	n := &node{base: fmt.Sprintf("http://127.0.0.1:%d", port), cmd: cmd, dir: dir, log: logFile}
	t.Cleanup(n.stop)
	n.waitReady(t)
	return n
}

func (n *node) waitReady(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	client := &http.Client{Timeout: time.Second}
	for time.Now().Before(deadline) {
		response, err := client.Get(n.base + "/healthz")
		if err == nil {
			response.Body.Close()
			if response.StatusCode == http.StatusOK {
				return
			}
		}
		if n.cmd.ProcessState != nil && n.cmd.ProcessState.Exited() {
			logs, _ := os.ReadFile(filepath.Join(n.dir, "node.log"))
			t.Fatalf("kimmyd exited; log:\n%s", logs)
		}
		time.Sleep(50 * time.Millisecond)
	}
	logs, _ := os.ReadFile(filepath.Join(n.dir, "node.log"))
	t.Fatalf("kimmyd never became healthy; log:\n%s", logs)
}

func (n *node) stop() {
	if n.cmd.Process != nil {
		_ = n.cmd.Process.Kill()
		_ = n.cmd.Wait()
	}
	_ = n.log.Close()
}

// testContext bounds every test, so a change stream that never delivers fails
// rather than stalling the package. A hang is a failure mode this client can
// genuinely have.
func testContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	return ctx
}
