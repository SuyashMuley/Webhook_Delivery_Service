package loadtest

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"sync"
	"syscall"
	"time"
)

// Proc is a child process (an api or worker binary) that can be crashed and restarted.
type Proc struct {
	Name    string
	Bin     string
	Env     []string
	LogPath string
	Ready   string // URL polled until it answers 200

	mu   sync.Mutex
	cmd  *exec.Cmd
	done chan struct{}
	log  *os.File
	Exit error // how the last run ended
	Runs int
}

func (p *Proc) Start() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.cmd != nil {
		return fmt.Errorf("%s is already running", p.Name)
	}
	lf, err := os.OpenFile(p.LogPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	cmd := exec.CommandContext(context.Background(), p.Bin) //nolint:gosec // the binaries are built from this repository; the process outlives any one request context
	cmd.Env = append(os.Environ(), p.Env...)
	cmd.Stdout, cmd.Stderr = lf, lf
	if err := cmd.Start(); err != nil {
		_ = lf.Close()
		return err
	}
	p.cmd, p.log, p.done = cmd, lf, make(chan struct{})
	p.Runs++
	go func(cmd *exec.Cmd, done chan struct{}) {
		err := cmd.Wait()
		p.mu.Lock()
		p.Exit = err
		p.cmd = nil
		_ = p.log.Close()
		p.mu.Unlock()
		close(done)
	}(cmd, p.done)
	return nil
}

func (p *Proc) pid() (int, chan struct{}) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.cmd == nil {
		return 0, nil
	}
	return p.cmd.Process.Pid, p.done
}

func (p *Proc) Alive() bool { pid, _ := p.pid(); return pid != 0 }

// Kill is `kill -9`: no signal handlers run, no drain, nothing is released.
func (p *Proc) Kill() {
	if pid, done := p.pid(); pid != 0 {
		_ = syscall.Kill(pid, syscall.SIGKILL)
		<-done
	}
}

// Stop asks the process to shut down gracefully (SIGTERM) and reports whether it
// exited on its own within timeout; if not it is killed.
func (p *Proc) Stop(timeout time.Duration) (graceful bool) {
	pid, done := p.pid()
	if pid == 0 {
		return true
	}
	_ = syscall.Kill(pid, syscall.SIGTERM)
	select {
	case <-done:
		return p.Exit == nil
	case <-time.After(timeout):
		_ = syscall.Kill(pid, syscall.SIGKILL)
		<-done
		return false
	}
}

// WaitReady polls the readiness URL until it answers 200.
func (p *Proc) WaitReady(ctx context.Context, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if !p.Alive() {
			return fmt.Errorf("%s exited before becoming ready (see %s)", p.Name, p.LogPath)
		}
		req, _ := http.NewRequestWithContext(ctx, "GET", p.Ready, nil)
		if resp, err := http.DefaultClient.Do(req); err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == 200 {
				return nil
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("%s not ready after %v (see %s)", p.Name, timeout, p.LogPath)
}

// Fleet is the set of service processes under test.
type Fleet struct {
	APIs    []*Proc
	Workers []*Proc
	// URLs
	APIURLs       []string
	MetricsURLs   []string // all processes' /metrics
	WorkerMetrics []string
}

// All returns every process.
func (f *Fleet) All() []*Proc { return append(append([]*Proc{}, f.APIs...), f.Workers...) }

// StopAll gracefully stops every process; it reports which ones had to be killed.
func (f *Fleet) StopAll(timeout time.Duration) (killed []string) {
	var wg sync.WaitGroup
	var mu sync.Mutex
	for _, p := range f.All() {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if !p.Stop(timeout) {
				mu.Lock()
				killed = append(killed, p.Name)
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	return killed
}
