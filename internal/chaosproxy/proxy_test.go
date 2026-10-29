package chaosproxy

import (
	"bufio"
	"context"
	"errors"
	"net"
	"testing"
	"time"
)

// echoServer replies to each line with the same line.
func echoServer(t *testing.T) string {
	t.Helper()
	ln, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer func() { _ = c.Close() }()
				r := bufio.NewReader(c)
				for {
					line, err := r.ReadString('\n')
					if err != nil {
						return
					}
					_, _ = c.Write([]byte(line))
				}
			}()
		}
	}()
	return ln.Addr().String()
}

func dial(addr string) (net.Conn, error) {
	return (&net.Dialer{Timeout: 2 * time.Second}).DialContext(context.Background(), "tcp", addr)
}

func roundTrip(c net.Conn, msg string, timeout time.Duration) (string, error) {
	_ = c.SetDeadline(time.Now().Add(timeout))
	if _, err := c.Write([]byte(msg + "\n")); err != nil {
		return "", err
	}
	return bufio.NewReader(c).ReadString('\n')
}

func TestPassForwards(t *testing.T) {
	p, err := New("127.0.0.1:0", echoServer(t))
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	c, err := dial(p.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	if got, err := roundTrip(c, "hello", time.Second); err != nil || got != "hello\n" {
		t.Fatalf("round trip = %q, %v", got, err)
	}
}

// A stall is silent: the client's request just hangs, and when the partition
// heals the same connection carries on, as TCP would after a transient outage.
func TestStallHoldsTrafficThenResumes(t *testing.T) {
	p, _ := New("127.0.0.1:0", echoServer(t))
	defer p.Close()
	c, _ := dial(p.Addr())
	defer func() { _ = c.Close() }()
	if _, err := roundTrip(c, "warmup", time.Second); err != nil {
		t.Fatal(err)
	}

	p.SetMode(Stall)
	if _, err := roundTrip(c, "during", 300*time.Millisecond); err == nil {
		t.Fatal("traffic flowed during a stall")
	} else if ne := net.Error(nil); !errors.As(err, &ne) || !ne.Timeout() {
		t.Fatalf("a stall must look like a timeout, got %v", err)
	}

	// The connection was timed out on the client side; open a fresh one while stalled: it must hang too.
	c2, _ := dial(p.Addr())
	defer func() { _ = c2.Close() }()
	go func() { time.Sleep(300 * time.Millisecond); p.SetMode(Pass) }()
	start := time.Now()
	got, err := roundTrip(c2, "after-heal", 3*time.Second)
	if err != nil || got != "after-heal\n" {
		t.Fatalf("after healing: %q, %v", got, err)
	}
	if time.Since(start) < 250*time.Millisecond {
		t.Fatalf("the request completed in %v; it should have waited for the heal", time.Since(start))
	}
}

func TestResetSeversConnectionsAndRefusesNewOnes(t *testing.T) {
	p, _ := New("127.0.0.1:0", echoServer(t))
	defer p.Close()
	c, _ := dial(p.Addr())
	defer func() { _ = c.Close() }()
	if _, err := roundTrip(c, "ok", time.Second); err != nil {
		t.Fatal(err)
	}

	p.SetMode(Reset)
	if _, err := roundTrip(c, "dead", time.Second); err == nil {
		t.Fatal("an existing connection survived a reset")
	}
	if c2, err := dial(p.Addr()); err == nil {
		if _, err := roundTrip(c2, "x", 500*time.Millisecond); err == nil {
			t.Fatal("a new connection worked during a reset")
		}
		_ = c2.Close()
	}

	p.SetMode(Pass)
	c3, err := dial(p.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c3.Close() }()
	if got, err := roundTrip(c3, "back", time.Second); err != nil || got != "back\n" {
		t.Fatalf("after healing a reset: %q, %v", got, err)
	}
}
