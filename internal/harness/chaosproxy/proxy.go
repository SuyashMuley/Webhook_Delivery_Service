// Package chaosproxy is a small TCP proxy that can simulate a network
// partition between a service and its database.
//
// Three behaviours, switchable while connections are open:
//
//	Pass      forward traffic normally
//	Stall     a silent partition: connections stay open but no bytes move, so
//	          clients see timeouts, not errors (what a dropped route looks like)
//	Reset     a loud partition: existing connections are severed and new ones
//	          are refused immediately (what a failed host or firewall looks like)
//
// It exists for tests and the load/chaos tool only.
package chaosproxy

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

type Mode int32

const (
	Pass Mode = iota
	Stall
	Reset
)

func (m Mode) String() string { return [...]string{"pass", "stall", "reset"}[m] }

type Proxy struct {
	ln     net.Listener
	target string

	mode atomic.Int32

	mu     sync.Mutex
	conns  map[net.Conn]struct{}
	closed bool
	wg     sync.WaitGroup

	healed chan struct{} // closed and replaced each time the mode leaves Stall
	done   chan struct{} // closed by Close
}

// New listens on listen (e.g. "127.0.0.1:0") and forwards to target.
func New(listen, target string) (*Proxy, error) {
	ln, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", listen)
	if err != nil {
		return nil, err
	}
	p := &Proxy{ln: ln, target: target, conns: map[net.Conn]struct{}{}, healed: make(chan struct{}), done: make(chan struct{})}
	p.wg.Add(1)
	go p.accept()
	return p, nil
}

// Addr is the address clients should connect to.
func (p *Proxy) Addr() string { return p.ln.Addr().String() }

// SetMode switches behaviour immediately, including for open connections.
func (p *Proxy) SetMode(m Mode) {
	old := Mode(p.mode.Swap(int32(m)))
	if old == Stall && m != Stall {
		p.mu.Lock()
		close(p.healed)
		p.healed = make(chan struct{})
		p.mu.Unlock()
	}
	if m == Reset {
		p.severAll()
	}
}

func (p *Proxy) Mode() Mode { return Mode(p.mode.Load()) }

// Close stops the proxy and drops every connection.
func (p *Proxy) Close() {
	p.mu.Lock()
	already := p.closed
	p.closed = true
	p.mu.Unlock()
	if !already {
		close(p.done)
	}
	_ = p.ln.Close()
	p.severAll()
	p.wg.Wait()
}

func (p *Proxy) severAll() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for c := range p.conns {
		_ = c.Close()
	}
}

func (p *Proxy) track(c net.Conn) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return false
	}
	p.conns[c] = struct{}{}
	return true
}

func (p *Proxy) untrack(c net.Conn) {
	p.mu.Lock()
	delete(p.conns, c)
	p.mu.Unlock()
}

func (p *Proxy) accept() {
	defer p.wg.Done()
	for {
		client, err := p.ln.Accept()
		if err != nil {
			return
		}
		if p.Mode() == Reset {
			_ = client.Close()
			continue
		}
		p.wg.Add(1)
		go p.serve(client)
	}
}

func (p *Proxy) serve(client net.Conn) {
	defer p.wg.Done()
	// A stalled proxy still accepts the connection; the dial happens once it heals.
	if !p.track(client) {
		_ = client.Close()
		return
	}
	defer p.untrack(client)
	defer func() { _ = client.Close() }()

	if !p.waitHealed() {
		return
	}
	server, err := (&net.Dialer{Timeout: 5 * time.Second}).DialContext(context.Background(), "tcp", p.target)
	if err != nil {
		return
	}
	if !p.track(server) {
		_ = server.Close()
		return
	}
	defer p.untrack(server)
	defer func() { _ = server.Close() }()

	done := make(chan struct{}, 2)
	go func() { p.pipe(server, client); done <- struct{}{} }()
	go func() { p.pipe(client, server); done <- struct{}{} }()
	<-done // either side closing ends the session
}

// waitHealed blocks while the proxy is stalled and reports whether the session
// should go on. It never reads from the client: doing so would consume bytes
// that belong to the stream.
func (p *Proxy) waitHealed() bool {
	for p.Mode() == Stall {
		p.mu.Lock()
		ch := p.healed
		p.mu.Unlock()
		select {
		case <-ch:
		case <-p.done:
			return false
		}
	}
	return p.Mode() != Reset
}

// pipe copies src to dst, holding each chunk while the proxy is stalled.
func (p *Proxy) pipe(dst, src net.Conn) {
	buf := make([]byte, 32<<10)
	for {
		n, err := src.Read(buf)
		if n > 0 {
			if !p.waitHealed() {
				return
			}
			if _, werr := dst.Write(buf[:n]); werr != nil {
				return
			}
		}
		if err != nil { // io.EOF (the sender closed) and real errors both end the session
			return
		}
	}
}
