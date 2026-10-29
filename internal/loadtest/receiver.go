package loadtest

import (
	"context"
	"hash/fnv"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"webhookdelivery/internal/invariants"
	"webhookdelivery/internal/sign"
)

// FaultKind is a way for the receiver to misbehave.
type FaultKind int

const (
	FaultNone      FaultKind = iota
	FaultStatus              // answer with an error status (default 500)
	FaultSlow                // answer 200, but only after Delay (longer than the sender's timeout)
	FaultReset               // accept the request, then drop the connection
	FaultGarbage             // answer with bytes that are not HTTP
	FaultBigBody             // answer 200 with a 5MB body (the sender caps what it reads)
	FaultBlackhole           // never answer
)

func (k FaultKind) String() string {
	return [...]string{"none", "status", "slow", "reset", "garbage", "bigbody", "blackhole"}[k]
}

// Fault describes the receiver's current behaviour: with probability Prob a
// request suffers Kind; otherwise it is answered normally.
type Fault struct {
	Kind   FaultKind
	Prob   float64 // 0 means 1 (every request) when Kind != FaultNone
	Status int     // for FaultStatus; default 500
	Delay  time.Duration
}

func (f Fault) String() string {
	if f.Kind == FaultNone {
		return "healthy"
	}
	p := f.Prob
	if p == 0 {
		p = 1
	}
	return f.Kind.String() + " " + formatPct(p)
}

func formatPct(p float64) string { return strconv.Itoa(int(p*100+0.5)) + "%" }

const shards = 64

type rec struct {
	first time.Time
	n     int
}

type shard struct {
	mu sync.Mutex
	m  map[invariants.Key]*rec
}

// Receiver is an HTTP server standing in for every customer endpoint. It
// verifies webhook signatures, records exactly what it acknowledged, and can be
// told to misbehave.
type Receiver struct {
	ln  net.Listener
	srv *http.Server

	secrets sync.Map // endpoint id -> []byte (signing key)
	fault   atomic.Pointer[Fault]
	obs     [shards]shard

	Requests     atomic.Int64
	Acked        atomic.Int64 // requests answered 2xx and recorded
	Faulted      atomic.Int64
	BadSignature atomic.Int64 // a request whose signature did not verify: always a bug
}

func NewReceiver() (*Receiver, error) {
	ln, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	r := &Receiver{ln: ln}
	for i := range r.obs {
		r.obs[i].m = map[invariants.Key]*rec{}
	}
	r.fault.Store(&Fault{})
	mux := http.NewServeMux()
	mux.HandleFunc("POST /hook/{endpoint}", r.handle)
	r.srv = &http.Server{Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	go func() { _ = r.srv.Serve(ln) }()
	return r, nil
}

func (r *Receiver) Close() { _ = r.srv.Close() }

// URL is where an endpoint with the given id should be pointed.
func (r *Receiver) URL(endpointID string) string {
	return "http://" + r.ln.Addr().String() + "/hook/" + endpointID
}

// Register tells the receiver the signing secret of an endpoint.
func (r *Receiver) Register(endpointID, secret string) error {
	key, err := sign.ParseSecret(secret)
	if err != nil {
		return err
	}
	r.secrets.Store(endpointID, key)
	return nil
}

func (r *Receiver) SetFault(f Fault) { r.fault.Store(&f) }

func (r *Receiver) shardFor(k invariants.Key) *shard {
	h := fnv.New32a()
	_, _ = h.Write([]byte(k.Event))
	_, _ = h.Write([]byte(k.Endpoint))
	return &r.obs[h.Sum32()%shards]
}

func (r *Receiver) record(k invariants.Key) {
	sh := r.shardFor(k)
	sh.mu.Lock()
	if e, ok := sh.m[k]; ok {
		e.n++
	} else {
		sh.m[k] = &rec{first: time.Now(), n: 1}
	}
	sh.mu.Unlock()
	r.Acked.Add(1)
}

// Count and Each implement invariants.Observations.
func (r *Receiver) Count(event, endpoint string) int {
	k := invariants.Key{Event: event, Endpoint: endpoint}
	sh := r.shardFor(k)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if e, ok := sh.m[k]; ok {
		return e.n
	}
	return 0
}

func (r *Receiver) Each(f func(k invariants.Key, n int)) {
	for i := range r.obs {
		sh := &r.obs[i]
		sh.mu.Lock()
		for k, e := range sh.m {
			f(k, e.n)
		}
		sh.mu.Unlock()
	}
}

// FirstSeen is when the endpoint first acknowledged the event.
func (r *Receiver) FirstSeen(event, endpoint string) (time.Time, bool) {
	k := invariants.Key{Event: event, Endpoint: endpoint}
	sh := r.shardFor(k)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if e, ok := sh.m[k]; ok {
		return e.first, true
	}
	return time.Time{}, false
}

func (r *Receiver) handle(w http.ResponseWriter, req *http.Request) {
	r.Requests.Add(1)
	endpoint := req.PathValue("endpoint")
	body, err := io.ReadAll(http.MaxBytesReader(w, req.Body, 1<<20))
	if err != nil {
		http.Error(w, "bad body", http.StatusBadRequest)
		return
	}

	keyAny, ok := r.secrets.Load(endpoint)
	if !ok {
		http.Error(w, "unknown endpoint", http.StatusNotFound)
		return
	}
	id := req.Header.Get("webhook-id")
	if err := sign.Verify(req.Header.Get("webhook-signature"), [][]byte{keyAny.([]byte)}, id,
		req.Header.Get("webhook-timestamp"), body, 5*time.Minute, time.Now()); err != nil {
		r.BadSignature.Add(1)
		http.Error(w, "invalid signature", http.StatusUnauthorized)
		return
	}
	key := invariants.Key{Event: id, Endpoint: endpoint}

	f := *r.fault.Load()
	if f.Kind != FaultNone && (f.Prob == 0 || rand.Float64() < f.Prob) { //nolint:gosec // fault injection, not security
		r.Faulted.Add(1)
		switch f.Kind {
		case FaultStatus:
			code := f.Status
			if code == 0 {
				code = http.StatusInternalServerError
			}
			http.Error(w, "injected failure", code)
			return
		case FaultSlow:
			// The request was processed; only the answer is late. If the sender gave up
			// meanwhile it will retry, and the endpoint will have seen the event twice.
			r.record(key)
			select {
			case <-time.After(f.Delay):
			case <-req.Context().Done():
				return
			}
			_, _ = w.Write([]byte("ok"))
			return
		case FaultReset:
			if c, _, err := w.(http.Hijacker).Hijack(); err == nil {
				_ = c.Close()
			}
			return
		case FaultGarbage:
			if c, _, err := w.(http.Hijacker).Hijack(); err == nil {
				_, _ = c.Write([]byte("\x00\x01 this is not HTTP \xff\xfe\r\n\r\n"))
				_ = c.Close()
			}
			return
		case FaultBigBody:
			r.record(key)
			w.Header().Set("Content-Type", "application/octet-stream")
			chunk := make([]byte, 64<<10)
			for range 80 { // 5MB
				if _, err := w.Write(chunk); err != nil {
					return
				}
			}
			return
		case FaultBlackhole:
			ctx, cancel := context.WithTimeout(req.Context(), 30*time.Second)
			defer cancel()
			<-ctx.Done()
			return
		}
	}
	r.record(key)
	_, _ = w.Write([]byte("ok"))
}
