package loadtest

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"webhookdelivery/internal/auth"
	"webhookdelivery/internal/sign"
	"webhookdelivery/internal/store"
)

// SetupTenants creates n tenants (directly in the database, as an operator
// would) with quotas high enough not to interfere, then creates fanout
// endpoints per tenant through the real API, each pointing at the receiver with
// its own signing secret.
func SetupTenants(ctx context.Context, owner *store.Store, apiURL string, recv *Receiver, n, fanout int) ([]Tenant, error) {
	var out []Tenant
	for i := range n {
		t, err := owner.CreateTenant(ctx, fmt.Sprintf("loadtest-%d", i))
		if err != nil {
			return nil, err
		}
		if err := owner.SetTenantLimits(ctx, t.ID, 1_000_000, 50_000_000, 5_000); err != nil {
			return nil, err
		}
		key, prefix, err := auth.GenerateKey()
		if err != nil {
			return nil, err
		}
		if _, err := owner.CreateAPIKey(ctx, t.ID, prefix, auth.HashKey(key), nil); err != nil {
			return nil, err
		}
		tn := Tenant{ID: t.ID, Key: key}
		for j := range fanout {
			secret, err := sign.GenerateSecret()
			if err != nil {
				return nil, err
			}
			// The endpoint's id is chosen by the server, and the receiver needs it in the
			// URL, so create it with a placeholder and point it at itself afterwards.
			id, err := apiCall(ctx, apiURL, key, "POST", "/v1/endpoints", map[string]any{
				"url": "http://127.0.0.1:1/placeholder", "secret": secret, "max_concurrency": 20,
				"description": fmt.Sprintf("load test %d/%d", i, j),
			})
			if err != nil {
				return nil, err
			}
			if _, err := apiCall(ctx, apiURL, key, "PATCH", "/v1/endpoints/"+id, map[string]any{"url": recv.URL(id)}); err != nil {
				return nil, err
			}
			if err := recv.Register(id, secret); err != nil {
				return nil, err
			}
			tn.Endpoints = append(tn.Endpoints, id)
		}
		out = append(out, tn)
	}
	return out, nil
}

// apiCall performs one API request and returns the "id" of the response.
func apiCall(ctx context.Context, base, key, method, path string, body any) (string, error) {
	raw, _ := json.Marshal(body)
	req, _ := http.NewRequestWithContext(ctx, method, base+path, bytes.NewReader(raw))
	req.Header.Set("Authorization", "Bearer "+key)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return "", fmt.Errorf("%s %s = %d: %s", method, path, resp.StatusCode, b)
	}
	var out struct {
		ID string `json:"id"`
	}
	_ = json.Unmarshal(b, &out)
	return out.ID, nil
}
