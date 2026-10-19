// Package smsflow implements the Whatomate channel contract using SMSFlow's
// public HTTPS API. It contains no private SMSFlow application dependencies.
package smsflow

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/shridarpatil/whatomate/pkg/channels"
)

const Version = "0.1.0"
const BaseURL = "https://portal.smsflow.co.za"

var phonePattern = regexp.MustCompile(`^\+[1-9][0-9]{6,14}$`)

type token struct {
	value       string
	expires     time.Time
	fingerprint [32]byte
}
type Provider struct {
	base   *url.URL
	client *http.Client
	mu     sync.Mutex
	tokens map[string]token
}

var _ channels.Provider = (*Provider)(nil)

func New() *Provider {
	p, _ := NewWithClient(BaseURL, &http.Client{Timeout: 20 * time.Second})
	return p
}

// NewWithClient allows a trusted deployment/test to choose its HTTPS endpoint.
// Do not expose endpoint selection as untrusted customer configuration.
func NewWithClient(base string, client *http.Client) (*Provider, error) {
	u, err := url.Parse(base)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || strings.Trim(u.Path, "/") != "" || client == nil {
		return nil, fmt.Errorf("SMSFlow endpoint must be an HTTPS origin")
	}
	clone := *client
	clone.Timeout = 20 * time.Second
	clone.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &Provider{base: u, client: &clone, tokens: map[string]token{}}, nil
}
func (*Provider) Descriptor() channels.Descriptor {
	return channels.Descriptor{ID: "smsflow", Name: "SMSFlow", Version: Version, APIVersion: channels.APIVersion, Channel: "sms"}
}
func fail(kind string) error { return &channels.ProviderError{Kind: kind} }

func (p *Provider) authenticate(ctx context.Context, c channels.Connection) (string, error) {
	id, secret := c.Credentials["client_id"], c.Credentials["client_secret"]
	if c.ID == "" || id == "" || secret == "" {
		return "", fail("unauthorized")
	}
	fp := sha256.Sum256([]byte(id + "\x00" + secret))
	p.mu.Lock()
	defer p.mu.Unlock()
	if t, ok := p.tokens[c.ID]; ok && t.fingerprint == fp && time.Now().Before(t.expires) {
		return t.value, nil
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.base.String()+"/api/integration/Authentication", nil)
	if err != nil {
		return "", fail("unavailable")
	}
	req.SetBasicAuth(id, secret)
	resp, err := p.client.Do(req)
	if err != nil {
		return "", fail("unavailable")
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fail("unauthorized")
	}
	var auth struct {
		Token            string
		ExpiresInMinutes int
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 64<<10)).Decode(&auth); err != nil || auth.Token == "" || auth.ExpiresInMinutes < 1 {
		return "", fail("unauthorized")
	}
	ttl := time.Duration(auth.ExpiresInMinutes) * time.Minute
	if ttl > 24*time.Hour {
		ttl = 24 * time.Hour
	}
	if ttl > 5*time.Minute {
		ttl -= time.Minute
	}
	// Bound credentials cache growth and discard stale entries opportunistically.
	for key, t := range p.tokens {
		if !time.Now().Before(t.expires) {
			delete(p.tokens, key)
		}
	}
	if len(p.tokens) >= 1024 {
		p.tokens = map[string]token{}
	}
	p.tokens[c.ID] = token{value: auth.Token, expires: time.Now().Add(ttl), fingerprint: fp}
	return auth.Token, nil
}

func (p *Provider) request(ctx context.Context, c channels.Connection, method, path string, body any, key string, out any) error {
	t, err := p.authenticate(ctx, c)
	if err != nil {
		return err
	}
	var data []byte
	if body != nil {
		data, err = json.Marshal(body)
		if err != nil {
			return fail("rejected")
		}
	}
	req, err := http.NewRequestWithContext(ctx, method, p.base.String()+path, bytes.NewReader(data))
	if err != nil {
		return fail("rejected")
	}
	req.Header.Set("Authorization", "Bearer "+t)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("X-SMSFlow-Connection-ID", c.ID)
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	resp, err := p.client.Do(req)
	if err != nil {
		if key != "" {
			return fail("unknown")
		}
		return fail("unavailable")
	}
	defer resp.Body.Close()
	if resp.StatusCode == 401 || resp.StatusCode == 403 {
		p.mu.Lock()
		delete(p.tokens, c.ID)
		p.mu.Unlock()
		return fail("unauthorized")
	}
	if resp.StatusCode == 402 {
		return fail("balance")
	}
	if resp.StatusCode == 409 {
		return fail("conflict")
	}
	if resp.StatusCode == 429 {
		return fail("rate_limited")
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		if key != "" && resp.StatusCode >= 500 {
			return fail("unknown")
		}
		return fail("unavailable")
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 2<<20)).Decode(out); err != nil {
		if key != "" {
			return fail("unknown")
		}
		return fail("incompatible")
	}
	return nil
}

func (p *Provider) Validate(ctx context.Context, c channels.Connection) (channels.ConnectionInfo, error) {
	var result struct {
		ContractVersion int                                                                                 `json:"contractVersion"`
		ConnectionID    string                                                                              `json:"connectionId"`
		Capabilities    struct{ Text, Inbound, DeliveryReports, Reconciliation, IdempotentSubmission bool } `json:"capabilities"`
		BalanceUnit     string                                                                              `json:"balanceUnit"`
		Currency        string                                                                              `json:"currency"`
	}
	err := p.request(ctx, c, http.MethodPost, "/api/integration/v2/connections/validate", map[string]string{"connectionId": c.ID}, "", &result)
	if err != nil {
		return channels.ConnectionInfo{}, err
	}
	caps := result.Capabilities
	if result.ContractVersion != 1 || result.ConnectionID != c.ID || !caps.Text || !caps.Reconciliation || !caps.IdempotentSubmission {
		return channels.ConnectionInfo{}, fail("incompatible")
	}
	var b struct{ Balance json.Number }
	if err := p.request(ctx, c, http.MethodGet, "/api/integration/Balance", nil, "", &b); err != nil {
		return channels.ConnectionInfo{}, err
	}
	unit := result.BalanceUnit
	if unit != "currency" && unit != "credits" {
		unit = "unknown"
	}
	return channels.ConnectionInfo{Capabilities: channels.Capabilities{Text: caps.Text, Inbound: caps.Inbound, DeliveryReports: caps.DeliveryReports, Reconciliation: caps.Reconciliation, IdempotentSubmission: caps.IdempotentSubmission}, Balance: &channels.Balance{Amount: b.Balance.String(), Unit: unit, Currency: result.Currency}}, nil
}

func (p *Provider) Send(ctx context.Context, c channels.Connection, m channels.Message) (channels.Submission, error) {
	limit := 3
	if raw := c.Config["max_segments"]; raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil {
			return channels.Submission{}, fail("rejected")
		}
		limit = parsed
	}
	if m.ClientMessageID == "" || len(m.ClientMessageID) > 128 || !phonePattern.MatchString(m.Destination) {
		return channels.Submission{}, fail("rejected")
	}
	if _, err := channels.PreviewSMS(m.Text, limit); err != nil {
		return channels.Submission{}, fail("rejected")
	}
	body := map[string]any{"sendOptions": map[string]any{"campaignName": "Whatomate", "checkOptOuts": true}, "messages": []any{map[string]any{"clientMessageId": m.ClientMessageID, "content": m.Text, "destination": m.Destination}}}
	var result struct {
		StatusCode   int
		SendResponse struct{ MessageReferences []string }
	}
	if err := p.request(ctx, c, http.MethodPost, "/api/integration/v2/messages", body, m.ClientMessageID, &result); err != nil {
		return channels.Submission{}, err
	}
	if result.StatusCode == 402 {
		return channels.Submission{}, fail("balance")
	}
	if result.StatusCode == 409 {
		return channels.Submission{}, fail("conflict")
	}
	if result.StatusCode < 200 || result.StatusCode >= 300 {
		return channels.Submission{}, fail("rejected")
	}
	if len(result.SendResponse.MessageReferences) != 1 || result.SendResponse.MessageReferences[0] == "" {
		return channels.Submission{}, fail("unknown")
	}
	return channels.Submission{ClientMessageID: m.ClientMessageID, ProviderMessageID: result.SendResponse.MessageReferences[0], Status: channels.StatusSubmitted}, nil
}

func (p *Provider) Reconcile(ctx context.Context, c channels.Connection, id string) (channels.Submission, error) {
	var found struct{ ClientMessageID, ProviderMessageID, Status, Code string }
	if err := p.request(ctx, c, http.MethodGet, "/api/integration/v2/messages/"+url.PathEscape(id)+"?connectionId="+url.QueryEscape(c.ID), nil, "", &found); err != nil {
		return channels.Submission{}, err
	}
	if found.ClientMessageID != id || !validStatus(found.Status) {
		return channels.Submission{}, fail("incompatible")
	}
	return channels.Submission{ClientMessageID: found.ClientMessageID, ProviderMessageID: found.ProviderMessageID, Status: found.Status, Code: boundedCode(found.Code)}, nil
}
func (p *Provider) Poll(ctx context.Context, c channels.Connection, cursor string) (channels.EventPage, error) {
	var result struct {
		Items []struct {
			EventID, Kind, ClientMessageID, ProviderMessageID, Status, Code, MobileNumber, Body string
			IsOptOut                                                                            bool
			OccurredAt                                                                          time.Time
		}
		NextCursor string
		HasMore    bool
	}
	path := "/api/integration/v2/events?connectionId=" + url.QueryEscape(c.ID) + "&after=" + url.QueryEscape(cursor) + "&take=100"
	if err := p.request(ctx, c, http.MethodGet, path, nil, "", &result); err != nil {
		return channels.EventPage{}, err
	}
	page := channels.EventPage{NextCursor: result.NextCursor, HasMore: result.HasMore, Events: make([]channels.Event, 0, len(result.Items))}
	if len(result.Items) > 100 || len(result.NextCursor) > 512 {
		return channels.EventPage{}, fail("incompatible")
	}
	for _, e := range result.Items {
		if e.EventID == "" || len(e.EventID) > 128 || e.ClientMessageID == "" || (e.Kind != "inbound" && e.Kind != "status") || (e.Kind == "status" && !validStatus(e.Status)) {
			return channels.EventPage{}, fail("incompatible")
		}
		page.Events = append(page.Events, channels.Event{ID: e.EventID, Kind: e.Kind, ClientMessageID: e.ClientMessageID, ProviderMessageID: e.ProviderMessageID, Status: e.Status, Code: boundedCode(e.Code), Phone: e.MobileNumber, Text: e.Body, OptOut: e.IsOptOut, OccurredAt: e.OccurredAt})
	}
	return page, nil
}
func validStatus(s string) bool {
	return s == channels.StatusQueued || s == channels.StatusSubmitted || s == channels.StatusDelivered || s == channels.StatusFailed || s == channels.StatusUnknown
}
func boundedCode(s string) string {
	if len(s) > 64 {
		return "provider_error"
	}
	for _, r := range s {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '_' || r == '-') {
			return "provider_error"
		}
	}
	return s
}
