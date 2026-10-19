// Package smsflow adapts the existing SMSFlow SMPP integration's HTTPS contract.
// It has no private implementation dependencies and does not run an SMPP server.
package smsflow

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"github.com/shridarpatil/whatomate/pkg/channels"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

const Version = "0.2.0"
const BaseURL = "https://portal.smsflow.co.za"
const api = "/api/smpp-gateway/v1"

var phonePattern = regexp.MustCompile(`^\+[1-9][0-9]{6,14}$`)
var hexID = regexp.MustCompile(`^[a-f0-9]{32}$`)

type token struct {
	value       string
	expires     time.Time
	fingerprint [32]byte
}
type Provider struct {
	base        *url.URL
	client      *http.Client
	mu          sync.Mutex
	tokens      map[string]token
	replyRoutes map[string]bool
}

var _ channels.Provider = (*Provider)(nil)

func New() *Provider {
	p, _ := NewWithClient(BaseURL, &http.Client{Timeout: 20 * time.Second})
	return p
}
func NewWithClient(base string, client *http.Client) (*Provider, error) {
	return NewWithReplyRoutes(base, client, nil)
}

// ReplyRoutes is trusted deployment configuration: only routes whose two-way
// service has been verified should advertise inbound/bot support.
func NewWithReplyRoutes(base string, client *http.Client, routes []string) (*Provider, error) {
	u, err := url.Parse(base)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || strings.Trim(u.Path, "/") != "" || client == nil {
		return nil, fmt.Errorf("SMSFlow endpoint must be an HTTPS origin")
	}
	clone := *client
	clone.Timeout = 20 * time.Second
	clone.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	allowed := map[string]bool{}
	for _, route := range routes {
		if value := strings.ToLower(strings.TrimSpace(route)); value != "" {
			allowed[value] = true
		}
	}
	return &Provider{base: u, client: &clone, tokens: map[string]token{}, replyRoutes: allowed}, nil
}
func (*Provider) Descriptor() channels.Descriptor {
	return channels.Descriptor{ID: "smsflow", Name: "SMSFlow (SMPP integration)", Version: Version, APIVersion: channels.APIVersion, Channel: "sms"}
}
func fail(kind string) error { return &channels.ProviderError{Kind: kind} }
func normalizedID(id string) (string, bool) {
	raw := strings.ToLower(strings.ReplaceAll(id, "-", ""))
	return raw, hexID.MatchString(raw)
}
func wire(c channels.Connection, id string) (string, error) {
	a, ok := normalizedID(c.ID)
	b, valid := normalizedID(id)
	if !ok || !valid {
		return "", fail("rejected")
	}
	return a + b, nil
}
func clientID(c channels.Connection, id string) (string, bool) {
	prefix, ok := normalizedID(c.ID)
	raw := strings.ToLower(id)
	if !ok || len(raw) != 64 || !strings.HasPrefix(raw, prefix) || !hexID.MatchString(raw[32:]) {
		return "", false
	}
	s := raw[32:]
	return s[:8] + "-" + s[8:12] + "-" + s[12:16] + "-" + s[16:20] + "-" + s[20:], true
}

func (p *Provider) authenticate(ctx context.Context, c channels.Connection) (string, error) {
	id, secret := c.Credentials["client_id"], c.Credentials["client_secret"]
	if _, valid := normalizedID(c.ID); !valid || id == "" || secret == "" {
		return "", fail("unauthorized")
	}
	fp := sha256.Sum256([]byte(id + "\x00" + secret))
	p.mu.Lock()
	defer p.mu.Unlock()
	if cached, ok := p.tokens[c.ID]; ok && cached.fingerprint == fp && time.Now().Before(cached.expires) {
		return cached.value, nil
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.base.String()+"/api/oauth/token", strings.NewReader("grant_type=client_credentials"))
	if err != nil {
		return "", fail("unavailable")
	}
	req.SetBasicAuth(id, secret)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := p.client.Do(req)
	if err != nil {
		return "", fail("unavailable")
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fail("unauthorized")
	}
	var auth struct {
		AccessToken string `json:"access_token"`
		TokenType   string `json:"token_type"`
		Expires     int    `json:"expires_in"`
	}
	if json.NewDecoder(io.LimitReader(resp.Body, 64<<10)).Decode(&auth) != nil || auth.AccessToken == "" || auth.Expires < 1 || !strings.EqualFold(auth.TokenType, "Bearer") {
		return "", fail("unauthorized")
	}
	ttl := time.Duration(min(auth.Expires, 86400)) * time.Second
	if ttl > time.Minute {
		ttl -= 30 * time.Second
	}
	for key, cached := range p.tokens {
		if !time.Now().Before(cached.expires) {
			delete(p.tokens, key)
		}
	}
	if len(p.tokens) >= 1024 {
		p.tokens = map[string]token{}
	}
	p.tokens[c.ID] = token{value: auth.AccessToken, expires: time.Now().Add(ttl), fingerprint: fp}
	return auth.AccessToken, nil
}
func (p *Provider) request(ctx context.Context, c channels.Connection, path string, body any, submission bool, out any) error {
	bearer, err := p.authenticate(ctx, c)
	if err != nil {
		return err
	}
	data, err := json.Marshal(body)
	if err != nil {
		return fail("rejected")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.base.String()+api+path, bytes.NewReader(data))
	if err != nil {
		return fail("rejected")
	}
	req.Header.Set("Authorization", "Bearer "+bearer)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	// The existing transport does not promise POST replay. Do not set an
	// Idempotency-Key header or install a retrying RoundTripper for submissions.
	resp, err := p.client.Do(req)
	if err != nil {
		if submission {
			return fail("unknown")
		}
		return fail("unavailable")
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case 401, 403:
		p.mu.Lock()
		delete(p.tokens, c.ID)
		p.mu.Unlock()
		return fail("unauthorized")
	case 402:
		return fail("balance")
	case 429:
		return fail("rate_limited")
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		if submission && resp.StatusCode >= 500 {
			return fail("unknown")
		}
		return fail("unavailable")
	}
	if json.NewDecoder(io.LimitReader(resp.Body, 2<<20)).Decode(out) != nil {
		if submission {
			return fail("unknown")
		}
		return fail("incompatible")
	}
	return nil
}
func (p *Provider) Validate(ctx context.Context, c channels.Connection) (channels.ConnectionInfo, error) {
	var result struct {
		IsValid                                 bool
		SmppAccountID, ChannelType, BillingType string
		RemainingBalance                        json.Number
		Errors                                  []json.RawMessage
		AllowedBindModes                        []string
	}
	err := p.request(ctx, c, "/registration/validate", map[string]any{"gatewayVersion": "whatomate-" + Version, "systemId": c.Credentials["client_id"], "requestedBindMode": "Transceiver"}, false, &result)
	if err != nil {
		return channels.ConnectionInfo{}, err
	}
	if !result.IsValid || result.SmppAccountID != c.Credentials["client_id"] || result.ChannelType == "" || len(result.Errors) != 0 || result.RemainingBalance.String() == "" {
		return channels.ConnectionInfo{}, fail("unauthorized")
	}
	unit := "unknown"
	if strings.EqualFold(result.BillingType, "Prepaid") {
		unit = "credits"
	} else if strings.EqualFold(result.BillingType, "Postpaid") {
		unit = "currency"
	}
	return channels.ConnectionInfo{Capabilities: channels.Capabilities{Text: true, DeliveryReports: true, Inbound: p.replyRoutes[strings.ToLower(result.ChannelType)], IdempotentSubmission: false, Reconciliation: false}, Balance: &channels.Balance{Amount: result.RemainingBalance.String(), Unit: unit}}, nil
}
func (p *Provider) Send(ctx context.Context, c channels.Connection, m channels.Message) (channels.Submission, error) {
	id, err := wire(c, m.ClientMessageID)
	if err != nil {
		return channels.Submission{}, err
	}
	limit := 3
	if value := c.Config["max_segments"]; value != "" {
		parsed, err := strconv.Atoi(value)
		if err != nil {
			return channels.Submission{}, fail("rejected")
		}
		limit = parsed
	}
	preview, err := channels.PreviewSMS(m.Text, limit)
	if err != nil || !phonePattern.MatchString(m.Destination) || !m.CheckOptOuts {
		return channels.Submission{}, fail("rejected")
	}
	coding := 0
	if preview.Encoding != "GSM-7" {
		coding = 8
	}
	body := map[string]any{"sendOptions": map[string]string{"batchName": "Whatomate"}, "messages": []any{map[string]any{"smppMessageId": id, "systemId": c.Credentials["client_id"], "destinationAddress": m.Destination, "destinationTon": 1, "destinationNpi": 1, "body": m.Text, "dataCoding": coding, "registeredDelivery": 1, "clientReference": id}}}
	type item struct {
		SmppMessageID string
		Accepted      bool
		Code          string
	}
	var result struct {
		StatusCode        int
		Results, Failures []item
	}
	if err := p.request(ctx, c, "/messages/submit", body, true, &result); err != nil {
		return channels.Submission{}, err
	}
	if result.StatusCode == 402 {
		return channels.Submission{}, fail("balance")
	}
	if result.StatusCode >= 500 {
		return channels.Submission{}, fail("unknown")
	}
	if result.StatusCode < 200 || result.StatusCode >= 300 {
		return channels.Submission{}, fail("rejected")
	}
	matches := []item{}
	seen := map[item]bool{}
	for _, entry := range append(result.Results, result.Failures...) {
		// The existing gateway also includes failed results in Failures.
		// Identical duplicates are one outcome; contradictions remain unknown.
		if entry.SmppMessageID == id && !seen[entry] {
			matches = append(matches, entry)
			seen[entry] = true
		}
	}
	if len(matches) != 1 {
		return channels.Submission{}, fail("unknown")
	}
	if !matches[0].Accepted {
		if matches[0].Code == "SUBMISSION_OUTCOME_UNKNOWN" || matches[0].Code == "MISSING_PROVIDER_RESULT" {
			return channels.Submission{}, fail("unknown")
		}
		return channels.Submission{}, fail("rejected")
	}
	return channels.Submission{ClientMessageID: m.ClientMessageID, ProviderMessageID: id, Status: channels.StatusSubmitted}, nil
}

// Recovery is receipt-driven. This transport has no lookup/replay endpoint;
// absence of a receipt never grants permission to submit again.
func (*Provider) Reconcile(_ context.Context, c channels.Connection, id string) (channels.Submission, error) {
	if _, err := wire(c, id); err != nil {
		return channels.Submission{}, err
	}
	return channels.Submission{ClientMessageID: id, Status: channels.StatusUnknown, Code: "awaiting_delivery_report"}, nil
}

type statusEvent struct {
	EventID                                    int64
	SmppMessageID, Status, RawStatus, SmppStat string
	StatusDateTimeUTC, IngestedUTC             time.Time
}
type replyEvent struct {
	EventID                            int64
	SmppMessageID, Reply, MobileNumber string
	IsOptOut                           bool
	ReceivedDateTimeUTC, IngestedUTC   time.Time
}

func parseCursor(c channels.Connection, cursor string) (int64, int64, error) {
	id, ok := normalizedID(c.ID)
	if !ok {
		return 0, 0, fail("incompatible")
	}
	if cursor == "" {
		return 0, 0, nil
	}
	parts := strings.Split(cursor, ":")
	if len(parts) != 4 || parts[0] != "smpp1" || parts[1] != id {
		return 0, 0, fail("incompatible")
	}
	a, e1 := strconv.ParseInt(parts[2], 10, 64)
	b, e2 := strconv.ParseInt(parts[3], 10, 64)
	if e1 != nil || e2 != nil || a < 0 || b < 0 {
		return 0, 0, fail("incompatible")
	}
	return a, b, nil
}
func (p *Provider) Poll(ctx context.Context, c channels.Connection, cursor string) (channels.EventPage, error) {
	a, b, err := parseCursor(c, cursor)
	if err != nil {
		return channels.EventPage{}, err
	}
	var statuses struct {
		LastEventID int64
		Items       []statusEvent
	}
	var replies struct {
		LastEventID int64
		Items       []replyEvent
	}
	if err = p.request(ctx, c, "/reporting/statuses", map[string]any{"afterEventId": a, "take": 100}, false, &statuses); err != nil {
		return channels.EventPage{}, err
	}
	if err = p.request(ctx, c, "/reporting/replies", map[string]any{"afterEventId": b, "take": 100}, false, &replies); err != nil {
		return channels.EventPage{}, err
	}
	if len(statuses.Items) > 100 || len(replies.Items) > 100 || statuses.LastEventID < a || replies.LastEventID < b {
		return channels.EventPage{}, fail("incompatible")
	}
	events := []channels.Event{}
	for _, event := range statuses.Items {
		if event.EventID <= 0 || event.EventID > statuses.LastEventID {
			return channels.EventPage{}, fail("incompatible")
		}
		if event.EventID <= a {
			continue
		}
		id, own := clientID(c, event.SmppMessageID)
		status := normalizeStatus(event.Status)
		if status == "" {
			status = normalizeStatus(event.RawStatus)
		}
		if status == "" {
			status = normalizeStatus(event.SmppStat)
		}
		if !own || status == "" {
			continue
		}
		events = append(events, channels.Event{ID: fmt.Sprintf("status:%d", event.EventID), Kind: "status", ClientMessageID: id, ProviderMessageID: event.SmppMessageID, Status: status, OccurredAt: event.StatusDateTimeUTC})
	}
	for _, event := range replies.Items {
		if event.EventID <= 0 || event.EventID > replies.LastEventID {
			return channels.EventPage{}, fail("incompatible")
		}
		if event.EventID <= b {
			continue
		}
		id, own := clientID(c, event.SmppMessageID)
		if !own {
			continue
		}
		phone := strings.TrimSpace(event.MobileNumber)
		if phone != "" && !strings.HasPrefix(phone, "+") {
			phone = "+" + phone
		}
		if !phonePattern.MatchString(phone) || event.Reply == "" || len(event.Reply) > 10000 {
			return channels.EventPage{}, fail("incompatible")
		}
		events = append(events, channels.Event{ID: fmt.Sprintf("reply:%d", event.EventID), Kind: "inbound", ClientMessageID: id, ProviderMessageID: event.SmppMessageID, Phone: phone, Text: event.Reply, OptOut: event.IsOptOut, OccurredAt: event.ReceivedDateTimeUTC})
	}
	id, _ := normalizedID(c.ID)
	return channels.EventPage{Events: events, NextCursor: fmt.Sprintf("smpp1:%s:%d:%d", id, statuses.LastEventID, replies.LastEventID), HasMore: len(statuses.Items) == 100 || len(replies.Items) == 100}, nil
}
func normalizeStatus(value string) string {
	switch strings.ToUpper(strings.TrimSpace(value)) {
	case "DELIVERED", "DELIVRD":
		return channels.StatusDelivered
	case "QUEUED", "PENDING", "ENROUTE", "SUBMITTED", "SUBMITD", "ACCEPTED", "SENT":
		return channels.StatusSubmitted
	case "FAILED", "UNDELIV", "UNDELIVERABLE", "EXPIRED", "DELETED", "REJECTED", "REJECTD", "CANNOT_SUBMIT", "NOROUTE", "BLIST":
		return channels.StatusFailed
	}
	return ""
}
