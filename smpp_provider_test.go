package smsflow_test

import (
	"context"
	"encoding/json"
	"fmt"
	smsflow "github.com/SMSFlow-ZA/whatomate-smsflow"
	"github.com/shridarpatil/whatomate/pkg/channels"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

const connID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
const msgID = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"

var wireID = strings.ReplaceAll(connID, "-", "") + strings.ReplaceAll(msgID, "-", "")

func conn() channels.Connection {
	return channels.Connection{ID: connID, Credentials: channels.Credentials{"client_id": "fixture-id", "client_secret": "private-secret"}}
}
func msg() channels.Message {
	return channels.Message{ClientMessageID: msgID, Destination: "+27820000000", Text: "Hello", CheckOptOuts: true}
}

type fixture struct {
	auth, sends atomic.Int32
	mode        string
}

func (f *fixture) handler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	switch r.URL.Path {
	case "/api/oauth/token":
		f.auth.Add(1)
		id, secret, ok := r.BasicAuth()
		_ = r.ParseForm()
		if !ok || id != "fixture-id" || secret != "private-secret" || r.Method != "POST" || r.FormValue("grant_type") != "client_credentials" {
			http.Error(w, "private-secret", 401)
			return
		}
		fmt.Fprint(w, `{"access_token":"fixture-token","token_type":"Bearer","expires_in":3600}`)
	case "/api/smpp-gateway/v1/registration/validate":
		if r.Header.Get("Authorization") != "Bearer fixture-token" {
			w.WriteHeader(401)
			return
		}
		if f.mode == "invalid" {
			fmt.Fprint(w, `{"isValid":false,"errors":[{"message":"private-secret"}]}`)
			return
		}
		fmt.Fprint(w, `{"isValid":true,"smppAccountId":"fixture-id","channelType":"Marketing","billingType":"Prepaid","remainingBalance":100,"allowedBindModes":["Transceiver"],"errors":[]}`)
	case "/api/smpp-gateway/v1/messages/submit":
		f.sends.Add(1)
		var body struct {
			Messages []struct {
				SmppMessageID, SystemID, DestinationAddress, Body string
				RegisteredDelivery                                int
			}
		}
		if json.NewDecoder(r.Body).Decode(&body) != nil || len(body.Messages) != 1 || body.Messages[0].SmppMessageID != wireID || body.Messages[0].RegisteredDelivery != 1 || r.Header.Get("Idempotency-Key") != "" {
			http.Error(w, "bad contract", 400)
			return
		}
		switch f.mode {
		case "lost":
			w.WriteHeader(503)
			fmt.Fprint(w, `{"statusCode":503,"failures":[{"code":"SUBMISSION_OUTCOME_UNKNOWN","message":"private-secret"}]}`)
		case "missing":
			fmt.Fprint(w, `{"statusCode":200,"results":[]}`)
		case "unknown":
			fmt.Fprint(w, `{"statusCode":207,"results":[{"smppMessageId":"`+wireID+`","accepted":false,"code":"SUBMISSION_OUTCOME_UNKNOWN"}],"failures":[{"smppMessageId":"`+wireID+`","accepted":false,"code":"SUBMISSION_OUTCOME_UNKNOWN"}]}`)
		case "balance":
			fmt.Fprint(w, `{"statusCode":402,"failures":[{"code":"INSUFFICIENT_CREDITS"}]}`)
		case "reject":
			fmt.Fprint(w, `{"statusCode":207,"results":[{"smppMessageId":"`+wireID+`","accepted":false,"code":"INVALID_DESTINATION","message":"private-secret"}],"failures":[{"smppMessageId":"`+wireID+`","accepted":false,"code":"INVALID_DESTINATION","message":"private-secret"}]}`)
		default:
			fmt.Fprint(w, `{"statusCode":200,"acceptedCount":1,"results":[{"smppMessageId":"`+wireID+`","accepted":true,"code":"ACCEPTED"}]}`)
		}
	case "/api/smpp-gateway/v1/reporting/statuses":
		var b struct{ AfterEventID int64 }
		_ = json.NewDecoder(r.Body).Decode(&b)
		if f.mode == "pages" {
			if b.AfterEventID == 0 {
				items := make([]map[string]any, 100)
				for i := range items {
					items[i] = map[string]any{"eventId": i + 1, "smppMessageId": strings.Repeat("c", 64), "status": "DELIVERED"}
				}
				_ = json.NewEncoder(w).Encode(map[string]any{"lastEventId": 100, "items": items})
			} else {
				fmt.Fprint(w, `{"lastEventId":101,"items":[{"eventId":101,"smppMessageId":"`+wireID+`","status":"DELIVERED"}]}`)
			}
			return
		}
		if b.AfterEventID == 0 {
			fmt.Fprint(w, `{"lastEventId":2,"items":[{"eventId":1,"smppMessageId":"`+strings.Repeat("c", 64)+`","status":"DELIVERED"},{"eventId":2,"smppMessageId":"`+wireID+`","status":"DELIVERED","statusDateTimeUtc":"2026-10-19T12:00:00Z"}]}`)
		} else {
			fmt.Fprint(w, `{"lastEventId":2,"items":[]}`)
		}
	case "/api/smpp-gateway/v1/reporting/replies":
		var b struct{ AfterEventID int64 }
		_ = json.NewDecoder(r.Body).Decode(&b)
		if b.AfterEventID == 0 {
			fmt.Fprint(w, `{"lastEventId":1,"items":[{"eventId":1,"smppMessageId":"`+wireID+`","reply":"STOP","isOptOut":true,"mobileNumber":"27820000000","receivedDateTimeUtc":"2026-10-19T12:00:01Z"}]}`)
		} else {
			fmt.Fprint(w, `{"lastEventId":1,"items":[]}`)
		}
	default:
		http.Error(w, "Retired API must not be called", 404)
	}
}

func TestForeignPagesAdvanceIndependentCursorsWithoutLeakingEvents(t *testing.T) {
	f := &fixture{mode: "pages"}
	s := httptest.NewTLSServer(http.HandlerFunc(f.handler))
	defer s.Close()
	p, _ := smsflow.NewWithClient(s.URL, s.Client())
	first, err := p.Poll(context.Background(), conn(), "")
	if err != nil || !first.HasMore || len(first.Events) != 1 || first.Events[0].Kind != "inbound" {
		t.Fatalf("first %+v %v", first, err)
	}
	second, err := p.Poll(context.Background(), conn(), first.NextCursor)
	if err != nil || len(second.Events) != 1 || second.Events[0].Kind != "status" || second.Events[0].ClientMessageID != msgID {
		t.Fatalf("second %+v %v", second, err)
	}
	if f.sends.Load() != 0 {
		t.Fatal("Polling submitted SMS")
	}
}
func TestExistingSMPPContractAndConnectionScopedRestartSafeReports(t *testing.T) {
	f := &fixture{}
	s := httptest.NewTLSServer(http.HandlerFunc(f.handler))
	defer s.Close()
	p, err := smsflow.NewWithClient(s.URL, s.Client())
	if err != nil {
		t.Fatal(err)
	}
	info, err := p.Validate(context.Background(), conn())
	if err != nil {
		t.Fatal(err)
	}
	if !info.Capabilities.Text || !info.Capabilities.DeliveryReports || info.Capabilities.Inbound || info.Capabilities.IdempotentSubmission || info.Capabilities.Reconciliation || info.Balance == nil || info.Balance.Amount != "100" {
		t.Fatalf("Untruthful transport capabilities: %+v", info)
	}
	result, err := p.Send(context.Background(), conn(), msg())
	if err != nil || result.Status != "submitted" || result.ProviderMessageID != wireID {
		t.Fatalf("submit %+v %v", result, err)
	}
	page, err := p.Poll(context.Background(), conn(), "")
	if err != nil || len(page.Events) != 2 {
		t.Fatalf("events %+v %v", page, err)
	}
	if page.Events[0].ClientMessageID != msgID || page.Events[0].Status != "delivered" || page.Events[1].Text != "STOP" || !page.Events[1].OptOut || page.Events[0].ID == page.Events[1].ID {
		t.Fatalf("events %+v", page.Events)
	}
	restarted, _ := smsflow.NewWithClient(s.URL, s.Client())
	next, err := restarted.Poll(context.Background(), conn(), page.NextCursor)
	if err != nil || len(next.Events) != 0 || next.NextCursor != page.NextCursor || f.sends.Load() != 1 || f.auth.Load() != 2 {
		t.Fatalf("restart %+v %v", next, err)
	}
}

func TestInboundCapabilityRequiresTrustedVerifiedRoute(t *testing.T) {
	f := &fixture{}
	s := httptest.NewTLSServer(http.HandlerFunc(f.handler))
	defer s.Close()
	for _, routes := range [][]string{{"Other"}, {"Marketing"}} {
		p, err := smsflow.NewWithReplyRoutes(s.URL, s.Client(), routes)
		if err != nil {
			t.Fatal(err)
		}
		info, err := p.Validate(context.Background(), conn())
		if err != nil || info.Capabilities.Inbound != (routes[0] == "Marketing") || f.sends.Load() != 0 {
			t.Fatalf("unsafe route capability %+v %v", info, err)
		}
	}
}
func TestUnknownSubmissionIsNeverResentByRecovery(t *testing.T) {
	for _, mode := range []string{"lost", "missing", "unknown"} {
		t.Run(mode, func(t *testing.T) {
			f := &fixture{mode: mode}
			s := httptest.NewTLSServer(http.HandlerFunc(f.handler))
			defer s.Close()
			p, _ := smsflow.NewWithClient(s.URL, s.Client())
			_, err := p.Send(context.Background(), conn(), msg())
			pe, ok := err.(*channels.ProviderError)
			if !ok || pe.Kind != "unknown" || strings.Contains(err.Error(), "private-secret") {
				t.Fatalf("unsafe error %v", err)
			}
			restarted, _ := smsflow.NewWithClient(s.URL, s.Client())
			unknown, err := restarted.Reconcile(context.Background(), conn(), msgID)
			if err != nil || unknown.Status != "unknown" || f.sends.Load() != 1 {
				t.Fatalf("unsafe recovery %+v %v", unknown, err)
			}
			page, err := restarted.Poll(context.Background(), conn(), "")
			if err != nil || page.Events[0].Status != "delivered" || f.sends.Load() != 1 {
				t.Fatalf("late report %+v %v", page, err)
			}
		})
	}
}
func TestCreditAndValidationErrorsDoNotExposeProviderBodies(t *testing.T) {
	for _, tc := range []struct{ mode, kind string }{{"balance", "balance"}, {"reject", "rejected"}} {
		t.Run(tc.mode, func(t *testing.T) {
			f := &fixture{mode: tc.mode}
			s := httptest.NewTLSServer(http.HandlerFunc(f.handler))
			defer s.Close()
			p, _ := smsflow.NewWithClient(s.URL, s.Client())
			_, err := p.Send(context.Background(), conn(), msg())
			pe, ok := err.(*channels.ProviderError)
			if !ok || pe.Kind != tc.kind || strings.Contains(err.Error(), "private-secret") || f.sends.Load() != 1 {
				t.Fatalf("unsafe result %v", err)
			}
		})
	}
	f := &fixture{mode: "invalid"}
	s := httptest.NewTLSServer(http.HandlerFunc(f.handler))
	defer s.Close()
	p, _ := smsflow.NewWithClient(s.URL, s.Client())
	_, err := p.Validate(context.Background(), conn())
	if err == nil || strings.Contains(err.Error(), "private-secret") || f.sends.Load() != 0 {
		t.Fatalf("unsafe validation %v", err)
	}
}
func TestLocalLimitsConsentAndTLS(t *testing.T) {
	if _, err := smsflow.NewWithClient("http://example.com", http.DefaultClient); err == nil {
		t.Fatal("Plaintext endpoint")
	}
	f := &fixture{}
	s := httptest.NewTLSServer(http.HandlerFunc(f.handler))
	defer s.Close()
	p, _ := smsflow.NewWithClient(s.URL, s.Client())
	for _, text := range []string{strings.Repeat("A", 460), strings.Repeat("😀", 101)} {
		m := msg()
		m.Text = text
		if _, err := p.Send(context.Background(), conn(), m); err == nil {
			t.Fatal("Segment limit ignored")
		}
	}
	m := msg()
	m.CheckOptOuts = false
	if _, err := p.Send(context.Background(), conn(), m); err == nil {
		t.Fatal("Missing consent accepted")
	}
	if f.sends.Load() != 0 {
		t.Fatal("Invalid content submitted")
	}
	if _, err := p.Poll(context.Background(), conn(), "smpp1:foreign:1:2"); err == nil {
		t.Fatal("Foreign cursor accepted")
	}
}
func TestCredentialsNeverFollowRedirect(t *testing.T) {
	var leaked atomic.Int32
	target := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { leaked.Add(1) }))
	defer target.Close()
	redirect := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, target.URL, 307) }))
	defer redirect.Close()
	p, _ := smsflow.NewWithClient(redirect.URL, redirect.Client())
	_, _ = p.Validate(context.Background(), conn())
	if leaked.Load() != 0 {
		t.Fatal("Credentials redirected")
	}
}
