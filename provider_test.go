package smsflow_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	smsflow "github.com/SMSFlow-ZA/whatomate-smsflow"
	"github.com/shridarpatil/whatomate/pkg/channels"
)

func TestStandardAPIAuthenticationSendAndConnectionScopedEvents(t *testing.T) {
	var auths, sends atomic.Int32
	s := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/api/integration/Authentication" {
			auths.Add(1)
			u, p, ok := r.BasicAuth()
			if !ok || u != "client" || p != "test-secret" {
				t.Error("credentials not passed using Basic auth")
			}
			fmt.Fprint(w, `{"token":"test-token","expiresInMinutes":60,"schema":"Bearer"}`)
			return
		}
		if r.Header.Get("Authorization") != "Bearer test-token" {
			t.Error("missing bearer authentication")
		}
		switch r.URL.Path {
		case "/api/integration/v2/connections/validate":
			fmt.Fprint(w, `{"contractVersion":1,"connectionId":"connection-1","capabilities":{"text":true,"inbound":true,"deliveryReports":true,"reconciliation":true,"idempotentSubmission":true}}`)
		case "/api/integration/Balance":
			fmt.Fprint(w, `{"balance":120.5}`)
		case "/api/integration/BulkMessages":
			sends.Add(1)
			if r.Header.Get("Idempotency-Key") != "message-1" || r.Header.Get("X-SMSFlow-Connection-ID") != "connection-1" {
				t.Error("missing durable correlation")
			}
			var body struct {
				SendOptions struct{ CheckOptOuts bool }
				Messages    []struct{ Content, Destination, ClientMessageID string }
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Error(err)
			}
			if !body.SendOptions.CheckOptOuts || len(body.Messages) != 1 || body.Messages[0].ClientMessageID != "message-1" || body.Messages[0].Content != "Hello" {
				t.Error("incorrect send payload")
			}
			fmt.Fprint(w, `{"statusCode":200,"sendResponse":{"messageReferences":["provider-1"]}}`)
		case "/api/integration/v2/messages/message-1":
			if r.URL.Query().Get("connectionId") != "connection-1" {
				t.Error("reconciliation must be scoped")
			}
			fmt.Fprint(w, `{"clientMessageId":"message-1","providerMessageId":"provider-1","status":"submitted"}`)
		case "/api/integration/v2/events":
			if r.URL.Query().Get("connectionId") != "connection-1" || r.URL.Query().Get("after") != "cursor-1" {
				t.Error("poll scope/cursor lost")
			}
			fmt.Fprint(w, `{"items":[{"eventId":"event-2","kind":"inbound","clientMessageId":"message-1","mobileNumber":"+27820000000","body":"Reply","isOptOut":false,"occurredAt":"2026-10-19T08:00:00Z"}],"nextCursor":"cursor-2","hasMore":false}`)
		default:
			t.Errorf("unexpected path %s", r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	defer s.Close()
	p, err := smsflow.NewWithClient(s.URL, s.Client())
	if err != nil {
		t.Fatal(err)
	}
	c := channels.Connection{ID: "connection-1", Credentials: channels.Credentials{"client_id": "client", "client_secret": "test-secret"}}
	info, err := p.Validate(context.Background(), c)
	if err != nil || !info.Capabilities.Inbound {
		t.Fatalf("validate %+v %v", info, err)
	}
	if sends.Load() != 0 {
		t.Fatal("validation sent a chargeable SMS")
	}
	sent, err := p.Send(context.Background(), c, channels.Message{ClientMessageID: "message-1", Destination: "+27820000000", Text: "Hello"})
	if err != nil || sent.Status != "submitted" || sent.ProviderMessageID != "provider-1" {
		t.Fatalf("send %+v %v", sent, err)
	}
	found, err := p.Reconcile(context.Background(), c, "message-1")
	if err != nil || found.ProviderMessageID != "provider-1" {
		t.Fatalf("reconcile %+v %v", found, err)
	}
	events, err := p.Poll(context.Background(), c, "cursor-1")
	if err != nil || events.NextCursor != "cursor-2" || len(events.Events) != 1 || events.Events[0].Text != "Reply" {
		t.Fatalf("poll %+v %v", events, err)
	}
	if auths.Load() != 1 || sends.Load() != 1 {
		t.Fatal("unnecessary authentication or duplicate submission")
	}
}

func TestApplicationErrorsAndUnknownSendOutcomesAreNeverRetried(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		status     int
		wantKind   string
	}{
		{"insufficient credit", `{"statusCode":402,"errors":["test-secret"]}`, 200, "balance"},
		{"ambiguous upstream failure", `test-secret`, 502, "unknown"},
		{"missing provider correlation", `{"statusCode":200,"sendResponse":{}}`, 200, "unknown"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var sends atomic.Int32
			s := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/api/integration/Authentication" {
					fmt.Fprint(w, `{"token":"test-token","expiresInMinutes":60}`)
					return
				}
				sends.Add(1)
				w.WriteHeader(tc.status)
				fmt.Fprint(w, tc.body)
			}))
			defer s.Close()
			p, err := smsflow.NewWithClient(s.URL, s.Client())
			if err != nil {
				t.Fatal(err)
			}
			_, err = p.Send(context.Background(), channels.Connection{ID: "c", Credentials: channels.Credentials{"client_id": "id", "client_secret": "test-secret"}}, channels.Message{ClientMessageID: "m", Destination: "+27820000000", Text: "Hello"})
			pe, ok := err.(*channels.ProviderError)
			if !ok || pe.Kind != tc.wantKind {
				t.Fatalf("error=%v", err)
			}
			if strings.Contains(err.Error(), "test-secret") || sends.Load() != 1 {
				t.Fatal("secret leak or duplicate POST")
			}
		})
	}
}

func TestUnsecuredProviderURLRejected(t *testing.T) {
	if _, err := smsflow.NewWithClient("http://example.com", http.DefaultClient); err == nil {
		t.Fatal("credentials could be sent over plaintext HTTP")
	}
}
