# SMSFlow provider for Whatomate

AGPL-3.0 Go module for channel API 1. Version 0.2 uses the existing SMSFlow SMPP
integration's HTTPS submission and reporting contract. It shares authentication,
billing, routing and reporting with the SMPP gateway. Native SMPP-over-TCP is
outside this adapter's transport; no new listener or per-instance container is
required. No private SMSFlow implementation/package is included.

## Compatibility and account setup

Use a compatible Whatomate host with durable one-shot dispatch and receipt-driven
recovery. The V2-only provider 0.1 preview is superseded. This module calls no
`/api/integration/v2` endpoint and needs none of that preview's database tables.
See [the existing wire contract](docs/api-contract.md).

Customers supply an SMSFlow Client ID and Client Secret with SMPP integration
access enabled. SMS is charged to their own account; subscriptions do not include
SMS credit. The host encrypts credentials and restricts configuration to admins.
Validation checks the account and balance without sending SMS.

The production origin is `https://portal.smsflow.co.za`. Alternate origins are
trusted deployment/test configuration, never customer URLs. HTTPS is required and
credentials never follow redirects. Only deployment-verified reply-capable routes
should be passed to `NewWithReplyRoutes`; other accounts can send one-way SMS.

## Submission safety

The transport advertises `idempotent_submission=false` and no synchronous lookup.
The host commits one dispatch claim before `Send`. It must never submit an
uncertain or expired claim again. This adapter sets no `Idempotency-Key` and never
retries a submit automatically. Standalone callers need an equivalent durable
one-shot guard; the transport does not promise to replay an HTTP POST result.

Connection and message UUIDs form a stable 64-character SMPP message identifier.
Independent delivery/reply cursors survive host restart. Foreign connection
reports are filtered even when credentials are reused. Late reports can resolve
unknown submissions; missing reports leave them unknown for operator review.
A code rollback or database restore does not prove a send never happened.

Carrier timeouts/missing acknowledgements must not be automatically refunded or
resent. The existing backend's `SUBMISSION_OUTCOME_UNKNOWN` per-message result
preserves this distinction; definite validation failures remain rejected.

The host owns consent, suppression, contact permissions, the three-segment default,
fallback opt-in and history. SMS does not open WhatsApp's 24-hour window. Content
is never silently truncated.

## Verification

`go test -race ./...` and `go vet ./...` use local TLS fixtures. Coverage includes
the existing OAuth/SMPP contract, no-send validation, lost responses, credit/errors,
separate cursors, foreign reports, restart, segments, consent, redirects and truthful
capabilities. Controlled-host and carrier acceptance remain separate release gates.
Keep credentials, captures and customer data out of Git.
