# Standard SMSFlow channel API — contract version 1

This additive contract is required before real-account use. Existing callers
without channel headers keep their existing API behavior. Authentication is the
standard Basic Client ID/Client Secret exchange at
`GET /api/integration/Authentication`, yielding the Bearer token used below.

## Connection validation

`POST /api/integration/v2/connections/validate` with `{ "connectionId": "<uuid>" }`.
Return `contractVersion: 1`, the matching `connectionId`, and `capabilities`
containing `text`, `inbound`, `deliveryReports`, `reconciliation` and
`idempotentSubmission`. Return optional `balanceUnit` (`credits` or `currency`)
and `currency`. Balance itself is read using existing `GET /api/integration/Balance`.
Validation sends no SMS. Registration is idempotent and binds a dedicated API
credential to one connection; conflicting use must return HTTP 409. Credential
rotation must preserve that registered connection/account identity.

## Send

`POST /api/integration/v2/messages`, using the standard BulkMessages body,
Bearer authentication and headers:

```text
X-SMSFlow-Connection-ID: <registered connection UUID>
Idempotency-Key: <stable client message UUID>
```

```json
{
  "sendOptions": { "campaignName": "Whatomate", "checkOptOuts": true },
  "messages": [{ "clientMessageId": "<same UUID>", "content": "Hello", "destination": "+27820000000" }]
}
```

V1 channel submissions contain one message. The server durably reserves the
connection/key and a hash of the accepted payload before dispatch. Repetition
with the same payload returns the recorded outcome; a different payload returns
409. A pending/uncertain result must never initiate another downstream send.
Retain suppression checks, billing, existing route selection and authorization.
Do not silently trim content. Successful responses use the existing body with
`statusCode` and `sendResponse.messageReferences` (one stable message identifier).
The adapter checks application errors even when HTTP status is 200. The plugin
never falls back to the unversioned send URL: an older server can ignore unknown
headers there, so a versioned route is necessary for safe API rollback. The
versioned endpoint delegates to the existing standard billing/send handler.

## Reconciliation

`GET /api/integration/v2/messages/{clientMessageId}?connectionId={uuid}` returns
`clientMessageId`, `providerMessageId`, `status`, and optional safe `code`.
Statuses are `queued`, `submitted`, `delivered`, `failed`, or `unknown`.
Unknown includes an uncertain downstream outcome; it is not permission to send
again. No read receipt exists for SMS. Lookup is scoped to the authenticated key
and its registered connection, not a telephone-number search.

## Delivery and inbound events

`GET /api/integration/v2/events?connectionId={uuid}&after={opaqueCursor}&take=100`.
Return `items`, `nextCursor` and `hasMore`. Each item has `eventId`, `kind`
(`status` or `inbound`), `clientMessageId`, optional `providerMessageId`, `status`,
`code`, `mobileNumber`, `body`, `isOptOut` and UTC `occurredAt` as appropriate.

Events have stable identifiers and can be replayed. A cursor is scoped to its
connection, must not skip late-arriving events, and advances only over committed
records. The host atomically persists events and its next cursor. Unknown
correlations are quarantined, never routed by telephone number alone.

Polling interval is 30 seconds, with bounded exponential backoff on failures.
The server must identify an expired checkpoint explicitly; it cannot silently
discard missed replies. Read markers for other SMSFlow clients are independent.
Upstream carrier events require trusted, authenticated ingestion before they
become public API events. Existing SQL-integration routes are not this contract.
