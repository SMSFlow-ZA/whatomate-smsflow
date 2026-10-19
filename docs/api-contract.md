# Existing SMSFlow SMPP integration contract

The adapter uses the HTTPS contract already used by the SMSFlow SMPP gateway.
It introduces no sending endpoint and includes no private server source.
Use an SMPP-enabled API credential over a trusted HTTPS origin.

## Authentication and validation

`POST /api/oauth/token`, HTTP Basic Client ID/Client Secret, form body
`grant_type=client_credentials`. Read `access_token`, `token_type: Bearer` and
`expires_in` seconds. Tokens are cached by connection/credential fingerprint.
A rejected token is discarded; a submit is not automatically repeated.

`POST /api/smpp-gateway/v1/registration/validate`, Bearer auth:

```json
{"gatewayVersion":"whatomate-0.2.0","systemId":"CLIENT_ID","requestedBindMode":"Transceiver"}
```

Read `isValid`, `smppAccountId`, `channelType`, `billingType`, `remainingBalance`
and `errors`. The account must match the credential. Validation sends no SMS.
Prepaid balances are credits; postpaid balances are currency without an invented
currency code or conversion. Inbound additionally requires trusted verification
of the route; a transceiver bind mode alone does not prove replies are available.

## Submit once

`POST /api/smpp-gateway/v1/messages/submit`, Bearer auth:

```json
{
  "sendOptions":{"batchName":"Whatomate"},
  "messages":[{
    "smppMessageId":"CONNECTION_UUID_WITHOUT_DASHES_AND_MESSAGE_UUID_WITHOUT_DASHES",
    "systemId":"CLIENT_ID",
    "destinationAddress":"+27820000000",
    "destinationTon":1,"destinationNpi":1,
    "body":"Example message","dataCoding":0,"registeredDelivery":1,
    "clientReference":"SAME_NAMESPACED_IDENTIFIER"
  }]
}
```

The wire identifier is exactly 64 lowercase hexadecimal characters. UTF-16 uses
dataCoding 8; GSM uses 0. The host authorizes the recipient and consent/opt-out
state before dispatch. Content is never trimmed.

Read `statusCode`, `results` and `failures`, matching `smppMessageId`. Failed
items can appear identically in both arrays; contradictions are unknown. HTTP or
application 402 means insufficient credit. An accepted matching item is submitted,
not delivered. A lost response, missing item or `SUBMISSION_OUTCOME_UNKNOWN` is
unknown and must never trigger another submit. Read per-item outcomes on HTTP207.
No idempotency header is sent; transport replay is not assumed.

## Poll delivery and replies

`POST /api/smpp-gateway/v1/reporting/statuses` and
`POST /api/smpp-gateway/v1/reporting/replies`, Bearer auth, each with independent
`{"afterEventId":0,"take":100}`. Read `lastEventId` and `items`. The persisted
adapter cursor binds both numeric checkpoints to the connection UUID.

Status items supply `eventId`, `smppMessageId`, `status`/`rawStatus`/`smppStat`
and `statusDateTimeUtc`. Reply items supply `eventId`, `smppMessageId`, `reply`,
`mobileNumber`, `isOptOut` and `receivedDateTimeUtc`. Unknown statuses are skipped
while the cursor advances; they never become terminal failure. Status and reply
event identifiers use distinct namespaces.

Only messages with this connection's identifier prefix are returned to the host.
Foreign items still advance the cursor to prevent starvation. The host commits
events and cursor atomically, deduplicates and checks current scope. Replies do
not update WhatsApp's session window. Two-way routes are required for reply bots.

`Reconcile` returns unknown without resubmission because this contract has no
direct client-ID lookup. Durable polling resolves late reports. Restore with
dispatch held, retain attempts/cursors and reconcile unknown work before resuming.
Never delete an unknown attempt to force another send.
