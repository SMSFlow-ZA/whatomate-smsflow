# SMSFlow provider for Whatomate

AGPL-3.0 provider module implementing channel API version 1 from the public
Whatomate fork. Providers are compiled into the application, not uploaded or
executed dynamically. No private SMSFlow source or billing implementation is
included.

## Development status

The adapter is covered by local HTTPS contract tests. Real-account enablement
requires the additive SMSFlow standard channel API described in
[the contract](docs/api-contract.md). It must not be enabled against an older API
that silently ignores idempotency headers. Connection validation requires the
versioned contract, durable idempotency and reconciliation before a Whatomate
host enables sending. One-way routes can send; replies and automatic reply bots
require a verified reply-capable route.

Each customer supplies their own SMSFlow Client ID and Client Secret. Their
existing SMSFlow account pays for SMS. Credentials must be encrypted by the host
and are never returned to browsers. Ordinary agents do not manage credentials.
Production uses `https://portal.smsflow.co.za`; alternate endpoints are trusted
server configuration only. Redirects are not followed with credentials.

The host owns durable queueing, consent/opt-out checks, the three-segment default,
fallback policy, cursor checkpoints and conversation history. Validate the
connection before sending. A network timeout during submission is **unknown**;
reconcile the same client identifier instead of sending another message.

## Build and test

```sh
go test ./...
go vet ./...
```

Tests use local TLS servers, no real credentials or chargeable SMS. The module
pins the public SDK fork while upstream channel proposal
[whatomate#601](https://github.com/shridarpatil/whatomate/issues/601) is discussed.
Changing the channel API major version requires a compatible provider release.

## Licence

GNU Affero General Public License version 3. Keep corresponding source and
notices with modified application releases. See [LICENSE](LICENSE).
