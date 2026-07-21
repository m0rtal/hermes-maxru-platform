# Hermes MAX.ru Platform Plugin

Plugin-platform adapter for [MAX.ru](https://max.ru) messenger Bot API.

## Why a plugin (not core PR)?

This adapter is distributed as a Hermes **plugin platform**. It lives in
`~/.hermes/plugins/platforms/maxru/` and requires **zero changes to core Hermes**.
This means `hermes update` will not break the integration, and the code can be
maintained independently — useful when upstream PRs may be delayed or rejected.

## Installation

```bash
# Clone into Hermes plugins directory
git clone https://github.com/m0rtal/hermes-maxru-platform.git \
  ~/.hermes/plugins/platforms/maxru

# Or use the install script
curl -fsSL https://raw.githubusercontent.com/m0rtal/hermes-maxru-platform/main/install.sh | bash
```

Restart the Hermes gateway:

```bash
hermes gateway restart
```

## Configuration

Add to `~/.hermes/.env`:

```bash
MAXRU_TOKEN=your_bot_token_from_master_bot
MAXRU_API_URL=https://platform-api2.max.ru
MAXRU_ALLOWED_USERS=1234567890,0987654321
MAXRU_HOME_CHANNEL=1234567890
MAXRU_LONG_POLLING=true
```

Or add a `maxru` section to `~/.hermes/config.yaml`:

```yaml
maxru:
  token: "your_bot_token"
  api_url: "https://platform-api2.max.ru"
  allowed_users: "1234567890"
  home_channel: "1234567890"
  long_polling: true
  update_timeout: 30
  rps_limit: 25
```

## Modes

### Long Polling (development / testing)

Set `MAXRU_LONG_POLLING=true`. The adapter calls `GET /updates` in a loop.
This is limited by MAX and not recommended for production.

### Webhook (production)

Set `MAXRU_LONG_POLLING=false` and provide a public HTTPS URL:

```bash
MAXRU_WEBHOOK_URL=https://your-server.example.com/webhooks/maxru
MAXRU_WEBHOOK_SECRET=optional-shared-secret
```

The adapter registers the webhook on `connect()`. Your HTTPS server must:
- support TLS with a certificate from a trusted CA (self-signed certs are
  rejected by MAX as of 2026-05-25)
- route `POST /webhooks/maxru` to `MaxruAdapter.handle_webhook_update()`

## Features

- [x] Inbound text messages
- [x] `bot_started` events / deep link payload
- [x] Callback buttons → routed as `[callback:payload]` text
- [x] Outbound text messages
- [x] Outbound images (by URL or local file)
- [x] Outbound files/documents
- [x] `clarify` tool rendered as inline keyboard
- [x] Home-channel cron delivery
- [x] Built-in rate limiting (default 25 rps)
- [x] Allowlist / allow-all auth

## Not supported

- Typing indicator (MAX API does not expose it)
- Voice messages as native audio (delivered as file)
- Group chats / channels (requires `bot_added` subscription handling)

## API Reference

https://dev.max.ru/docs-api

## License

MIT
