# hermes-max-adapter

A platform plugin that connects a **[MAX](https://max.ru) (max.ru)** messenger bot to
the [Hermes Agent](https://github.com/) gateway — the same ergonomics as the built-in
Telegram long-polling adapter.

- **Inbound** via long polling (`GET /updates`) — **no public URL / webhook required**.
- **Outbound** via the MAX Bot REST API (`POST /messages`).
- **No external SDK** — uses `httpx`, already a Hermes dependency.
- Verified against the official Go client
  ([max-messenger/max-bot-api-client-go](https://github.com/max-messenger/max-bot-api-client-go))
  and the TamTam-derived REST API documented at [dev.max.ru](https://dev.max.ru/).

MAX is a Russian messenger; its Bot API is issued via **@MasterBot**.

## Features

| Direction | Supported |
|---|---|
| Inbound text | ✅ |
| Inbound attachments | ✅ image · video · audio · file · sticker (downloaded from the CDN `url` into the Hermes media cache and passed to the agent) |
| Outbound text | ✅ plain or `format=markdown` |
| Outbound media | ✅ upload (`POST /uploads`) → attach |
| Typing indicator | ✅ `typing_on` |
| Inline-keyboard approvals | ✅ gated-command approval prompts with Allow / Session / Always / Deny buttons |
| User allowlist | ✅ by numeric `user_id` |

Inbound `audio` attachments are mapped to `MessageType.VOICE` so the Hermes STT
pipeline auto-transcribes them when delivered (see the limitation below).

## Install

Clone (or copy) this repository into your Hermes plugins directory so the files
land at `~/.hermes/plugins/max/`:

```bash
git clone https://github.com/adobrotvorskiy/hermes-max-adapter.git ~/.hermes/plugins/max
```

Enable it in `~/.hermes/config.yaml`:

```yaml
gateway:
  platforms:
    max:
      enabled: true
      extra:
        token: "..."          # or set env MAX_TOKEN
        markdown: false
        poll_timeout: 30
        allowed_users: []     # empty = use env allowlist
        home_channel: "<chat_id>"
```

Get a bot token from **@MasterBot** in MAX, then restart the gateway:

```bash
systemctl --user restart hermes-gateway.service
```

## Configuration

All settings can come from `config.yaml` (`gateway.platforms.max.extra`) **or**
from environment variables (env overrides config):

| Env var | Default | Purpose |
|---|---|---|
| `MAX_TOKEN` | — (required) | Bot token from @MasterBot. Reissue with `/revoke` if leaked. |
| `MAX_API_BASE` | `https://platform-api.max.ru` | Override API base URL. |
| `MAX_API_VERSION` | omitted (latest) | Optional `?v=` API version on each request. |
| `MAX_MARKDOWN` | `false` | Send messages with `format=markdown`. |
| `MAX_POLL_TIMEOUT` | `30` | Long-poll timeout, seconds (0–90). |
| `MAX_ALLOWED_USERS` | — | Comma-separated numeric `user_id`s allowed to talk to the bot. |
| `MAX_ALLOW_ALL_USERS` | `false` | Allow anyone (dev only). |
| `MAX_HOME_CHANNEL` | — | Default `chat_id` for cron / notification delivery (`deliver=max`). |

The token is read from env/config only — **nothing is hardcoded**, and the token
is redacted from logs.

## API notes (verified)

- Base URL: `https://platform-api.max.ru/`
- Auth header: `Authorization: <raw token>` — **no `Bearer` prefix**.
- Inbound: `GET /updates` (marker + timeout long poll). No update-type
  subscription is required; the bot receives all updates by default.
- Outbound text: `POST /messages?chat_id=<id>` body `{text, format, notify}`.
- Typing: `POST /chats/{chatId}/actions` body `{action: "typing_on"}`.
- Attachments: `POST /uploads?type=<image|file|...>` → upload → attach.

Inbound attachment shapes match the official client's schema: `image`
(`payload {url, token, photo_id}`), `audio`/`video`/`file` (`payload {url, token}`,
`file` also carries `filename`/`size`), `sticker`, `contact`, `share`, `location`,
`inline_keyboard`.

## Known limitation — inbound voice messages

> **MAX does not deliver inbound voice notes (microphone recordings) to bots via
> long polling.**

This was verified live: text and photo messages arrive as `message_created`
updates, but a voice note produces **no update at all** from the server (the
message is not even marked as received). There is no separate "voice" attachment
type in the MAX API — a voice note would arrive as `type: "audio"`, which this
adapter already handles — but the server simply does not emit the update over
`GET /updates`. This is a platform behavior, not a client bug.

If you need inbound voice, possible avenues (untested here): switch the inbound
path from long polling to a **webhook** subscription (`POST /subscriptions`,
requires a public HTTPS endpoint), and/or check the bot's media settings in
@MasterBot.

## License

[MIT](LICENSE) © Aleksey Dobrotvorskiy
