"""
MAX Platform Adapter for Hermes Agent.

A plugin-based gateway adapter that connects a MAX (max.ru) bot to the
Hermes agent.  Inbound messages arrive via long polling (``GET /updates``);
outbound messages go through the MAX Bot REST API (``POST /messages``).
Same ergonomics as the built-in Telegram long-polling adapter — no public
URL required.  No external SDK: uses ``httpx``, already a Hermes dependency.

MAX Bot API is the TamTam-derived REST API documented at https://dev.max.ru/.
Verified against the official Go client
(github.com/max-messenger/max-bot-api-client-go):

* Base URL          ``https://platform-api.max.ru/``
* Auth              ``Authorization: <raw token>`` header (no ``Bearer`` prefix)
* Optional ``?v=``  API version query param (omitted by default → latest)
* Inbound           ``GET /updates`` (marker + timeout long poll)
* Outbound text     ``POST /messages?chat_id=<id>``  body ``{text, format, notify}``
* Typing            ``POST /chats/{chatId}/actions``  body ``{action: "typing_on"}``
* Attachments       ``POST /uploads?type=<image|file|...>`` → upload → attach

Configuration in config.yaml::

    gateway:
      platforms:
        max:
          enabled: true
          extra:
            token: "..."          # or env MAX_TOKEN
            api_base: "https://platform-api.max.ru"
            markdown: false
            poll_timeout: 30
            allowed_users: []     # empty = use env allowlist
            home_channel: "<chat_id>"

Or via environment variables (override config.yaml):
    MAX_TOKEN, MAX_API_BASE, MAX_MARKDOWN, MAX_POLL_TIMEOUT,
    MAX_API_VERSION, MAX_ALLOWED_USERS, MAX_ALLOW_ALL_USERS, MAX_HOME_CHANNEL
"""

import asyncio
import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

try:
    import httpx
    HTTPX_AVAILABLE = True
except ImportError:  # pragma: no cover — httpx is a Hermes dependency
    httpx = None  # type: ignore[assignment]
    HTTPX_AVAILABLE = False

from gateway.platforms.base import (
    BasePlatformAdapter,
    SendResult,
    MessageEvent,
    MessageType,
    cache_image_from_bytes,
    cache_audio_from_bytes,
    cache_video_from_bytes,
    cache_document_from_bytes,
)
from gateway.config import Platform, PlatformConfig

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DEFAULT_API_BASE = "https://platform-api.max.ru"
# MAX caps message text at 4000 characters.
MAX_MESSAGE_LENGTH = 4000
# Long-poll defaults (server allows 0–90s; httpx read timeout adds a buffer).
DEFAULT_POLL_TIMEOUT = 30
POLL_READ_BUFFER = 20
# Reconnect backoff schedule (seconds) for the polling loop.
RECONNECT_BACKOFF = (1, 2, 5, 10, 15, 30)
# Bounded set of recently-seen message ids to drop duplicates on reconnect.
DEDUP_MAX_SIZE = 2000


def _truthy(value: Optional[str]) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


# ---------------------------------------------------------------------------
# MAX Adapter
# ---------------------------------------------------------------------------

class MaxAdapter(BasePlatformAdapter):
    """Async MAX adapter implementing the BasePlatformAdapter interface.

    Instantiated by the ``adapter_factory`` passed to ``register_platform()``.
    """

    MAX_MESSAGE_LENGTH = MAX_MESSAGE_LENGTH

    def __init__(self, config: PlatformConfig, **kwargs):
        super().__init__(config=config, platform=Platform("max"))

        extra = getattr(config, "extra", {}) or {}

        # Credentials & endpoint (env overrides config.yaml).
        self.token: str = (
            os.getenv("MAX_TOKEN")
            or extra.get("token")
            or getattr(config, "token", "")
            or ""
        ).strip()
        self.api_base: str = (
            os.getenv("MAX_API_BASE") or extra.get("api_base") or DEFAULT_API_BASE
        ).rstrip("/")
        # API version sent as ?v=; omitted by default (server uses latest).
        self.api_version: str = (
            os.getenv("MAX_API_VERSION") or extra.get("api_version") or ""
        ).strip()

        # Formatting. MAX-flavored markdown matches the CommonMark that Hermes
        # emits (**bold**, *italic*, ~~strike~~, `code`, [text](url), # head),
        # so default it ON — otherwise the raw asterisks show through. Disable
        # with MAX_MARKDOWN=false for plain-text-only bots.
        self.markdown: bool = (
            _truthy(os.getenv("MAX_MARKDOWN"))
            if os.getenv("MAX_MARKDOWN") is not None
            else bool(extra.get("markdown", True))
        )

        # Long-poll timeout (clamped to the server-allowed 0–90s window).
        try:
            self.poll_timeout: int = int(
                os.getenv("MAX_POLL_TIMEOUT") or extra.get("poll_timeout", DEFAULT_POLL_TIMEOUT)
            )
        except (TypeError, ValueError):
            self.poll_timeout = DEFAULT_POLL_TIMEOUT
        self.poll_timeout = max(0, min(90, self.poll_timeout))

        # Message length limit can be tuned via the platform registry entry.
        max_len = extra.get("max_message_length")
        if max_len is None:
            try:
                from gateway.platform_registry import platform_registry
                entry = platform_registry.get("max")
                if entry and entry.max_message_length:
                    max_len = entry.max_message_length
            except Exception:
                pass
        self.max_message_length = int(max_len or MAX_MESSAGE_LENGTH)

        # Runtime state.
        self._http_client: Optional["httpx.AsyncClient"] = None
        self._poll_task: Optional[asyncio.Task] = None
        self._marker: Optional[int] = None
        self._bot_user_id: Optional[int] = None
        self._seen_mids: Dict[str, float] = {}
        # Inline-keyboard approval state: approval_id -> session_key.
        self._approval_state: Dict[int, str] = {}
        self._approval_counter: int = 0

    @property
    def name(self) -> str:
        return "MAX"

    # ── Auth / request plumbing ───────────────────────────────────────────

    def _headers(self) -> Dict[str, str]:
        # MAX expects the raw token in the Authorization header (no Bearer).
        return {"Authorization": self.token}

    def _base_params(self) -> Dict[str, str]:
        return {"v": self.api_version} if self.api_version else {}

    def _safe(self, text: str) -> str:
        """Redact the bot token from any string before logging."""
        if self.token and text:
            return text.replace(self.token, "***")
        return text

    async def _request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        json_body: Optional[Dict[str, Any]] = None,
        timeout: float = 30.0,
    ) -> tuple[int, Any]:
        """Issue an authenticated MAX API call. Returns (status_code, parsed_body).

        On transport failure returns (0, error_string).  Never raises; never
        logs the token.
        """
        if not self._http_client:
            return 0, "HTTP client not initialized"
        url = f"{self.api_base}{path}"
        q = {**self._base_params(), **(params or {})}
        try:
            resp = await self._http_client.request(
                method, url, params=q, json=json_body,
                headers=self._headers(), timeout=timeout,
            )
        except httpx.TimeoutException:
            return 0, "timeout"
        except Exception as e:  # noqa: BLE001 — surface transport errors as data
            return 0, self._safe(str(e))
        try:
            body = resp.json()
        except Exception:
            body = resp.text
        return resp.status_code, body

    # ── Connection lifecycle ──────────────────────────────────────────────

    async def connect(self) -> bool:
        if not HTTPX_AVAILABLE:
            logger.error("MAX: httpx not installed. Run: pip install httpx")
            self._set_fatal_error("httpx_missing", "httpx not installed", retryable=False)
            return False
        if not self.token:
            logger.error("MAX: MAX_TOKEN not configured")
            self._set_fatal_error("config_missing", "MAX_TOKEN must be set", retryable=False)
            return False

        try:
            limits = None
            try:
                from gateway.platforms._http_client_limits import platform_httpx_limits
                limits = platform_httpx_limits()
            except Exception:
                pass
            self._http_client = (
                httpx.AsyncClient(limits=limits) if limits is not None else httpx.AsyncClient()
            )
        except Exception as e:
            logger.error("MAX: failed to create HTTP client: %s", self._safe(str(e)))
            self._set_fatal_error("client_init_failed", str(e), retryable=True)
            return False

        # Verify the token and learn our own user_id (to filter self-messages).
        status, body = await self._request("GET", "/me", timeout=20.0)
        if status == 401 or status == 403:
            logger.error("MAX: token rejected (HTTP %s) — check MAX_TOKEN", status)
            self._set_fatal_error("unauthorized", f"MAX rejected token (HTTP {status})", retryable=False)
            await self._close_client()
            return False
        if status != 200 or not isinstance(body, dict):
            logger.error("MAX: /me failed (HTTP %s): %s", status, str(body)[:200])
            self._set_fatal_error("me_failed", f"/me returned HTTP {status}", retryable=True)
            await self._close_client()
            return False

        self._bot_user_id = body.get("user_id")
        bot_name = body.get("name") or body.get("username") or "?"
        logger.info("MAX: connected as %s (user_id=%s)", bot_name, self._bot_user_id)

        # Mark connected BEFORE launching the loop so `while self._running`
        # is already True when the task first executes.
        self._mark_connected()
        self._poll_task = asyncio.create_task(self._run_poll())
        return True

    async def _close_client(self) -> None:
        if self._http_client:
            try:
                await self._http_client.aclose()
            except Exception:
                pass
            self._http_client = None

    async def disconnect(self) -> None:
        self._mark_disconnected()
        if self._poll_task and not self._poll_task.done():
            self._poll_task.cancel()
            try:
                await self._poll_task
            except asyncio.CancelledError:
                pass
        self._poll_task = None
        await self._close_client()
        self._seen_mids.clear()
        logger.info("MAX: disconnected")

    # ── Long-poll loop ────────────────────────────────────────────────────

    async def _run_poll(self) -> None:
        backoff_idx = 0
        while self._running:
            loop_start = asyncio.get_event_loop().time()
            try:
                await self._poll_once()
                backoff_idx = 0  # a clean cycle resets backoff
                continue
            except asyncio.CancelledError:
                return
            except _FatalPollError:
                self._running = False
                return
            except Exception as e:  # noqa: BLE001
                if not self._running:
                    return
                logger.warning("MAX: poll error: %s", self._safe(str(e)))

            if not self._running:
                return
            # Reset backoff if we stayed alive a while before erroring.
            if asyncio.get_event_loop().time() - loop_start >= 60.0:
                backoff_idx = 0
            delay = RECONNECT_BACKOFF[min(backoff_idx, len(RECONNECT_BACKOFF) - 1)]
            logger.info("MAX: retrying poll in %ds", delay)
            await asyncio.sleep(delay)
            backoff_idx += 1

    async def _poll_once(self) -> None:
        params: Dict[str, Any] = {"timeout": self.poll_timeout, "limit": 100}
        if self._marker is not None:
            params["marker"] = self._marker
        # httpx read timeout must outlast the server-side long-poll hold.
        status, body = await self._request(
            "GET", "/updates", params=params,
            timeout=float(self.poll_timeout + POLL_READ_BUFFER),
        )
        if status in (401, 403):
            logger.error("MAX: token rejected during poll (HTTP %s)", status)
            self._set_fatal_error("unauthorized", f"MAX rejected token (HTTP {status})", retryable=False)
            raise _FatalPollError()
        if status == 0:
            # Transport error / timeout — let the loop back off and retry.
            raise RuntimeError(str(body))
        if status != 200 or not isinstance(body, dict):
            raise RuntimeError(f"/updates HTTP {status}: {str(body)[:200]}")

        for update in body.get("updates", []) or []:
            try:
                await self._on_update(update)
            except Exception as e:  # noqa: BLE001 — one bad update must not kill the loop
                logger.warning("MAX: failed to handle update: %s", self._safe(str(e)))

        # Advance the marker so acked updates are not redelivered.
        new_marker = body.get("marker")
        if new_marker is not None:
            self._marker = new_marker

    # ── Inbound dispatch ──────────────────────────────────────────────────

    async def _on_update(self, update: Dict[str, Any]) -> None:
        utype = update.get("update_type")
        if utype == "message_callback":
            await self._on_callback(update)
            return
        if utype != "message_created":
            return  # ignore edits/removals/membership events for now
        message = update.get("message") or {}
        sender = message.get("sender") or {}
        recipient = message.get("recipient") or {}
        body = message.get("body") or {}

        sender_id = sender.get("user_id")
        # Drop our own echoes and other bots' messages (reply-loop guard).
        if self._bot_user_id is not None and sender_id == self._bot_user_id:
            return
        if sender.get("is_bot"):
            return

        text = (body.get("text") or "").strip()
        attachments = body.get("attachments") or []

        mid = body.get("mid") or ""
        if mid and self._is_duplicate(mid):
            return

        chat_id = recipient.get("chat_id")
        if chat_id is None:
            return
        # Acknowledge receipt so MAX marks the incoming message as read.
        await self._mark_seen(chat_id)
        # MAX chat_type: "dialog" = 1:1 DM, "chat" = group.
        raw_type = recipient.get("chat_type") or "dialog"
        chat_type = "dm" if raw_type == "dialog" else "group"

        user_name = sender.get("name") or sender.get("username") or str(sender_id)

        source = self.build_source(
            chat_id=str(chat_id),
            chat_name=user_name if chat_type == "dm" else str(chat_id),
            chat_type=chat_type,
            user_id=str(sender_id) if sender_id is not None else None,
            user_name=user_name,
        )

        ts = message.get("timestamp")
        try:
            # MAX timestamps are epoch milliseconds.
            timestamp = (
                datetime.fromtimestamp(int(ts) / 1000, tz=timezone.utc)
                if ts else datetime.now(tz=timezone.utc)
            )
        except (ValueError, OSError, TypeError):
            timestamp = datetime.now(tz=timezone.utc)

        # Download any photos/files/voice the user attached so the agent can
        # actually see them (food photos are the whole point of this bot).
        media_urls: List[str] = []
        media_types: List[str] = []
        message_type = MessageType.TEXT
        if attachments:
            media_urls, media_types, message_type = await self._ingest_attachments(attachments)

        # Nothing usable: no text and every attachment failed to download.
        if not text and not media_urls:
            if attachments:
                logger.warning(
                    "MAX: message %s had %d attachment(s) but none could be ingested",
                    mid or "?", len(attachments),
                )
            return

        event = MessageEvent(
            text=text,
            message_type=message_type,
            source=source,
            message_id=str(mid) if mid else None,
            raw_message=update,
            timestamp=timestamp,
            media_urls=media_urls,
            media_types=media_types,
        )
        await self.handle_message(event)

    # MAX attachment.type → how we cache it. Only media types are ingested;
    # inline_keyboard / location / share / contact are ignored here.
    async def _ingest_attachments(
        self, attachments: List[Dict[str, Any]]
    ) -> tuple[List[str], List[str], MessageType]:
        """Download inbound MAX attachments into the local media cache.

        Returns ``(media_urls, media_types, message_type)``.  ``media_urls`` are
        local file paths the agent's vision/file tools can read; ``media_types``
        is a parallel list of MIME strings.  An attachment that fails to
        download is skipped (and logged) rather than dropping the whole message.
        ``message_type`` reflects the first successfully-ingested attachment,
        defaulting to ``TEXT`` when nothing came through.

        MAX attachment payloads expose a direct CDN ``url`` (image → photo_id +
        token + url, media/file → url + token); ``file`` attachments also carry
        a top-level ``filename`` and ``size``.  See the official Go client
        schemes (PhotoAttachmentPayload / MediaAttachmentPayload /
        FileAttachment).
        """
        media_urls: List[str] = []
        media_types: List[str] = []
        first_type: Optional[MessageType] = None

        for att in attachments:
            if not isinstance(att, dict):
                continue
            atype = (att.get("type") or "").lower()
            if atype not in ("image", "video", "audio", "file", "sticker"):
                continue
            payload = att.get("payload") or {}
            url = payload.get("url")
            if not url:
                logger.warning("MAX: %s attachment has no download url", atype)
                continue

            data, dl_name = await self._load_bytes(url, default_name=f"{atype}.bin")
            if data is None:
                logger.warning("MAX: failed to download %s attachment", atype)
                continue
            # `file` attachments carry the real name at the top level.
            fname = att.get("filename") or dl_name

            try:
                if atype in ("image", "sticker"):
                    ext = self._ext_from_name(fname, ".jpg")
                    path = cache_image_from_bytes(data, ext=ext)
                    media_types.append(f"image/{ext.lstrip('.') or 'jpeg'}")
                    mtype = MessageType.STICKER if atype == "sticker" else MessageType.PHOTO
                elif atype == "video":
                    ext = self._ext_from_name(fname, ".mp4")
                    path = cache_video_from_bytes(data, ext=ext)
                    media_types.append(f"video/{ext.lstrip('.') or 'mp4'}")
                    mtype = MessageType.VIDEO
                elif atype == "audio":
                    # MAX has no distinct "voice" attachment type — voice notes
                    # arrive as `audio`. Map to VOICE so the gateway STT path
                    # auto-transcribes them. MessageType.AUDIO is treated as a
                    # plain audio file and is NEVER transcribed (see run.py STT
                    # routing); voice notes are the realistic case for this bot.
                    ext = self._ext_from_name(fname, ".ogg")
                    path = cache_audio_from_bytes(data, ext=ext)
                    media_types.append(f"audio/{ext.lstrip('.') or 'ogg'}")
                    mtype = MessageType.VOICE
                else:  # file
                    path = cache_document_from_bytes(data, fname or "file.bin")
                    media_types.append("application/octet-stream")
                    mtype = MessageType.DOCUMENT
            except Exception as e:  # noqa: BLE001 — one bad file must not kill the message
                logger.warning("MAX: failed to cache %s attachment: %s", atype, self._safe(str(e)))
                continue

            media_urls.append(path)
            if first_type is None:
                first_type = mtype
            logger.info("MAX: ingested %s attachment → %s", atype, path)

        return media_urls, media_types, (first_type or MessageType.TEXT)

    @staticmethod
    def _ext_from_name(name: Optional[str], default: str) -> str:
        """Best-effort file extension from a filename/URL tail, else *default*."""
        if name:
            base = name.split("?")[0]
            dot = base.rfind(".")
            if 0 <= dot < len(base) - 1:
                ext = base[dot:].lower()
                if len(ext) <= 6 and ext[1:].isalnum():
                    return ext
        return default

    def _is_duplicate(self, mid: str) -> bool:
        now = asyncio.get_event_loop().time()
        if len(self._seen_mids) > DEDUP_MAX_SIZE:
            self._seen_mids.clear()
        if mid in self._seen_mids:
            return True
        self._seen_mids[mid] = now
        return False

    # ── Outbound ──────────────────────────────────────────────────────────

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        if not self._http_client:
            return SendResult(success=False, error="Not connected", retryable=True)

        chunks = self.truncate_message(content, max_length=self.max_message_length)
        last_id: Optional[str] = None
        extra_ids: List[str] = []
        for chunk in chunks:
            res = await self._send_chunk(chat_id, chunk)
            if not res.success:
                return res
            if last_id is not None:
                extra_ids.append(last_id)
            last_id = res.message_id
        return SendResult(
            success=True,
            message_id=last_id,
            continuation_message_ids=tuple(extra_ids),
        )

    async def _send_chunk(self, chat_id: str, text: str) -> SendResult:
        body: Dict[str, Any] = {"text": text, "notify": True}
        if self.markdown:
            body["format"] = "markdown"
        status, resp = await self._request(
            "POST", "/messages", params={"chat_id": chat_id}, json_body=body, timeout=30.0,
        )
        # MAX returns 400 on malformed markdown — retry once as plain text so a
        # formatting glitch never drops the message.
        if status == 400 and "format" in body:
            body.pop("format", None)
            status, resp = await self._request(
                "POST", "/messages", params={"chat_id": chat_id}, json_body=body, timeout=30.0,
            )
        if status == 200 and isinstance(resp, dict):
            mid = (((resp.get("message") or {}).get("body") or {}).get("mid"))
            return SendResult(success=True, message_id=str(mid) if mid else None)
        retryable = status == 0 or status >= 500 or status == 429
        return SendResult(
            success=False,
            error=f"MAX send HTTP {status}: {str(resp)[:200]}",
            retryable=retryable,
        )

    async def send_typing(self, chat_id: str, metadata=None) -> None:
        if not self._http_client:
            return
        try:
            await self._request(
                "POST", f"/chats/{chat_id}/actions",
                json_body={"action": "typing_on"}, timeout=10.0,
            )
        except Exception:
            pass  # typing is best-effort

    async def _mark_seen(self, chat_id: str) -> None:
        """Tell MAX the bot has read the incoming message (read receipt)."""
        if not self._http_client:
            return
        try:
            await self._request(
                "POST", f"/chats/{chat_id}/actions",
                json_body={"action": "mark_seen"}, timeout=10.0,
            )
        except Exception:
            pass  # read receipt is best-effort

    # ── Interactive command approval (inline keyboard) ────────────────────

    async def send_exec_approval(
        self,
        chat_id: str,
        command: str,
        session_key: str,
        description: str = "dangerous command",
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        """Send a tappable approval prompt instead of the text /approve flow.

        The gateway detects this method on the adapter class and routes
        dangerous-command approvals here. Button taps arrive as
        ``message_callback`` updates and resolve the waiting agent thread via
        ``resolve_gateway_approval()`` — the same mechanism as ``/approve``.
        """
        if not self._http_client:
            return SendResult(success=False, error="Not connected")
        self._approval_counter += 1
        approval_id = self._approval_counter
        text = self._approval_text(command, description)
        keyboard = {
            "type": "inline_keyboard",
            "payload": {
                "buttons": [
                    [
                        {"type": "callback", "text": "✅ Allow once", "payload": f"ea:once:{approval_id}"},
                        {"type": "callback", "text": "🔓 This session", "payload": f"ea:session:{approval_id}"},
                    ],
                    [
                        {"type": "callback", "text": "💚 Always", "payload": f"ea:always:{approval_id}"},
                        {"type": "callback", "text": "🙅 Deny", "payload": f"ea:deny:{approval_id}"},
                    ],
                ]
            },
        }
        # Plain text (no format) so special chars in the command can't trip the
        # markdown parser and drop the approval prompt.
        body = {"text": text, "notify": True, "attachments": [keyboard]}
        status, resp = await self._request(
            "POST", "/messages", params={"chat_id": chat_id}, json_body=body, timeout=30.0,
        )
        if status == 200 and isinstance(resp, dict):
            self._approval_state[approval_id] = session_key
            mid = (((resp.get("message") or {}).get("body") or {}).get("mid"))
            return SendResult(success=True, message_id=str(mid) if mid else None)
        return SendResult(success=False, error=f"MAX approval send HTTP {status}: {str(resp)[:200]}")

    @staticmethod
    def _approval_text(command: str, description: str) -> str:
        """Human-readable approval prompt for a gated command.

        Renders a neutral request with a preview of the command instead of a
        raw plumbing dump. Override / wrap this if you want to give the prompt
        your agent's own voice.
        """
        inner = command or ""
        # Unwrap the execute_code heredoc so we show the actual snippet,
        # not the `execute_code <<'PY' … PY` plumbing.
        if inner.startswith("execute_code <<'PY'\n") and inner.endswith("\nPY"):
            inner = inner[len("execute_code <<'PY'\n"):-len("\nPY")]
        preview = inner.strip()
        if len(preview) > 500:
            preview = preview[:500].rstrip() + " …"
        body = (description or "").strip() or (
            "The agent wants to run a command on the server and needs your approval."
        )
        if preview:
            body += f"\n\nCommand:\n{preview}"
        body += "\n\nApprove?"
        return body

    async def _on_callback(self, update: Dict[str, Any]) -> None:
        """Resolve an inline-keyboard approval tap."""
        cb = update.get("callback") or {}
        payload = cb.get("payload") or ""
        callback_id = cb.get("callback_id")
        if not callback_id or not payload.startswith("ea:"):
            return
        try:
            _, choice, sid = payload.split(":", 2)
            approval_id = int(sid)
        except (ValueError, TypeError):
            return
        # Only allowlisted users may resolve an approval.
        presser_id = (cb.get("user") or {}).get("user_id")
        if not self._user_allowed(presser_id):
            await self._answer_callback(callback_id, "Not allowed.")
            return
        session_key = self._approval_state.pop(approval_id, None)
        if session_key is None:
            await self._answer_callback(callback_id, "This request was already handled or has expired.")
            return
        try:
            from tools.approval import resolve_gateway_approval
            resolve_gateway_approval(session_key, choice)
        except Exception as e:  # noqa: BLE001
            logger.warning("MAX: resolve approval failed: %s", self._safe(str(e)))
        label = {
            "once": "✅ Approved — running it.",
            "session": "🔓 Approved for this session.",
            "always": "💚 Approved — won't ask again for this.",
            "deny": "🙅 Denied.",
        }.get(choice, "Done")
        # Edit the original message to drop the buttons + toast the decision.
        await self._answer_callback(callback_id, label, edit_text=label)

    async def _answer_callback(
        self, callback_id: str, notification: str, edit_text: Optional[str] = None
    ) -> None:
        body: Dict[str, Any] = {}
        if edit_text is not None:
            body["message"] = {"text": edit_text}
        if notification:
            body["notification"] = notification
        try:
            await self._request(
                "POST", "/answers", params={"callback_id": callback_id},
                json_body=body, timeout=10.0,
            )
        except Exception:
            pass  # answering the tap is best-effort

    def _user_allowed(self, user_id) -> bool:
        """Allowlist check for who may resolve an approval (mirrors gateway auth)."""
        if user_id is None:
            return False
        if _truthy(os.getenv("MAX_ALLOW_ALL_USERS")):
            return True
        raw = os.getenv("MAX_ALLOWED_USERS", "").strip()
        if not raw:
            return False
        allowed = {u.strip() for u in raw.split(",") if u.strip()}
        return str(user_id) in allowed

    async def get_chat_info(self, chat_id: str) -> Dict[str, Any]:
        status, body = await self._request("GET", f"/chats/{chat_id}", timeout=15.0)
        if status == 200 and isinstance(body, dict):
            raw_type = body.get("type") or "dialog"
            return {
                "name": body.get("title") or str(chat_id),
                "type": "dm" if raw_type == "dialog" else "group",
                "chat_id": str(chat_id),
            }
        return {"name": str(chat_id), "type": "dm", "chat_id": str(chat_id)}

    # ── Attachments (best-effort; degrade to a text link on any failure) ───

    async def _upload(self, data: bytes, filename: str, upload_type: str) -> Optional[Dict[str, Any]]:
        """Run the MAX two-step upload. Returns the attachment payload or None.

        Step 1: ``POST /uploads?type=<image|video|audio|file>`` → ``{url}``.
        Step 2: POST the binary (multipart field ``data``) to that URL → the
        attachment payload (``{photos: {...}}`` for images, ``{token: ...}``
        otherwise).
        """
        status, body = await self._request("POST", "/uploads", params={"type": upload_type}, timeout=30.0)
        if status != 200 or not isinstance(body, dict):
            logger.warning("MAX: /uploads(%s) HTTP %s", upload_type, status)
            return None
        upload_url = body.get("url")
        if not upload_url:
            return None
        try:
            resp = await self._http_client.post(
                upload_url, files={"data": (filename, data)}, timeout=120.0,
            )
        except Exception as e:  # noqa: BLE001
            logger.warning("MAX: upload POST failed: %s", self._safe(str(e)))
            return None
        if resp.status_code >= 300:
            logger.warning("MAX: upload POST HTTP %s", resp.status_code)
            return None
        try:
            payload = resp.json()
        except Exception:
            payload = {}
        if upload_type == "image":
            if isinstance(payload, dict) and payload.get("photos"):
                return {"type": "image", "payload": {"photos": payload["photos"]}}
            token = payload.get("token") if isinstance(payload, dict) else None
            return {"type": "image", "payload": {"token": token}} if token else None
        token = payload.get("token") if isinstance(payload, dict) else None
        if not token:
            return None
        return {"type": upload_type, "payload": {"token": token}}

    async def _send_with_attachment(
        self, chat_id: str, attachment: Dict[str, Any], caption: Optional[str]
    ) -> SendResult:
        body: Dict[str, Any] = {"attachments": [attachment], "notify": True}
        if caption:
            body["text"] = caption[: self.max_message_length]
        if self.markdown:
            body["format"] = "markdown"
        # MAX may answer "attachment.not.ready" (HTTP 400) while it finishes
        # processing video/audio — retry a few times with a short delay.
        for attempt in range(4):
            status, resp = await self._request(
                "POST", "/messages", params={"chat_id": chat_id}, json_body=body, timeout=30.0,
            )
            if status == 200 and isinstance(resp, dict):
                mid = (((resp.get("message") or {}).get("body") or {}).get("mid"))
                return SendResult(success=True, message_id=str(mid) if mid else None)
            if status == 400 and "not.ready" in str(resp):
                await asyncio.sleep(1.5 * (attempt + 1))
                continue
            break
        return SendResult(success=False, error=f"MAX attachment send HTTP {status}: {str(resp)[:200]}")

    async def send_image(
        self,
        chat_id: str,
        image_url: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        data, fname = await self._load_bytes(image_url, default_name="image.jpg")
        if data is not None:
            attachment = await self._upload(data, fname, "image")
            if attachment:
                res = await self._send_with_attachment(chat_id, attachment, caption)
                if res.success:
                    return res
        # Fallback: send the URL/path as text so nothing is silently dropped.
        return await super().send_image(chat_id, image_url, caption, reply_to, metadata)

    async def send_document(
        self,
        chat_id: str,
        file_path: str,
        caption: Optional[str] = None,
        file_name: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        **kwargs,
    ) -> SendResult:
        data, fname = await self._load_bytes(file_path, default_name=file_name or "file.bin")
        if data is not None:
            attachment = await self._upload(data, fname, "file")
            if attachment:
                res = await self._send_with_attachment(chat_id, attachment, caption)
                if res.success:
                    return res
        return await super().send_document(
            chat_id, file_path, caption, file_name, reply_to, metadata, **kwargs
        )

    async def _load_bytes(self, src: str, default_name: str) -> tuple[Optional[bytes], str]:
        """Read bytes from a local path or download from an http(s) URL."""
        try:
            if src.startswith("http://") or src.startswith("https://"):
                resp = await self._http_client.get(src, timeout=60.0)
                if resp.status_code >= 300:
                    return None, default_name
                name = src.rsplit("/", 1)[-1].split("?")[0] or default_name
                return resp.content, name or default_name
            if os.path.isfile(src):
                with open(src, "rb") as fh:
                    return fh.read(), os.path.basename(src) or default_name
        except Exception as e:  # noqa: BLE001
            logger.warning("MAX: could not load %s: %s", default_name, self._safe(str(e)))
        return None, default_name


# ---------------------------------------------------------------------------
# Internal signalling
# ---------------------------------------------------------------------------

class _FatalPollError(Exception):
    """Raised inside the poll loop to stop reconnecting (e.g. 401)."""


# ---------------------------------------------------------------------------
# Plugin registration helpers
# ---------------------------------------------------------------------------

def _token_configured() -> str:
    return (os.getenv("MAX_TOKEN") or "").strip()


def check_requirements() -> bool:
    """True when MAX is minimally configured and httpx is importable."""
    if not HTTPX_AVAILABLE:
        return False
    return bool(_token_configured())


def validate_config(config) -> bool:
    extra = getattr(config, "extra", {}) or {}
    token = os.getenv("MAX_TOKEN") or extra.get("token") or getattr(config, "token", "")
    return bool(str(token).strip())


def is_connected(config) -> bool:
    return validate_config(config)


def _env_enablement() -> Optional[dict]:
    """Seed ``PlatformConfig.extra`` from env vars during gateway config load.

    Lets env-only setups surface in ``hermes gateway status`` and
    ``get_connected_platforms()`` without instantiating the adapter.  The
    special ``home_channel`` key is promoted to a ``HomeChannel`` dataclass
    by the core hook.
    """
    token = _token_configured()
    if not token:
        return None
    seed: dict = {"token": token}
    api_base = os.getenv("MAX_API_BASE", "").strip()
    if api_base:
        seed["api_base"] = api_base.rstrip("/")
    if os.getenv("MAX_MARKDOWN") is not None:
        seed["markdown"] = _truthy(os.getenv("MAX_MARKDOWN"))
    poll = os.getenv("MAX_POLL_TIMEOUT", "").strip()
    if poll:
        try:
            seed["poll_timeout"] = int(poll)
        except ValueError:
            pass
    home = os.getenv("MAX_HOME_CHANNEL", "").strip()
    if home:
        seed["home_channel"] = {
            "chat_id": home,
            "name": os.getenv("MAX_HOME_CHANNEL_NAME", home),
        }
    return seed


def interactive_setup() -> None:
    """Minimal ``hermes gateway setup`` flow for MAX.

    Lazy-imports CLI helpers so the plugin stays importable in non-CLI
    contexts (gateway runtime, tests).
    """
    try:
        from hermes_cli.setup import prompt_env_var  # type: ignore
    except Exception:
        logger.info(
            "MAX setup: set MAX_TOKEN (bot token from @MasterBot) in your "
            "Hermes .env, then run `hermes gateway status`."
        )
        return
    prompt_env_var(
        "MAX_TOKEN",
        "MAX bot token (from @MasterBot in the MAX app)",
        password=True,
    )


async def _standalone_send(
    pconfig,
    chat_id: str,
    message: str,
    *,
    thread_id: Optional[str] = None,
    media_files: Optional[List[str]] = None,
    force_document: bool = False,
) -> Dict[str, Any]:
    """Out-of-process delivery for cron jobs (``deliver=max``).

    Opens an ephemeral httpx client and POSTs a single message so
    ``deliver=max`` works even when cron runs separately from the gateway.
    ``thread_id``/``media_files`` are accepted for signature parity.
    """
    if not HTTPX_AVAILABLE:
        return {"error": "MAX standalone send: httpx not installed"}
    extra = getattr(pconfig, "extra", {}) or {}
    token = (os.getenv("MAX_TOKEN") or extra.get("token") or getattr(pconfig, "token", "") or "").strip()
    if not token:
        return {"error": "MAX standalone send: MAX_TOKEN must be configured"}
    if not chat_id:
        return {"error": "MAX standalone send: chat_id is required"}

    api_base = (os.getenv("MAX_API_BASE") or extra.get("api_base") or DEFAULT_API_BASE).rstrip("/")
    api_version = (os.getenv("MAX_API_VERSION") or extra.get("api_version") or "").strip()
    markdown = (
        _truthy(os.getenv("MAX_MARKDOWN"))
        if os.getenv("MAX_MARKDOWN") is not None
        else bool(extra.get("markdown", True))
    )

    params: Dict[str, Any] = {"chat_id": chat_id}
    if api_version:
        params["v"] = api_version
    body: Dict[str, Any] = {"text": message[:MAX_MESSAGE_LENGTH], "notify": True}
    if markdown:
        body["format"] = "markdown"

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(
                f"{api_base}/messages", params=params, json=body,
                headers={"Authorization": token},
            )
            # Retry once as plain text if markdown failed to parse (400).
            if resp.status_code == 400 and "format" in body:
                body.pop("format", None)
                resp = await client.post(
                    f"{api_base}/messages", params=params, json=body,
                    headers={"Authorization": token},
                )
    except Exception as e:  # noqa: BLE001
        return {"error": f"MAX standalone send failed: {str(e).replace(token, '***')}"}

    if resp.status_code != 200:
        return {"error": f"MAX standalone send HTTP {resp.status_code}: {resp.text[:200]}"}
    try:
        mid = (((resp.json().get("message") or {}).get("body") or {}).get("mid"))
    except Exception:
        mid = None
    return {"success": True, "platform": "max", "chat_id": chat_id, "message_id": mid}


def register(ctx):
    """Plugin entry point — called by the Hermes plugin system at startup."""
    ctx.register_platform(
        name="max",
        label="MAX",
        adapter_factory=lambda cfg: MaxAdapter(cfg),
        check_fn=check_requirements,
        validate_config=validate_config,
        is_connected=is_connected,
        required_env=["MAX_TOKEN"],
        install_hint="pip install httpx   # already a Hermes dependency",
        setup_fn=interactive_setup,
        # Env-driven auto-configuration so env-only setups show up in
        # `hermes gateway status` without instantiating the HTTP client.
        env_enablement_fn=_env_enablement,
        # Cron home-channel delivery: `deliver=max` routes to MAX_HOME_CHANNEL.
        cron_deliver_env_var="MAX_HOME_CHANNEL",
        # Out-of-process cron delivery (gateway not in this process).
        standalone_sender_fn=_standalone_send,
        # Auth env vars for _is_user_authorized() integration.
        allowed_users_env="MAX_ALLOWED_USERS",
        allow_all_env="MAX_ALLOW_ALL_USERS",
        max_message_length=MAX_MESSAGE_LENGTH,
        emoji="🟣",
        # MAX identifiers are opaque numeric user_ids — no phone/email PII.
        pii_safe=True,
        allow_update_command=True,
        platform_hint=(
            "You are chatting via MAX (max.ru) messenger. Keep replies "
            "concise and conversational. MAX renders standard markdown: "
            "**bold**, *italic*, ~~strike~~, `inline code`, ```code blocks```, "
            "[links](url) and # headings. Put identifiers/paths/code in backticks "
            "so underscores and asterisks aren't misread as formatting. Messages "
            "are limited to 4000 characters (longer replies are split automatically)."
        ),
    )
