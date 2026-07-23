"""Hermes plugin adapter for the MAX.ru messenger Bot API.

This is a **plugin platform** that lives in ``~/.hermes/plugins/platforms/maxru/``
and does not modify core Hermes code. It supports:

- Inbound messages via Long Polling (``GET /updates``) for dev/test
- Inbound messages via Webhook (``POST`` to a user-supplied HTTPS URL) for prod
- Outbound text/image/file messages via ``POST /messages``
- Optional inline keyboards via the ``clarify`` tool
- Cron delivery to a configured ``MAXRU_HOME_CHANNEL``

API docs: https://dev.max.ru/docs-api
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

# Defer aiohttp import so check_maxru_requirements() can run even when aiohttp
# is missing. The plugin loader calls check_fn before constructing the adapter.
aiohttp: Any = None

logger = logging.getLogger(__name__)


def _import_aiohttp():
    global aiohttp
    if aiohttp is None:
        import aiohttp as _aiohttp
        aiohttp = _aiohttp
    return aiohttp


def check_maxru_requirements() -> bool:
    """Check that runtime dependencies are available."""
    try:
        _import_aiohttp()
        return True
    except ImportError:
        logger.warning("maxru-platform: aiohttp not installed")
        return False


# Hermes imports — available when the plugin is loaded by the gateway.
# Imported here (not at module top) because the module is loaded by Hermes
# after the gateway package is on sys.path.
from gateway.platforms.base import (
    BasePlatformAdapter,
    MessageEvent,
    MessageType,
    SendResult,
)
from gateway.session import SessionSource
from gateway.config import Platform, PlatformConfig


def register(ctx) -> None:
    """Plugin entry point called by Hermes."""
    ctx.register_platform(
        name="maxru",
        label="MAX.ru",
        adapter_factory=_build_adapter,
        check_fn=check_maxru_requirements,
        is_connected=_is_connected,
        required_env=["MAXRU_TOKEN"],
        install_hint="pip install aiohttp",
        setup_fn=interactive_setup,
        env_enablement_fn=_env_enablement,
        apply_yaml_config_fn=_apply_yaml_config,
        allowed_users_env="MAXRU_ALLOWED_USERS",
        allow_all_env="MAXRU_ALLOW_ALL_USERS",
        cron_deliver_env_var="MAXRU_HOME_CHANNEL",
        standalone_sender_fn=_standalone_send,
        max_message_length=4000,
        emoji="🇲",
        allow_update_command=True,
        platform_hint=(
            "You are chatting via MAX.ru messenger. MAX supports basic Markdown "
            "and HTML formatting. Long messages may be truncated at 4000 characters. "
            "Image URLs and uploaded files are supported."
        ),
    )


def _build_adapter(config: PlatformConfig) -> "MaxruAdapter":
    return MaxruAdapter(config)


def _is_connected(config) -> bool:
    token = getattr(config, "token", None)
    if not token and hasattr(config, "extra"):
        token = (config.extra or {}).get("token")
    if not token:
        token = os.getenv("MAXRU_TOKEN", "")
    return bool(str(token).strip())


def _env_enablement() -> Optional[dict]:
    """Seed PlatformConfig.extra from environment before adapter construction."""
    token = os.getenv("MAXRU_TOKEN")
    if not token:
        return None
    return {
        "token": token,
        "api_url": os.getenv("MAXRU_API_URL", "https://platform-api2.max.ru"),
        "allowed_users": _parse_id_list(os.getenv("MAXRU_ALLOWED_USERS", "")),
        "allow_all_users": os.getenv("MAXRU_ALLOW_ALL_USERS", "").lower() in ("1", "true", "yes"),
        "join_pin": os.getenv("MAXRU_JOIN_PIN", "").strip(),
        "home_channel": os.getenv("MAXRU_HOME_CHANNEL"),
        "long_polling": os.getenv("MAXRU_LONG_POLLING", "true").lower() in ("1", "true", "yes"),
        "webhook_url": os.getenv("MAXRU_WEBHOOK_URL"),
        "update_timeout": int(os.getenv("MAXRU_UPDATE_TIMEOUT", "30")),
        "rps_limit": float(os.getenv("MAXRU_RPS_LIMIT", "25")),
    }


def _apply_yaml_config(yaml_cfg: dict, maxru_cfg: dict) -> Optional[dict]:
    """Translate ``maxru:`` section in config.yaml into adapter extras."""
    extras: dict = {}
    if not maxru_cfg:
        return None

    for key, value in maxru_cfg.items():
        if key == "allowed_users" and isinstance(value, str):
            value = _parse_id_list(value)
        elif key == "allow_all_users":
            value = str(value).lower() in ("1", "true", "yes")
        extras[key] = value

    extras.setdefault("api_url", "https://platform-api2.max.ru")
    return extras or None


def _parse_id_list(raw: str) -> list[str]:
    return [s.strip() for s in raw.split(",") if s.strip()]


def interactive_setup() -> None:
    """Minimal interactive setup (placeholder)."""
    print("MAX.ru setup: set MAXRU_TOKEN and (optionally) MAXRU_HOME_CHANNEL in ~/.hermes/.env")


class MaxruAPIError(Exception):
    pass


class MaxruAdapter(BasePlatformAdapter):
    """MAX.ru Bot API adapter for Hermes."""

    supports_code_blocks = False
    supports_async_delivery = True
    splits_long_messages = False
    typed_command_prefix = "/"

    def __init__(self, config: PlatformConfig):
        aiohttp = _import_aiohttp()
        super().__init__(config, Platform("maxru"))
        extra = config.extra or {}
        self.token: str = extra.get("token", "")
        self.api_url: str = extra.get("api_url", "https://platform-api2.max.ru").rstrip("/")
        self.allowed_users: set[str] = set(extra.get("allowed_users", []))
        self.allow_all_users: bool = bool(extra.get("allow_all_users", False))
        # join_pin is deprecated; authorization now goes through hermes pairing
        # (PairingStore). Kept as a no-op attribute so old config.yaml entries
        # don't crash the adapter.
        self.join_pin: str = str(extra.get("join_pin", os.getenv("MAXRU_JOIN_PIN", ""))).strip()
        self.home_channel: Optional[str] = extra.get("home_channel")
        self.long_polling: bool = bool(extra.get("long_polling", True))
        self.webhook_url: Optional[str] = extra.get("webhook_url")
        self.update_timeout: int = int(extra.get("update_timeout", 30))
        self.rps_limit: float = float(extra.get("rps_limit", 25))

        self._session: Optional[Any] = None
        self._poll_task: Optional[asyncio.Task] = None
        self._stop_event = asyncio.Event()
        self._last_update_id: Optional[int] = None
        # Token bucket rate limiter (target below MAX's 30 rps)
        self._rate_tokens = self.rps_limit
        self._rate_last = time.monotonic()

    MAX_MESSAGE_LENGTH = 4000
    splits_long_messages: bool = True
    SUPPORTS_MESSAGE_EDITING: bool = False

    def _persist_allowed_users(self) -> None:
        """Persist the current allowed_users set to config.yaml."""
        try:
            import subprocess, os
            value = ",".join(sorted(self.allowed_users))
            env = {**os.environ, "HERMES_HOME": "/root/.hermes"}
            subprocess.run(
                ["hermes", "config", "set", "maxru.allowed_users", value],
                capture_output=True,
                text=True,
                env=env,
                timeout=10,
                check=False,
            )
        except Exception as e:
            logger.warning("maxru: failed to persist allowed_users: %s", e)

    # --------------------------------------------------------------------- #
    # Lifecycle
    # --------------------------------------------------------------------- #

    async def connect(self, *, is_reconnect: bool = False) -> bool:
        aiohttp = _import_aiohttp()
        logger.warning("maxru: connect() called; token present=%s is_reconnect=%s", bool(self.token), is_reconnect)
        if not self.token:
            logger.error("maxru: MAXRU_TOKEN not set")
            return False

        connector = aiohttp.TCPConnector(verify_ssl=False)
        self._session = aiohttp.ClientSession(
            headers={"Authorization": self.token},
            timeout=aiohttp.ClientTimeout(total=60),
            connector=connector,
        )

        # Verify token with /me
        try:
            me = await self._api_get("/me")
            logger.warning("maxru: /me ok: username=%s user_id=%s", me.get("username"), me.get("user_id"))
        except MaxruAPIError as e:
            logger.error("maxru: /me failed: %s", e)
            return False

        if self.long_polling:
            self._stop_event.clear()
            self._poll_task = asyncio.create_task(self._poll_loop())
        elif self.webhook_url:
            await self._register_webhook()

        logger.warning("maxru: connect() finished successfully; long_polling=%s", self.long_polling)
        return True

    async def disconnect(self) -> None:
        self._stop_event.set()
        if self._poll_task:
            self._poll_task.cancel()
            try:
                await self._poll_task
            except asyncio.CancelledError:
                pass
        if self._session:
            await self._session.close()
            self._session = None

    # --------------------------------------------------------------------- #
    # Inbound: Long Polling
    # --------------------------------------------------------------------- #

    async def _poll_loop(self) -> None:
        logger.info("maxru: starting long-polling loop")
        while not self._stop_event.is_set():
            try:
                params: dict[str, Any] = {"limit": 100, "timeout": self.update_timeout}
                if self._last_update_id is not None:
                    params["marker"] = self._last_update_id

                data = await self._api_get("/updates", params=params)
                if not isinstance(data, dict):
                    logger.warning("maxru: /updates returned non-dict: %r", data)
                    await asyncio.sleep(1)
                    continue

                updates = data.get("updates") or []
                marker = data.get("marker")
                logger.warning("maxru: /updates received %d updates, marker=%s", len(updates), marker)
                if marker is not None:
                    self._last_update_id = marker

                for update in updates:
                    logger.warning("maxru: raw update: %r", update)
                    if isinstance(update, dict):
                        await self._handle_update(update)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.exception("maxru: polling error: %s", e)
                await asyncio.sleep(5)

    # --------------------------------------------------------------------- #
    # Inbound: Webhook
    # --------------------------------------------------------------------- #

    async def _register_webhook(self) -> None:
        if not self.webhook_url:
            return
        payload = {
            "url": self.webhook_url,
            "update_types": ["message_created", "bot_started", "message_callback"],
        }
        try:
            await self._api_post("/subscriptions", payload)
            logger.info("maxru: registered webhook %s", self.webhook_url)
        except MaxruAPIError as e:
            logger.error("maxru: webhook registration failed: %s", e)

    async def handle_webhook_update(self, update: dict) -> None:
        """Entry point for external webhook handler."""
        await self._handle_update(update)

    async def _handle_update(self, update: dict) -> None:
        update_type = update.get("update_type")
        logger.warning("maxru: _handle_update type=%s update=%r", update_type, update)
        message = update.get("message", {}) or {}
        recipient = message.get("recipient", {}) or {}
        sender = message.get("sender", {}) or {}

        # Events like bot_started have top-level chat_id/user instead of
        # nested inside message.recipient / message.sender.
        chat_id = recipient.get("chat_id") or update.get("chat_id")
        raw_user = sender or update.get("user", {})
        user_id = str(raw_user.get("user_id", "")) or str(update.get("user_id", ""))
        user_name = raw_user.get("name", "")

        timestamp_ms = message.get("timestamp") or update.get("timestamp")
        timestamp_dt = None
        if isinstance(timestamp_ms, (int, float)) and timestamp_ms > 0:
            timestamp_dt = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)

        if update_type == "message_created":
            body = message.get("body", {}) or {}
            text = body.get("text", "")
            # If user is not yet approved, issue a pairing code via the
            # unified Hermes PairingStore.
            if user_id and not self._is_user_allowed(user_id):
                await self._handle_unauthorized_dm(
                    chat_id=chat_id,
                    user_id=user_id,
                    user_name=user_name,
                )
                return
            event = self._build_message_event(
                chat_id=chat_id,
                user_id=user_id,
                user_name=user_name,
                text=text,
                message_id=body.get("mid"),
                timestamp=timestamp_dt,
            )
            await self.handle_message(event)
            logger.warning("maxru: dispatched message_created from user=%s text=%r", user_id, text)

        elif update_type == "bot_started":
            payload = update.get("payload", "")
            greeting = "Привет!" if not payload else f"Привет! Диплинк: {payload}"
            # On /start the user isn't approved yet — issue a pairing code.
            if user_id and not self._is_user_allowed(user_id):
                await self._handle_unauthorized_dm(
                    chat_id=chat_id,
                    user_id=user_id,
                    user_name=user_name,
                )
                return
            await self.send(str(chat_id), greeting)

        elif update_type == "message_callback":
            callback = update.get("callback", {}) or {}
            callback_payload = callback.get("payload", "")
            callback_text = f"[callback:{callback_payload}]"
            event = self._build_message_event(
                chat_id=chat_id,
                user_id=user_id,
                user_name=user_name,
                text=callback_text,
                message_id=callback.get("message_id"),
                timestamp=timestamp_dt,
            )
            await self.handle_message(event)

    def _build_message_event(
        self,
        chat_id,
        user_id: str,
        user_name: str,
        text: str,
        message_id: Optional[Any],
        timestamp: Optional[datetime],
    ) -> MessageEvent:
        source = SessionSource(
            platform=self.platform,
            chat_id=str(user_id),
            chat_name=user_name or user_id or str(chat_id),
            chat_type="dm",
            user_id=user_id,
            user_name=user_name,
            message_id=str(message_id) if message_id is not None else None,
        )
        return MessageEvent(
            source=source,
            message_type=MessageType.TEXT,
            text=text,
            message_id=str(message_id) if message_id is not None else None,
            timestamp=timestamp or datetime.now(timezone.utc),
        )

    # --------------------------------------------------------------------- #
    # Authorization
    # --------------------------------------------------------------------- #

    def _is_user_allowed(self, user_id: str) -> bool:
        """Check whether ``user_id`` may talk to the bot.

        Authoritative source is the Hermes pairing store (``hermes pairing
        approve maxru <code>`` writes the user there). The static
        ``self.allowed_users`` set is kept as a one-shot fallback so that
        existing users in config.yaml continue to work after the upgrade
        without a forced re-pairing. It is *not* an alternate grant path —
        never call ``allowed_users.add()`` from the hot path again.
        """
        if self.allow_all_users:
            return bool(user_id)
        if not user_id:
            return False
        if user_id in self.allowed_users:
            return True
        try:
            from gateway.pairing import PairingStore  # type: ignore[import-not-found]
        except ImportError:
            # gateway/ not on sys.path inside the plugin's isolated import
            # context. Fall back to importlib so the security check still
            # works regardless of how Hermes loaded the plugin.
            import importlib

            pairing_mod = importlib.import_module("gateway.pairing")
            PairingStore = pairing_mod.PairingStore
        try:
            if PairingStore().is_approved("maxru", user_id):
                return True
        except Exception as e:  # pairing store not initialized yet
            logger.warning("maxru: PairingStore check failed: %s", e)
        return False

    async def _handle_unauthorized_dm(
        self,
        *,
        chat_id: Any,
        user_id: str,
        user_name: str,
    ) -> None:
        """Issue a Hermes pairing code to a new user.

        Mirrors the flow used by built-in platforms (Telegram, Discord):
        ``PairingStore.generate_code`` returns an 8-char code or ``None``
        if the user is rate-limited / platform is locked out / pending is
        full. The owner approves with
        ``hermes pairing approve maxru <CODE>`` and from that point on the
        user is in the approved allowlist.
        """
        try:
            from gateway.pairing import PairingStore  # type: ignore[import-not-found]
        except ImportError:
            import importlib

            pairing_mod = importlib.import_module("gateway.pairing")
            PairingStore = pairing_mod.PairingStore

        store = PairingStore()
        # Don't spam codes: if the user is already in the pairing rate-limit
        # window (1 req per 10 min) PairingStore.generate_code returns None.
        # Mirror the platform's behavior of going silent in that case rather
        # than issuing a fresh code every message.
        if store._is_rate_limited("maxru", user_id):
            logger.warning(
                "maxru: rate-limited pairing response for user=%s", user_id
            )
            return

        code = store.generate_code("maxru", user_id, user_name or "")
        if not code:
            # Lockout or pending-full: stay quiet, log only.
            logger.warning(
                "maxru: pairing code not issued for user=%s (lockout or pending full)",
                user_id,
            )
            return

        logger.warning(
            "maxru: issued pairing code for user=%s name=%r (awaiting hermes pairing approve)",
            user_id,
            user_name,
        )
        await self.send(
            str(chat_id),
            "Привет! Это приватный бот. "
            "Чтобы получить доступ, попроси владельца выполнить на сервере:\n\n"
            f"`hermes pairing approve maxru {code}`\n\n"
            "Код действует 1 час. После approve просто напиши сюда ещё раз.",
        )

    # --------------------------------------------------------------------- #
    # Outbound
    # --------------------------------------------------------------------- #

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: Optional[str] = None,
        metadata: Optional[dict] = None,
        **kwargs,
    ) -> SendResult:
        """Send a plain text message.

        In MAX API personal dialogs are addressed by ``user_id``,
        not ``chat_id``. ``BasePlatformAdapter`` calls us with the
        ``SessionSource.chat_id`` we populated from the inbound sender id,
        so we pass it through as ``user_id``.
        """
        if not content:
            return SendResult(success=True, message_id=None)

        payload: dict[str, Any] = {
            "text": content,
            "format": "markdown",
        }
        if reply_to:
            payload["reply_to_message_id"] = reply_to

        params = self._user_params(chat_id)
        logger.warning("maxru: send payload: chat_id=%s params=%r body=%r", chat_id, params, payload)
        return await self._send_message_payload(payload, params=params)

    async def send_image(
        self,
        chat_id: str,
        image_url: str,
        caption: str = "",
        **kwargs,
    ) -> SendResult:
        """Send an image by URL (no upload needed for images)."""
        payload: dict[str, Any] = {"format": "markdown"}
        if caption:
            payload["text"] = caption
        payload["attachments"] = [{"type": "image", "payload": {"url": image_url}}]
        return await self._send_message_payload(payload, params=self._user_params(chat_id))

    async def send_image_file(
        self,
        chat_id: str,
        path: str,
        caption: str = "",
        **kwargs,
    ) -> SendResult:
        """Upload a local image and send it."""
        token = await self._upload_file(path)
        payload: dict[str, Any] = {"format": "markdown"}
        if caption:
            payload["text"] = caption
        payload["attachments"] = [{"type": "image", "payload": {"token": token}}]
        return await self._send_message_payload(payload, params=self._user_params(chat_id))

    async def send_document(
        self,
        chat_id: str,
        path: str,
        caption: str = "",
        **kwargs,
    ) -> SendResult:
        """Upload a local file and send it."""
        token = await self._upload_file(path)
        payload: dict[str, Any] = {
            "format": "markdown",
            "attachments": [{"type": "file", "payload": {"token": token}}],
        }
        if caption:
            payload["text"] = caption
        return await self._send_message_payload(payload, params=self._user_params(chat_id))

    def _user_params(self, chat_id: str) -> dict[str, Any]:
        """Return MAX API query params for a personal-dialog recipient."""
        return {"user_id": int(chat_id) if chat_id.isdigit() else chat_id}

    async def send_typing(self, chat_id: str, metadata=None) -> None:
        """MAX API does not expose a typing indicator; noop."""
        return

    async def get_chat_info(self, chat_id: str) -> dict:
        """Return basic chat info."""
        return {
            "name": chat_id,
            "type": "dm",
            "chat_id": chat_id,
        }

    # --------------------------------------------------------------------- #
    # Interactive UX
    # --------------------------------------------------------------------- #

    async def send_clarify(
        self,
        chat_id: str,
        question: str,
        choices: Optional[list],
        clarify_id: str,
        session_key: str,
        metadata: Optional[dict] = None,
        **kwargs,
    ) -> SendResult:
        """Render a clarify tool question as an inline keyboard."""
        if not choices:
            return await self.send(chat_id, f"❓ {question}")
        buttons = [[{"type": "callback", "text": c, "payload": f"cl:{clarify_id}:{i}"}] for i, c in enumerate(choices)]
        payload = {
            "chat_id": chat_id,
            "text": question,
            "format": "markdown",
            "attachments": [
                {
                    "type": "inline_keyboard",
                    "payload": {"buttons": buttons},
                }
            ],
        }
        return await self._send_message_payload(payload, params=self._user_params(chat_id))

    # --------------------------------------------------------------------- #
    # API helpers
    # --------------------------------------------------------------------- #

    async def _api_get(self, path: str, params: Optional[dict] = None) -> Any:
        await self._acquire_rate_token()
        if not self._session:
            raise MaxruAPIError("session not initialized")
        async with self._session.get(self.api_url + path, params=params) as resp:
            return await self._parse_response(resp)

    async def _api_post(self, path: str, json_data: dict, params: Optional[dict] = None) -> Any:
        await self._acquire_rate_token()
        if not self._session:
            raise MaxruAPIError("session not initialized")
        async with self._session.post(self.api_url + path, params=params, json=json_data) as resp:
            return await self._parse_response(resp)

    async def _parse_response(self, resp: Any) -> Any:
        body = await resp.text()
        if resp.status >= 400:
            raise MaxruAPIError(f"HTTP {resp.status}: {body[:200]}")
        if not body:
            return None
        try:
            return json.loads(body)
        except json.JSONDecodeError:
            return body

    async def _acquire_rate_token(self) -> None:
        now = time.monotonic()
        elapsed = now - self._rate_last
        self._rate_last = now
        self._rate_tokens = min(self.rps_limit, self._rate_tokens + elapsed * self.rps_limit)
        if self._rate_tokens < 1:
            sleep = (1 - self._rate_tokens) / self.rps_limit
            await asyncio.sleep(max(0, sleep))
            self._rate_tokens = 0
        self._rate_tokens -= 1

    async def _upload_file(self, path: str) -> str:
        aiohttp = _import_aiohttp()
        if not self._session:
            raise MaxruAPIError("session not initialized")
        await self._acquire_rate_token()
        file_path = Path(path)
        with file_path.open("rb") as f:
            data = aiohttp.FormData()
            data.add_field("file", f, filename=file_path.name)
            async with self._session.post(self.api_url + "/uploads", data=data) as resp:
                result = await self._parse_response(resp)
        token = result.get("token") if isinstance(result, dict) else None
        if not token:
            raise MaxruAPIError(f"upload did not return token: {result}")
        return token

    async def _send_message_payload(self, payload: dict, params: Optional[dict] = None) -> SendResult:
        try:
            result = await self._api_post("/messages", payload, params=params)
            message_id = result.get("message_id") if isinstance(result, dict) else None
            return SendResult(success=True, message_id=str(message_id) if message_id else None)
        except Exception as e:
            logger.exception("maxru: send failed: %s", e)
            return SendResult(success=False, error=str(e))


# ----------------------------------------------------------------------------- #
# Standalone sender (for cron / send_message_tool outside the gateway process)
# ----------------------------------------------------------------------------- #

async def _standalone_send(
    pconfig,
    chat_id,
    message,
    *,
    thread_id=None,
    media_files=None,
    force_document=False,
) -> dict:
    """Send a text message without a running adapter."""
    aiohttp = _import_aiohttp()
    extra = (pconfig.extra or {}) if pconfig else {}
    token = extra.get("token") or os.getenv("MAXRU_TOKEN")
    api_url = (extra.get("api_url") or os.getenv("MAXRU_API_URL", "https://platform-api2.max.ru")).rstrip("/")
    if not token:
        return {"success": False, "error": "MAXRU_TOKEN not configured"}

    payload = {"text": message, "format": "markdown"}
    params = {"user_id": int(chat_id) if str(chat_id).isdigit() else chat_id}
    timeout = aiohttp.ClientTimeout(total=60)
    connector = aiohttp.TCPConnector(verify_ssl=False)
    async with aiohttp.ClientSession(headers={"Authorization": token}, timeout=timeout, connector=connector) as session:
        try:
            async with session.post(api_url + "/messages", params=params, json=payload) as resp:
                if resp.status >= 400:
                    body = await resp.text()
                    return {"success": False, "error": f"HTTP {resp.status}: {body[:200]}"}
                return {"success": True, "status": resp.status}
        except Exception as e:
            return {"success": False, "error": str(e)}
