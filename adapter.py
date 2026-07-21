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

    # --------------------------------------------------------------------- #
    # Lifecycle
    # --------------------------------------------------------------------- #

    async def connect(self) -> bool:
        aiohttp = _import_aiohttp()
        if not self.token:
            logger.error("maxru: MAXRU_TOKEN not set")
            return False

        self._session = aiohttp.ClientSession(
            headers={"Authorization": self.token},
            timeout=aiohttp.ClientTimeout(total=60),
        )

        # Verify token with /me
        try:
            me = await self._api_get("/me")
            logger.info("maxru: connected as %s", me.get("username", me.get("user_id")))
        except MaxruAPIError as e:
            logger.error("maxru: /me failed: %s", e)
            return False

        if self.long_polling:
            self._stop_event.clear()
            self._poll_task = asyncio.create_task(self._poll_loop())
        elif self.webhook_url:
            await self._register_webhook()

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
                    params["offset"] = self._last_update_id + 1

                updates = await self._api_get("/updates", params=params)
                for update in updates or []:
                    await self._handle_update(update)
                    update_id = update.get("update_id")
                    if update_id is not None:
                        self._last_update_id = update_id
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
        chat_id = update.get("chat_id")
        user = update.get("user") or {}
        user_id = str(user.get("user_id", ""))
        user_name = user.get("name", "")

        if not self._is_user_allowed(user_id):
            logger.debug("maxru: user %s not allowed", user_id)
            return

        timestamp_ms = update.get("timestamp")
        timestamp_dt = None
        if isinstance(timestamp_ms, (int, float)) and timestamp_ms > 0:
            timestamp_dt = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)

        if update_type == "message_created":
            message = update.get("message", {})
            body = message.get("body", {}) or {}
            text = body.get("text", "")
            event = self._build_message_event(
                chat_id=chat_id,
                user_id=user_id,
                user_name=user_name,
                text=text,
                message_id=message.get("message_id"),
                timestamp=timestamp_dt,
            )
            await self.handle_message(event)

        elif update_type == "bot_started":
            payload = update.get("payload", "")
            greeting = "Привет!" if not payload else f"Привет! Диплинк: {payload}"
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
            chat_id=str(chat_id),
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
        if self.allow_all_users:
            return True
        if user_id in self.allowed_users:
            return True
        return False

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
        """Send a plain text message."""
        if not content:
            return SendResult(success=True, message_id=None)

        payload: dict[str, Any] = {
            "chat_id": chat_id,
            "text": content,
        }
        if reply_to:
            payload["reply_to_message_id"] = reply_to

        return await self._send_message_payload(payload)

    async def send_image(
        self,
        chat_id: str,
        image_url: str,
        caption: str = "",
        **kwargs,
    ) -> SendResult:
        """Send an image by URL (no upload needed for images)."""
        payload: dict[str, Any] = {"chat_id": chat_id}
        if caption:
            payload["text"] = caption
        payload["attachments"] = [{"type": "image", "payload": {"url": image_url}}]
        return await self._send_message_payload(payload)

    async def send_image_file(
        self,
        chat_id: str,
        path: str,
        caption: str = "",
        **kwargs,
    ) -> SendResult:
        """Upload a local image and send it."""
        token = await self._upload_file(path)
        payload: dict[str, Any] = {"chat_id": chat_id}
        if caption:
            payload["text"] = caption
        payload["attachments"] = [{"type": "image", "payload": {"token": token}}]
        return await self._send_message_payload(payload)

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
            "chat_id": chat_id,
            "attachments": [{"type": "file", "payload": {"token": token}}],
        }
        if caption:
            payload["text"] = caption
        return await self._send_message_payload(payload)

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
            "attachments": [
                {
                    "type": "inline_keyboard",
                    "payload": {"buttons": buttons},
                }
            ],
        }
        return await self._send_message_payload(payload)

    # --------------------------------------------------------------------- #
    # API helpers
    # --------------------------------------------------------------------- #

    async def _api_get(self, path: str, params: Optional[dict] = None) -> Any:
        await self._acquire_rate_token()
        if not self._session:
            raise MaxruAPIError("session not initialized")
        async with self._session.get(self.api_url + path, params=params) as resp:
            return await self._parse_response(resp)

    async def _api_post(self, path: str, json_data: dict) -> Any:
        await self._acquire_rate_token()
        if not self._session:
            raise MaxruAPIError("session not initialized")
        async with self._session.post(self.api_url + path, json=json_data) as resp:
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

    async def _send_message_payload(self, payload: dict) -> SendResult:
        try:
            result = await self._api_post("/messages", payload)
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

    payload = {"chat_id": chat_id, "text": message}
    timeout = aiohttp.ClientTimeout(total=60)
    async with aiohttp.ClientSession(headers={"Authorization": token}, timeout=timeout) as session:
        try:
            async with session.post(api_url + "/messages", json=payload) as resp:
                if resp.status >= 400:
                    body = await resp.text()
                    return {"success": False, "error": f"HTTP {resp.status}: {body[:200]}"}
                return {"success": True, "status": resp.status}
        except Exception as e:
            return {"success": False, "error": str(e)}
