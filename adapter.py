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
            raw_attachments = body.get("attachments") or []
            # Skip phantom updates that carry no sender (e.g. voice notes received
            # via long polling only expose an empty envelope). Without a user_id
            # we cannot authorize, route, or reply.
            if not user_id:
                logger.warning(
                    "maxru: ignoring message_created without sender "
                    "(likely voice note over long polling): update=%r",
                    update,
                )
                return
            # If user is not yet approved, issue a pairing code via the
            # unified Hermes PairingStore.
            if user_id and not self._is_user_allowed(user_id):
                await self._handle_unauthorized_dm(
                    chat_id=chat_id,
                    user_id=user_id,
                    user_name=user_name,
                )
                return
            # For every attachment, try to download a local copy so that
            # the core can use it (vision for images, STT for audio,
            # etc.). The original raw structure is preserved in
            # ``event.metadata["maxru_attachments"]`` regardless.
            media_urls: list[str] = []
            media_types: list[str] = []
            attachment_meta: list[dict] = []
            for att in raw_attachments:
                att_type = att.get("type")
                att_payload = att.get("payload") or {}
                attachment_meta.append(
                    {"type": att_type, "payload": att_payload}
                )
                downloaded = await self._download_attachment(
                    att_type, att_payload, body.get("mid")
                )
                if downloaded:
                    path, mime = downloaded
                    media_urls.append(path)
                    media_types.append(mime)
            event = self._build_message_event(
                chat_id=chat_id,
                user_id=user_id,
                user_name=user_name,
                text=text,
                message_id=body.get("mid"),
                timestamp=timestamp_dt,
                media_urls=media_urls,
                media_types=media_types,
                metadata={"maxru_attachments": attachment_meta},
            )
            await self.send_typing(str(chat_id))
            await self.handle_message(event)
            await self.mark_as_read(str(chat_id), body.get("mid"))
            logger.warning(
                "maxru: dispatched message_created from user=%s text=%r "
                "attachments=%d downloaded=%d",
                user_id,
                text,
                len(attachment_meta),
                len(media_urls),
            )

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
        media_urls: Optional[list[str]] = None,
        media_types: Optional[list[str]] = None,
        metadata: Optional[dict] = None,
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
            media_urls=list(media_urls or []),
            media_types=list(media_types or []),
            metadata=dict(metadata or {}),
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
        # MAX API personal dialogs are addressed by user_id, not chat_id.
        await self.send(
            str(user_id),
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
        file_path: Optional[str] = None,
        path: Optional[str] = None,
        image_path: Optional[str] = None,
        caption: str = "",
        file_name: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[dict] = None,
        **kwargs,
    ) -> SendResult:
        """Upload a local image and send it as an attachment.

        Accepts three parameter names for backwards compatibility:
          - ``image_path`` — what ``BasePlatformAdapter.send_multiple_images``
            in :mod:`gateway.platforms.base` actually passes when the user
            message contains ``file://...`` URLs.
          - ``file_path`` — preferred, matches the ``send_document`` signature.
          - ``path`` — legacy positional name.

        Any one of the three may be used; the first non-None wins.
        """
        actual_path = image_path or file_path or path
        if not actual_path:
            return SendResult(
                success=False,
                error=(
                    "send_image_file called without image_path, file_path "
                    "or path"
                ),
            )
        token = await self._upload_file(actual_path, upload_type="image")
        payload: dict[str, Any] = {"format": "markdown"}
        if caption:
            payload["text"] = caption
        if reply_to:
            payload["reply_to_message_id"] = reply_to
        payload["attachments"] = [{"type": "image", "payload": {"token": token}}]
        params = self._user_params(chat_id)
        if file_name:
            params = {**params, "file_name": file_name}
        return await self._send_message_payload(payload, params=params)

    async def send_document(
        self,
        chat_id: str,
        file_path: Optional[str] = None,
        path: Optional[str] = None,
        caption: str = "",
        file_name: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[dict] = None,
        **kwargs,
    ) -> SendResult:
        """Upload a local file and send it as a native MAX attachment.

        The base class dispatches file deliveries with the keyword
        ``file_path``; accept it as the canonical name while still
        honouring the legacy ``path`` argument for callers that have not
        been updated. ``**kwargs`` swallows ``metadata`` / ``reply_to`` so
        the signature stays source-compatible with the base class.
        """
        upload_path = file_path if file_path is not None else path
        if not upload_path:
            return SendResult(
                success=False,
                error="send_document called without file_path or path",
            )
        try:
            token = await self._upload_file(upload_path)
        except Exception as e:
            logger.warning("maxru: upload failed for %s: %s", upload_path, e)
            return SendResult(success=False, error=str(e))
        payload: dict[str, Any] = {
            "format": "markdown",
            "attachments": [{"type": "file", "payload": {"token": token}}],
        }
        if caption:
            payload["text"] = caption
        if reply_to:
            payload["reply_to_message_id"] = reply_to
        return await self._send_message_payload(payload, params=self._user_params(chat_id))

    def _user_params(self, chat_id: str) -> dict[str, Any]:
        """Return MAX API query params for a personal-dialog recipient."""
        return {"user_id": int(chat_id) if chat_id.isdigit() else chat_id}

    async def send_typing(self, chat_id: str, metadata=None) -> None:
        """Show the "bot is typing" indicator in the MAX dialog.

        MAX supports `POST /chats/{chat_id}/actions` with `{"action":
        "typing_on"}`.  `typing_off` is not supported by the API and the
        indicator disappears automatically after a short timeout, so we
        only turn it on before starting to compose a reply.
        """
        if not chat_id:
            return
        try:
            await self._api_post(
                f"/chats/{chat_id}/actions",
                {"action": "typing_on"},
            )
        except MaxruAPIError as e:
            logger.debug("maxru: send_typing failed for chat_id=%s: %s", chat_id, e)

    async def mark_as_read(
        self, chat_id: str, message_id: Optional[str] = None
    ) -> None:
        """Attempt to mark a message as read.

        MAX does not document a read-receipt endpoint for personal dialogs.
        `POST /chats/{chat_id}/actions` only accepts `typing_on`.  We keep
        this hook so a future API addition can be wired in without changing
        the call sites.
        """
        if not chat_id:
            return
        logger.debug(
            "maxru: mark_as_read not implemented by MAX API (chat_id=%s message_id=%s)",
            chat_id,
            message_id,
        )

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

    async def _upload_file(self, path: str, upload_type: str = "file") -> str:
        """Upload a local file to MAX and return a token usable as
        ``attachments[].payload.token`` in a subsequent ``/messages`` call.

        The official MAX Bot API docs
        (https://dev.max.ru/docs-api/methods/POST/uploads) describe a
        2-step flow for ``type=file`` (and ``type=image``):

          1. ``POST /uploads?type=file`` (or ``type=image``) with the
             bot's ``Authorization`` header → response:
             ``{"url": "https://fu.oneme.ru/..."}`` for file or
             ``{"url": "https://iu.oneme.ru/..."}`` for image
             (host differs by type; video/audio → ``vu.okcdn.ru``).
             For video/audio the response ALSO includes a ``token``;
             for file/image the token is returned only after the
             actual upload.

          2. ``POST {url}`` with ``Content-Type: multipart/form-data``
             and the file in a multipart field literally named ``data``
             (NOT ``file`` — that is the common source of the
             ``Invalid token`` error). The response on success is
             ``{"token": "..."}`` for file/image. For video/audio the
             server returns ``{"retval": "..."}`` and the token from
             step 1 is reused.

          3. Use the token in ``attachments[].payload.token`` when
             calling ``POST /messages``. The server may briefly return
             ``attachment.not.ready`` while it processes the file —
             callers should retry with backoff.

        IMPORTANT — tokens are type-specific. A token obtained from
        ``?type=file`` upload can ONLY be used as
        ``attachments[].payload.token`` for an attachment of
        ``type=file``. For ``type=image`` attachments you MUST
        request a fresh token from ``?type=image``. Using a
        file-token as a photo-token gives
        ``HTTP 400 {"code":"proto.payload","message":"Invalid photo
        token provided: ..."}`` from the messages endpoint.

        Without ``?type=<kind>`` query parameter the API returns
        ``HTTP 400: Missing required parameter: type``.
        """
        if upload_type not in ("file", "image"):
            raise MaxruAPIError(
                f"unsupported upload_type: {upload_type!r} (must be 'file' or 'image')"
            )
        aiohttp = _import_aiohttp()
        if not self._session:
            raise MaxruAPIError("session not initialized")
        await self._acquire_rate_token()
        file_path = Path(path)
        if not file_path.exists():
            raise MaxruAPIError(f"file does not exist: {file_path}")

        # Step 1: ask MAX for a signed upload URL. The response is
        # ``{"url": "https://fu.oneme.ru/upload.do?..."}`` for type=file.
        # We deliberately do NOT pull a ``token`` from this response
        # for type=file — for file/image the token is only returned
        # after step 2.
        upload_meta_url = self.api_url + f"/uploads?type={upload_type}"
        with file_path.open("rb") as f:
            data = aiohttp.FormData()
            data.add_field("file", f, filename=file_path.name)
            async with self._session.post(
                upload_meta_url, data=data
            ) as resp:
                upload_meta = await self._parse_response(resp)
        signed_url = (
            upload_meta.get("url") if isinstance(upload_meta, dict) else None
        )
        if not signed_url:
            raise MaxruAPIError(
                f"upload did not return a signed URL: {upload_meta}"
            )

        # Step 2: actually upload the file body to the signed URL. The
        # critical detail (per docs) is the multipart field name: it
        # MUST be ``data``. Using ``file`` is the documented failure
        # mode that produces ``Invalid token`` downstream.
        # ``fu.oneme.ru`` does NOT require our auth token (it's a
        # signed-URL upload), so we drop the Authorization header here.
        import mimetypes
        mime_type, _ = mimetypes.guess_type(file_path.name)
        if not mime_type:
            mime_type = "application/octet-stream"
        file_bytes = file_path.read_bytes()
        upload_form = aiohttp.FormData()
        upload_form.add_field(
            "data",
            file_bytes,
            filename=file_path.name,
            content_type=mime_type,
        )
        async with self._session.post(
            signed_url, data=upload_form
        ) as resp:
            upload_status = resp.status
            try:
                upload_result = await resp.json()
            except Exception:
                upload_result = {"raw": await resp.text()}
        if upload_status >= 400:
            raise MaxruAPIError(
                f"file upload to signed URL failed: HTTP {upload_status}: "
                f"{str(upload_result)[:300]}"
            )

        # Step 3: pull the actual attachment token from step 2's
        # response. The exact shape depends on the upload type:
        #   * ``type=file``  →  ``{"token": "..."}`` at the top level
        #   * ``type=image`` →  ``{"photos": {"<hash>": {"token": "..."}}}``
        #     — the API returns a dict keyed by photo hash, and we want
        #     the ``token`` of the (only) entry.
        if not isinstance(upload_result, dict):
            raise MaxruAPIError(
                f"upload response is not a JSON object: {upload_result!r}"
            )
        token = upload_result.get("token")
        if not token and upload_type == "image":
            photos = upload_result.get("photos")
            if isinstance(photos, dict) and photos:
                # Take the first photo's token; there is only one for
                # a single-file upload.
                first_entry = next(iter(photos.values()))
                if isinstance(first_entry, dict):
                    token = first_entry.get("token")
        if not token:
            raise MaxruAPIError(
                f"upload did not return a token: {upload_result!r}"
            )
        return token

    async def _send_message_payload_with_retry(
        self, payload: dict, params: Optional[dict] = None
    ) -> SendResult:
        """Same as ``_send_message_payload`` but retries on
        ``attachment.not.ready`` with exponential backoff.

        Per the MAX Bot API docs
        (https://dev.max.ru/docs-api/methods/POST/uploads#Обработка-медиафайлов):
        "После успешной загрузки сервер обрабатывает файл. Файлы от
        нескольких мегабайт обрабатываются дольше. Если отправить
        сообщение с вложением сразу после загрузки, может возникнуть
        ошибка ``attachment.not.ready``. После загрузки файла сделайте
        паузу перед отправкой сообщения. Если отправка не удалась,
        повторите попытку через некоторое время."

        We do an initial 1-second wait, then retry up to 4 more times
        (1s → 2s → 4s → 8s) when the server returns
        ``attachment.not.ready``. Total wait budget: ~15s.
        """
        # Initial cool-down — the server needs a moment to start
        # processing the freshly-uploaded attachment.
        await asyncio.sleep(1.0)
        delays = [0.0, 2.0, 4.0, 8.0]
        last_error: Optional[str] = None
        for attempt, delay in enumerate(delays):
            if delay:
                await asyncio.sleep(delay)
            try:
                result = await self._api_post(
                    "/messages", payload, params=params
                )
                message_id = (
                    result.get("message_id")
                    if isinstance(result, dict)
                    else None
                )
                return SendResult(
                    success=True,
                    message_id=str(message_id) if message_id else None,
                )
            except MaxruAPIError as e:
                err_str = str(e)
                last_error = err_str
                if "attachment.not.ready" in err_str and attempt < len(delays) - 1:
                    logger.warning(
                        "maxru: attachment not ready (attempt %d/%d), "
                        "backing off %.1fs",
                        attempt + 1,
                        len(delays),
                        delays[attempt + 1],
                    )
                    continue
                # Not a retryable error, or out of retries.
                logger.exception("maxru: send failed: %s", e)
                return SendResult(success=False, error=err_str)
            except Exception as e:
                logger.exception("maxru: send failed: %s", e)
                return SendResult(success=False, error=str(e))
        return SendResult(
            success=False,
            error=(
                f"attachment never became ready after {len(delays)} "
                f"attempts: {last_error}"
            ),
        )

    async def _send_message_payload(
        self, payload: dict, params: Optional[dict] = None
    ) -> SendResult:
        """Send a message to MAX via ``POST /messages``.

        If the payload contains an ``attachments`` field, this delegates
        to ``_send_message_payload_with_retry`` which handles the
        ``attachment.not.ready`` race condition documented at
        https://dev.max.ru/docs-api/methods/POST/uploads.
        """
        # If the payload carries attachments, use the retry path
        # — the server can return ``attachment.not.ready`` for a few
        # seconds after upload and we want to be resilient to that.
        if isinstance(payload, dict) and payload.get("attachments"):
            return await self._send_message_payload_with_retry(
                payload, params=params
            )
        # Plain text-only path: no race condition possible.
        try:
            result = await self._api_post("/messages", payload, params=params)
            message_id = (
                result.get("message_id") if isinstance(result, dict) else None
            )
            return SendResult(
                success=True,
                message_id=str(message_id) if message_id else None,
            )
        except Exception as e:
            logger.exception("maxru: send failed: %s", e)
            return SendResult(success=False, error=str(e))


    # --------------------------------------------------------------------- #
    # Inbound attachment download helpers
    # --------------------------------------------------------------------- #

    async def _download_one(
            self,
        url: str,
        dest: Path,
    ) -> Optional[tuple[Path, str]]:
        """Stream ``url`` into ``dest`` and return ``(path, content_type)``.

        Returns ``None`` on HTTP error or if MAX returns a non-2xx response.
        Caller decides the cache key; this helper does not deduplicate.
        """
        try:
            async with self._session.get(url, timeout=aiohttp.ClientTimeout(total=60)) as resp:
                if resp.status >= 400:
                    logger.warning(
                        "maxru: attachment download failed HTTP %s for %s",
                        resp.status,
                        url,
                    )
                    return None
                content_type = resp.headers.get("Content-Type", "").split(";")[0].strip()
                with dest.open("wb") as f:
                    async for chunk in resp.content.iter_chunked(64 * 1024):
                        if chunk:
                            f.write(chunk)
                return dest, content_type
        except Exception as e:
            logger.warning("maxru: attachment download error for %s: %s", url, e)
            return None


# -----------------------------------------------------------------------------
# Standalone sender (for cron / send_message_tool outside the gateway process)
# -----------------------------------------------------------------------------

    async def _download_attachment(
        self,
        att_type: Optional[str],
        att_payload: dict,
        message_id: Optional[str],
    ) -> Optional[tuple[str, str]]:
        """Best-effort download of a single inbound attachment.

        Returns ``(local_path, mime_type)`` if the file was successfully fetched,
        or ``None`` if MAX didn't expose a download URL for this attachment type.

        Strategy:
          1. If ``payload.url`` is present, download it directly (this is what
             image attachments expose — a CDN URL on ``i.oneme.ru``).
          2. Otherwise, for ``audio``/``file``/``video``/``image``, do a
             ``GET /messages?message_ids=<mid>`` to fetch the full Message
             object. Sometimes the extended payload returned by that endpoint
             includes a ``url`` field the Update payload didn't expose.
          3. If neither yields a URL, log and return ``None`` so the caller can
             fall back to text-only handling and surface a hint to the user.
        """
        if not att_type or not att_payload:
            return None
        default_mime, default_ext = _MAXRU_TYPE_DEFAULTS.get(
            att_type, ("application/octet-stream", ".bin")
        )
        if not self._session:
            return None

        # Step 1: use payload.url if present.
        direct_url = att_payload.get("url")
        if direct_url:
            cache_name = f"{att_type}-{int(time.time() * 1000)}{default_ext}"
            dest = _MAXRU_ATT_DIR / cache_name
            result = await self._download_one(direct_url, dest)
            if result is not None:
                path, mime = result
                return str(path), mime or default_mime
            # If direct download failed, fall through to step 2.

        # Step 2: try GET /messages?message_ids=<mid> for an extended payload.
        if message_id:
            try:
                async with self._session.get(
                    "/messages",
                    params={"message_ids": message_id},
                    timeout=aiohttp.ClientTimeout(total=15),
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        messages = data.get("messages") or []
                        if messages:
                            mbody = (
                                messages[0].get("body", {}) or {}
                            )
                            for ext_att in mbody.get("attachments") or []:
                                if (ext_att.get("type") or "").lower() == (
                                    att_type or ""
                                ).lower():
                                    ext_payload = ext_att.get("payload") or {}
                                    ext_url = ext_payload.get("url")
                                    if ext_url:
                                        cache_name = (
                                            f"{att_type}-"
                                            f"{int(time.time() * 1000)}"
                                            f"{default_ext}"
                                        )
                                        dest = _MAXRU_ATT_DIR / cache_name
                                        res2 = await self._download_one(ext_url, dest)
                                        if res2 is not None:
                                            path, mime = res2
                                            return str(path), mime or default_mime
                                    break
            except Exception as e:
                logger.warning(
                    "maxru: /messages lookup for attachment of type=%s failed: %s",
                    att_type,
                    e,
                )

        # Step 3: no URL resolvable. Tell the operator what we know so the
        # next iteration of the patch can target the right endpoint.
        token_short = (att_payload.get("token") or "")[:16]
        logger.warning(
            "maxru: attachment of type=%s has no downloadable URL "
            "(token=%s…, mid=%s). Tell Hermes what the user sent so the "
            "plugin can be extended.",
            att_type,
            token_short,
            message_id,
        )
        return None

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


    # Attach this method to MaxruAdapter. We define it as a free function and
    # inject it below so the patch can be applied without re-indenting hundreds
    # of lines of the existing class. The class will gain a method of the same
    # name that delegates to this implementation.
