"""钉钉 Stream 模式主入口（多机器人版）。

功能：
- 通过 WebSocket 长连接接收钉钉 @机器人 消息（手机/PC 均可）
- 支持同一进程运行多个钉钉机器人，各自独立配置或共享 Claude CLI 实例
- 按 staffId 白名单鉴权
- 将消息转交给 Claude Code CLI，并优先用 AI 卡片流式展示（打字机效果）
- 卡片模板 ID 未配置或创建卡片失败时，自动降级为 Markdown 消息
- 多会话上下文隔离 + `--resume` 续接
- 支持图片+文字的富文本消息（下载图片后让 Claude 读取分析）
"""

from __future__ import annotations

import asyncio
import collections
import configparser
import hashlib
import json
import logging
import os
import re
import signal
import tempfile
import threading
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import httpx
from dingtalk_stream import (
    AckMessage,
    CallbackHandler,
    ChatbotHandler,
    ChatbotMessage,
    Credential,
    DingTalkStreamClient,
)
from dingtalk_stream.card_callback import Card_Callback_Router_Topic
from dingtalk_stream.chatbot import HostingContext

from card_stream import CardPermissionError, StreamingCardReplier
from cli_bridge import ClaudeCliBridge, ProcessHolder, SessionStore
from cron_scheduler import CronScheduler, extract_cron_commands

MAX_MESSAGE_CHARS = 4000
MAX_REGEN_ENTRIES = 200
IMAGE_DOWNLOAD_DIR = Path(tempfile.gettempdir()) / "ding-bridge-images"


class RegenStore:
    """存储最近 N 张卡片的上下文，用于"重新回答"按钮的回调重放。"""

    def __init__(self, maxlen: int = MAX_REGEN_ENTRIES) -> None:
        self._store: collections.OrderedDict[str, dict] = collections.OrderedDict()
        self._maxlen = maxlen

    def save(self, card_instance_id: str, context: dict) -> None:
        if card_instance_id in self._store:
            self._store.move_to_end(card_instance_id)
        self._store[card_instance_id] = context
        while len(self._store) > self._maxlen:
            self._store.popitem(last=False)

    def get(self, card_instance_id: str) -> dict | None:
        return self._store.get(card_instance_id)


class _SdkLogArgsFixFilter(logging.Filter):
    """修复 dingtalk_stream SDK 里 `logger.exception('unknown exception', e)` 的误用。"""

    def filter(self, record: logging.LogRecord) -> bool:
        if record.args:
            msg = record.msg if isinstance(record.msg, str) else str(record.msg)
            if "%" not in msg:
                try:
                    suffix = " ".join(repr(a) for a in record.args)
                except Exception:  # noqa: BLE001
                    suffix = "<unprintable args>"
                record.msg = f"{msg}: {suffix}"
                record.args = ()
        return True


def _patch_sdk_logger() -> None:
    sdk_logger = logging.getLogger("dingtalk_stream.client")
    if not any(isinstance(f, _SdkLogArgsFixFilter) for f in sdk_logger.filters):
        sdk_logger.addFilter(_SdkLogArgsFixFilter())


def _setup_logging(level: str) -> logging.Logger:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )
    _patch_sdk_logger()
    return logging.getLogger("ding-bridge")


def _parse_csv(raw: Optional[str]) -> set[str]:
    if not raw:
        return set()
    results = set()
    for item in raw.split(","):
        item = item.strip()
        if not item:
            continue
        # 支持 "名字:staffId" 格式，取冒号后的部分作为 ID
        if ":" in item:
            item = item.split(":", 1)[1].strip()
        results.add(item)
    return results


# ─── 配置 ───────────────────────────────────────────────────────────────────


@dataclass
class BotSettings:
    """单个机器人的完整配置（dingtalk 凭证 + claude 参数 + 安全）。"""

    name: str

    # dingtalk
    client_id: str
    client_secret: str
    robot_code: str
    card_template_id: str = ""
    card_content_key: str = "content"

    # security
    allowed_staff_ids: set[str] = field(default_factory=set)

    # claude (per-bot override, fallback to global)
    cli_path: str = "claude"
    cwd: str = ""
    model: str = ""
    permission_mode: str = "bypassPermissions"
    allowed_tools: str = ""
    disallowed_tools: str = ""
    append_system_prompt: str = ""

    # reaction
    enable_reaction: bool = True

    # runtime (per-bot override)
    cli_timeout_seconds: int = 300


class Settings:
    """从 config.ini 读取配置，支持多 [bot:*] section。"""

    def __init__(self, config_path: str | None = None) -> None:
        if config_path is None:
            config_path = str(Path(__file__).parent / "config.ini")
        if not os.path.isfile(config_path):
            raise RuntimeError(f"配置文件不存在: {config_path}")
        self._config_path = config_path
        self._load()

    def _load(self) -> None:
        cfg = configparser.ConfigParser()
        cfg.read(self._config_path, encoding="utf-8")

        # ── 全局 [claude] 默认值 ──
        self.claude_cli_path: str = cfg.get("claude", "cli_path", fallback="claude")
        self.claude_cwd: str = cfg.get("claude", "cwd", fallback="") or os.getcwd()
        self.claude_model: str = cfg.get("claude", "model", fallback="")
        self.claude_permission_mode: str = cfg.get("claude", "permission_mode", fallback="bypassPermissions")
        self.claude_allowed_tools: str = cfg.get("claude", "allowed_tools", fallback="")
        self.claude_disallowed_tools: str = cfg.get("claude", "disallowed_tools", fallback="")
        self.claude_append_system_prompt: str = cfg.get("claude", "append_system_prompt", fallback="")

        # ── 全局 [runtime] ──
        self.cli_timeout_seconds: int = cfg.getint("runtime", "cli_timeout_seconds", fallback=300)
        self.session_store_dir: str = cfg.get("runtime", "session_store_dir", fallback=".sessions")
        self.log_level: str = cfg.get("runtime", "log_level", fallback="INFO")

        # ── 解析 bot sections ──
        self.bots: list[BotSettings] = []
        bot_sections = [s for s in cfg.sections() if s.startswith("bot:")]

        if bot_sections:
            for section in bot_sections:
                bot_name = section[4:]  # strip "bot:" prefix
                self.bots.append(self._parse_bot_section(cfg, section, bot_name))
        elif cfg.has_section("dingtalk"):
            # 向后兼容旧格式
            self.bots.append(self._parse_legacy_bot(cfg))

        if not self.bots:
            raise RuntimeError(
                "未找到任何机器人配置。请在 config.ini 中添加 [bot:name] section，"
                "或保留旧的 [dingtalk] section。"
            )

    def _parse_bot_section(self, cfg: configparser.ConfigParser, section: str, bot_name: str) -> BotSettings:
        client_id = cfg.get(section, "client_id", fallback="")
        if not client_id:
            raise RuntimeError(f"[{section}] client_id 未设置")
        client_secret = cfg.get(section, "client_secret", fallback="")
        if not client_secret:
            raise RuntimeError(f"[{section}] client_secret 未设置")

        return BotSettings(
            name=bot_name,
            client_id=client_id,
            client_secret=client_secret,
            robot_code=cfg.get(section, "robot_code", fallback="") or client_id,
            card_template_id=cfg.get(section, "card_template_id", fallback=""),
            card_content_key=cfg.get(section, "card_content_key", fallback="content"),
            allowed_staff_ids=_parse_csv(cfg.get(section, "allowed_staff_ids", fallback="")),
            # claude overrides — fallback to global
            cli_path=cfg.get(section, "cli_path", fallback="") or self.claude_cli_path,
            cwd=cfg.get(section, "cwd", fallback="") or self.claude_cwd,
            model=cfg.get(section, "model", fallback="") or self.claude_model,
            permission_mode=cfg.get(section, "permission_mode", fallback="") or self.claude_permission_mode,
            allowed_tools=cfg.get(section, "allowed_tools", fallback="") or self.claude_allowed_tools,
            disallowed_tools=cfg.get(section, "disallowed_tools", fallback="") or self.claude_disallowed_tools,
            append_system_prompt=cfg.get(section, "append_system_prompt", fallback="") or self.claude_append_system_prompt,
            cli_timeout_seconds=cfg.getint(section, "cli_timeout_seconds", fallback=0) or self.cli_timeout_seconds,
            enable_reaction=cfg.getboolean(section, "enable_reaction", fallback=True),
        )

    def _parse_legacy_bot(self, cfg: configparser.ConfigParser) -> BotSettings:
        """兼容旧的 [dingtalk] + [security] 格式，转换为一个 BotSettings。"""
        client_id = cfg.get("dingtalk", "client_id", fallback="")
        if not client_id:
            raise RuntimeError("[dingtalk] client_id 未设置")
        client_secret = cfg.get("dingtalk", "client_secret", fallback="")
        if not client_secret:
            raise RuntimeError("[dingtalk] client_secret 未设置")

        return BotSettings(
            name="default",
            client_id=client_id,
            client_secret=client_secret,
            robot_code=cfg.get("dingtalk", "robot_code", fallback="") or client_id,
            card_template_id=cfg.get("dingtalk", "card_template_id", fallback=""),
            card_content_key=cfg.get("dingtalk", "card_content_key", fallback="content"),
            allowed_staff_ids=_parse_csv(cfg.get("security", "allowed_staff_ids", fallback="")),
            cli_path=self.claude_cli_path,
            cwd=self.claude_cwd,
            model=self.claude_model,
            permission_mode=self.claude_permission_mode,
            allowed_tools=self.claude_allowed_tools,
            disallowed_tools=self.claude_disallowed_tools,
            append_system_prompt=self.claude_append_system_prompt,
            cli_timeout_seconds=self.cli_timeout_seconds,
            enable_reaction=cfg.getboolean("dingtalk", "enable_reaction", fallback=True),
        )

    def reload(self) -> None:
        self._load()


# ─── 消息处理 ─────────────────────────────────────────────────────────────────


class BridgeHandler(ChatbotHandler):
    """钉钉消息回调处理器。"""

    def __init__(
        self,
        bot_settings: BotSettings,
        bridge: ClaudeCliBridge,
        logger: logging.Logger,
        regen_store: RegenStore,
        cron_scheduler: CronScheduler | None = None,
    ) -> None:
        super().__init__()
        self.bot_settings = bot_settings
        self.bridge = bridge
        self.logger = logger
        self.regen_store = regen_store
        self.cron_scheduler = cron_scheduler
        # conversation_id -> ProcessHolder，用于中断正在进行的对话
        self._active_holders: dict[str, ProcessHolder] = {}
        # access_token 缓存：(token, expire_timestamp)
        self._token_cache: tuple[str, float] = ("", 0.0)

    async def process(self, callback) -> tuple[int, str]:
        try:
            return await self._process_inner(callback)
        except Exception as exc:  # noqa: BLE001
            self.logger.exception("[%s] process callback failed: %s", self.bot_settings.name, exc)
            return AckMessage.STATUS_OK, "OK"

    async def _process_inner(self, callback) -> tuple[int, str]:
        incoming = ChatbotMessage.from_dict(callback.data)
        raw_data = callback.data

        sender = getattr(incoming, "sender_staff_id", None) or getattr(
            incoming, "senderStaffId", ""
        )
        nick = getattr(incoming, "sender_nick", None) or ""
        conversation_id = (
            getattr(incoming, "conversation_id", None)
            or getattr(incoming, "conversationId", "")
            or "default"
        )

        user_text, image_download_codes, file_infos = self._extract_message_content(incoming, raw_data)

        self.logger.info(
            "[%s] incoming: staffId=%s nick=%s conv=%s text=%r images=%d files=%d",
            self.bot_settings.name,
            sender,
            nick,
            conversation_id,
            user_text[:200],
            len(image_download_codes),
            len(file_infos),
        )

        if self.bot_settings.allowed_staff_ids and sender not in self.bot_settings.allowed_staff_ids:
            self.logger.warning("[%s] reject unauthorized staffId=%s", self.bot_settings.name, sender)
            self.reply_text(
                f"抱歉，你（{nick or sender}）不在白名单内。"
                f"如需使用，请让管理员把你的 staffId `{sender}` 加入 config.ini [bot:{self.bot_settings.name}] allowed_staff_ids。",
                incoming,
            )
            return AckMessage.STATUS_OK, "OK"

        # /stop 指令：中断当前会话正在执行的 CLI 进程
        if user_text.strip() in {"/stop", "停止", "中断"}:
            holder = self._active_holders.get(conversation_id)
            if holder and not holder.cancelled:
                asyncio.create_task(holder.interrupt())
                self.reply_text("⏹️ 正在中断当前对话，请稍候...", incoming)
            else:
                self.reply_text("当前没有正在执行的任务。", incoming)
            return AckMessage.STATUS_OK, "OK"

        # /cron 指令：管理定时任务
        if user_text.strip().startswith("/cron") and self.cron_scheduler:
            self._handle_cron_command(
                user_text.strip(), conversation_id, sender, incoming,
                conversation_type=getattr(incoming, "conversation_type", "1") or "1",
            )
            return AckMessage.STATUS_OK, "OK"

        if not user_text and not image_download_codes and not file_infos:
            self.reply_text("请发送文本消息、图片或文件，例如：@MyAI 你好", incoming)
            return AckMessage.STATUS_OK, "OK"

        msg_id = raw_data.get("msgId") or getattr(incoming, "message_id", "") or ""

        task = asyncio.create_task(
            self._handle_in_background(
                user_text=user_text,
                image_download_codes=image_download_codes,
                file_infos=file_infos,
                conversation_id=conversation_id,
                incoming=incoming,
                msg_id=msg_id,
            )
        )
        task.add_done_callback(self._log_task_exception)
        return AckMessage.STATUS_OK, "OK"

    def _log_task_exception(self, task: "asyncio.Task[None]") -> None:
        if task.cancelled():
            return
        exc = task.exception()
        if exc is not None:
            self.logger.exception("[%s] background handler task failed: %r", self.bot_settings.name, exc, exc_info=exc)

    @staticmethod
    def _extract_message_content(
        incoming: ChatbotMessage, raw_data: dict
    ) -> tuple[str, list[str], list[dict]]:
        """解析消息内容，返回 (文本, 图片download_codes, 文件信息列表)。

        文件信息格式: {"downloadCode": "...", "fileName": "xxx.java"}
        """
        content = raw_data.get("content") or {}
        msgtype = raw_data.get("msgtype") or ""

        # 单独发送的文件消息
        if msgtype == "file":
            download_code = content.get("downloadCode") or ""
            file_name = content.get("fileName") or "unknown_file"
            if download_code:
                return "", [], [{"downloadCode": download_code, "fileName": file_name}]
            return "", [], []

        # 单独发送的图片消息
        if msgtype == "picture":
            download_code = content.get("downloadCode") or ""
            if download_code:
                return "", [download_code], []
            return "", [], []

        # 图文混排消息（richText）
        rich_text_items = content.get("richText")
        if rich_text_items and isinstance(rich_text_items, list):
            texts: list[str] = []
            download_codes: list[str] = []
            for item in rich_text_items:
                if not isinstance(item, dict):
                    continue
                if item.get("type") == "picture":
                    code = item.get("downloadCode") or ""
                    if code:
                        download_codes.append(code)
                elif "text" in item:
                    t = item["text"].strip()
                    if t:
                        texts.append(t)
            return " ".join(texts), download_codes, []

        # 纯文本消息
        text_obj = getattr(incoming, "text", None)
        if text_obj is not None:
            text_content = getattr(text_obj, "content", None)
            if isinstance(text_content, str) and text_content.strip():
                return text_content.strip(), [], []

        return "", [], []

    async def _download_files(
        self,
        download_codes: list[str],
        is_image: bool = True,
        file_names: Optional[list[str]] = None,
    ) -> list[str]:
        """下载图片或文件，返回本地临时路径列表。"""
        if not download_codes:
            return []

        IMAGE_DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
        access_token = await self._get_dingtalk_access_token()
        if not access_token:
            self.logger.error("[%s] failed to get access token for file download", self.bot_settings.name)
            return []

        paths: list[str] = []
        async with httpx.AsyncClient(timeout=60) as client:
            for idx, code in enumerate(download_codes):
                try:
                    resp = await client.post(
                        "https://api.dingtalk.com/v1.0/robot/messageFiles/download",
                        headers={"x-acs-dingtalk-access-token": access_token},
                        json={
                            "downloadCode": code,
                            "robotCode": self.bot_settings.robot_code,
                        },
                    )
                    if resp.status_code != 200:
                        self.logger.warning(
                            "[%s] file download API returned %d: %s",
                            self.bot_settings.name,
                            resp.status_code,
                            resp.text[:200],
                        )
                        continue

                    data = resp.json()
                    download_url = data.get("downloadUrl") or ""
                    if not download_url:
                        self.logger.warning("[%s] no downloadUrl in response: %s", self.bot_settings.name, data)
                        continue

                    file_resp = await client.get(download_url)
                    if file_resp.status_code != 200:
                        self.logger.warning("[%s] file fetch failed: %d", self.bot_settings.name, file_resp.status_code)
                        continue

                    # 确定文件名
                    if is_image:
                        content_type = file_resp.headers.get("content-type", "")
                        ext = ".png"
                        if "jpeg" in content_type or "jpg" in content_type:
                            ext = ".jpg"
                        elif "gif" in content_type:
                            ext = ".gif"
                        elif "webp" in content_type:
                            ext = ".webp"
                        filename = hashlib.md5(code.encode()).hexdigest()[:16] + ext
                    else:
                        original_name = (file_names[idx] if file_names and idx < len(file_names) else "unknown")
                        prefix = hashlib.md5(code.encode()).hexdigest()[:8]
                        filename = f"{prefix}_{original_name}"

                    filepath = IMAGE_DOWNLOAD_DIR / filename
                    filepath.write_bytes(file_resp.content)
                    paths.append(str(filepath))
                    self.logger.info("[%s] downloaded file: %s (%d bytes)", self.bot_settings.name, filepath, len(file_resp.content))

                except Exception as exc:  # noqa: BLE001
                    self.logger.warning("[%s] file download failed: %s", self.bot_settings.name, exc)

        return paths

    async def _get_dingtalk_access_token(self) -> str:
        cached_token, expire_at = self._token_cache
        if cached_token and time.time() < expire_at:
            return cached_token

        try:
            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.post(
                    "https://api.dingtalk.com/v1.0/oauth2/accessToken",
                    json={
                        "appKey": self.bot_settings.client_id,
                        "appSecret": self.bot_settings.client_secret,
                    },
                )
                if resp.status_code == 200:
                    token = resp.json().get("accessToken", "")
                    if token:
                        self._token_cache = (token, time.time() + 5400)  # 缓存 1.5h
                    return token
                self.logger.warning("[%s] get access token failed: %d %s", self.bot_settings.name, resp.status_code, resp.text[:200])
        except Exception as exc:  # noqa: BLE001
            self.logger.warning("[%s] get access token exception: %s", self.bot_settings.name, exc)
        return ""

    async def _add_reaction(self, conversation_id: str, msg_id: str, emoji_name: str) -> None:
        if not msg_id:
            return
        try:
            token = await self._get_dingtalk_access_token()
            if not token:
                return
            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.post(
                    "https://api.dingtalk.com/v1.0/robot/emotion/reply",
                    headers={"x-acs-dingtalk-access-token": token},
                    json={
                        "openConversationId": conversation_id,
                        "openMsgId": msg_id,
                        "emotionName": emoji_name,
                        "emotionType": 1,
                        "robotCode": self.bot_settings.robot_code,
                    },
                )
                if resp.status_code != 200:
                    self.logger.warning("[%s] add_reaction %s failed: %d %s", self.bot_settings.name, emoji_name, resp.status_code, resp.text[:500])
        except Exception as exc:  # noqa: BLE001
            self.logger.warning("[%s] add_reaction exception: %s", self.bot_settings.name, exc)

    async def _remove_reaction(self, conversation_id: str, msg_id: str, emoji_name: str) -> None:
        if not msg_id:
            return
        try:
            token = await self._get_dingtalk_access_token()
            if not token:
                return
            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.post(
                    "https://api.dingtalk.com/v1.0/robot/emotion/recall",
                    headers={"x-acs-dingtalk-access-token": token},
                    json={
                        "openConversationId": conversation_id,
                        "openMsgId": msg_id,
                        "emotionName": emoji_name,
                        "emotionType": 1,
                        "robotCode": self.bot_settings.robot_code,
                    },
                )
                if resp.status_code != 200:
                    self.logger.warning("[%s] remove_reaction %s failed: %d %s", self.bot_settings.name, emoji_name, resp.status_code, resp.text[:500])
        except Exception as exc:  # noqa: BLE001
            self.logger.warning("[%s] remove_reaction exception: %s", self.bot_settings.name, exc)

    async def _handle_in_background(
        self,
        user_text: str,
        image_download_codes: list[str],
        file_infos: list[dict],
        conversation_id: str,
        incoming: ChatbotMessage,
        msg_id: str = "",
    ) -> None:
        use_reaction = bool(msg_id and self.bot_settings.enable_reaction)
        if use_reaction:
            await self._add_reaction(conversation_id, msg_id, "Hmm…")

        # 注册 ProcessHolder，供 /stop 中断使用
        holder = ProcessHolder()
        self._active_holders[conversation_id] = holder

        sender = getattr(incoming, "sender_staff_id", None) or getattr(incoming, "senderStaffId", "")
        conv_type = getattr(incoming, "conversation_type", "1") or "1"

        image_paths = await self._download_files(image_download_codes, is_image=True)
        file_paths = await self._download_files(
            [f["downloadCode"] for f in file_infos],
            is_image=False,
            file_names=[f.get("fileName", "unknown") for f in file_infos],
        )
        all_attachment_paths = image_paths + file_paths
        final_prompt = self._build_prompt_with_attachments(user_text, image_paths, file_paths)

        success = False
        try:
            card_template_id = self.bot_settings.card_template_id
            if card_template_id:
                handled = await self._process_with_card(
                    user_text=final_prompt,
                    conversation_id=conversation_id,
                    incoming=incoming,
                    card_template_id=card_template_id,
                    content_key=self.bot_settings.card_content_key,
                    holder=holder,
                    staff_id=sender,
                    conversation_type=conv_type,
                )
                if handled:
                    success = True
                    return

            await self._process_with_markdown(
                user_text=final_prompt,
                conversation_id=conversation_id,
                incoming=incoming,
                holder=holder,
                staff_id=sender,
                conversation_type=conv_type,
            )
            success = True
        finally:
            self._active_holders.pop(conversation_id, None)
            for path in all_attachment_paths:
                try:
                    os.remove(path)
                except OSError:
                    pass
            if use_reaction:
                emoji = "OK" if success else ("Facepalm" if not holder.cancelled else "OK")
                await self._remove_reaction(conversation_id, msg_id, "Hmm…")
                await self._add_reaction(conversation_id, msg_id, emoji)

    @staticmethod
    def _build_prompt_with_attachments(
        user_text: str, image_paths: list[str], file_paths: list[str]
    ) -> str:
        if not image_paths and not file_paths:
            return user_text

        parts: list[str] = []

        if image_paths:
            refs = "\n".join(f"- {p}" for p in image_paths)
            parts.append(f"用户发送了 {len(image_paths)} 张图片，已保存到以下路径：\n{refs}\n请先用 Read 工具读取图片内容。")

        if file_paths:
            refs = "\n".join(f"- {p}" for p in file_paths)
            parts.append(f"用户发送了 {len(file_paths)} 个文件，已保存到以下路径：\n{refs}\n请先用 Read 工具读取文件内容。")

        text_part = user_text or "请分析这些文件"
        parts.append(f"用户的文字消息：{text_part}")

        return "\n\n".join(parts)

    def _handle_cron_command(
        self,
        text: str,
        conversation_id: str,
        staff_id: str,
        incoming: ChatbotMessage,
        conversation_type: str = "1",
    ) -> None:
        """处理 /cron 命令。

        用法:
        - /cron add "0 9 * * *" 检查待办     创建定时任务
        - /cron add 30m 提醒我喝水            30 分钟后一次性提醒
        - /cron list                          查看任务列表
        - /cron remove <id 或名称>            删除任务
        """
        scheduler = self.cron_scheduler
        if not scheduler:
            self.reply_text("⚠️ 定时任务功能未启用。", incoming)
            return

        parts = text.split(maxsplit=2)
        # /cron → 显示帮助
        if len(parts) < 2:
            self.reply_text(
                "⏰ **定时任务命令**\n\n"
                "- `/cron add <schedule> <prompt>` — 创建\n"
                "  例: `/cron add \"0 9 * * *\" 检查今日待办`\n"
                "  例: `/cron add 30m 提醒我喝水`\n"
                "  例: `/cron add \"every 2h\" 检查服务状态`\n"
                "- `/cron list` — 查看任务\n"
                "- `/cron remove <id或名称>` — 删除任务\n\n"
                "💡 你也可以用自然语言，比如\"每天 9 点提醒我看待办\"",
                incoming,
            )
            return

        action = parts[1].lower()

        if action == "list":
            jobs = scheduler.list_jobs(staff_id=staff_id)
            self.reply_text(CronScheduler.format_job_list(jobs), incoming)
            return

        if action == "remove" and len(parts) >= 3:
            ref = parts[2].strip()
            if scheduler.remove_job(ref, staff_id=staff_id):
                self.reply_text(f"✅ 定时任务已删除: `{ref}`", incoming)
            else:
                self.reply_text(f"❌ 未找到匹配的定时任务: `{ref}`", incoming)
            return

        if action == "add" and len(parts) >= 3:
            rest = parts[2].strip()
            # 解析 schedule 和 prompt
            # 支持格式: /cron add "0 9 * * *" prompt 或 /cron add 30m prompt
            schedule_str, prompt = self._parse_cron_add_args(rest)
            if not schedule_str or not prompt:
                self.reply_text(
                    "⚠️ 格式错误。用法: `/cron add <schedule> <prompt>`\n"
                    "例: `/cron add 30m 提醒我喝水`",
                    incoming,
                )
                return
            try:
                job = scheduler.create_job(
                    prompt=prompt,
                    schedule=schedule_str,
                    conversation_id=conversation_id,
                    staff_id=staff_id,
                    conversation_type=conversation_type,
                )
                self.reply_text(
                    f"✅ 定时任务已创建\n\n"
                    f"- **名称**: {job['name']}\n"
                    f"- **ID**: `{job['id']}`\n"
                    f"- **调度**: {job['schedule_display']}\n"
                    f"- **下次执行**: {job.get('next_run_at', '?')}",
                    incoming,
                )
            except ValueError as exc:
                self.reply_text(f"⚠️ {exc}", incoming)
            return

        self.reply_text("⚠️ 未知的 cron 子命令。发送 `/cron` 查看帮助。", incoming)

    @staticmethod
    def _parse_cron_add_args(rest: str) -> tuple[str, str]:
        """解析 /cron add 后面的参数，返回 (schedule, prompt)。

        支持:
        - "0 9 * * *" 检查待办    → ('0 9 * * *', '检查待办')
        - 30m 提醒我喝水           → ('30m', '提醒我喝水')
        - "every 2h" 检查状态      → ('every 2h', '检查状态')
        """
        rest = rest.strip()

        # 引号包裹的 schedule
        if rest.startswith('"') or rest.startswith("'"):
            quote = rest[0]
            end = rest.find(quote, 1)
            if end > 0:
                schedule = rest[1:end]
                prompt = rest[end + 1:].strip()
                return schedule, prompt

        # 无引号：第一个空白分隔的 token 作为 schedule
        # 但 cron 表达式有 5 段，需要特殊处理
        tokens = rest.split()
        if not tokens:
            return "", ""

        # 尝试 5 段 cron 表达式
        if len(tokens) >= 6 and all(re.match(r"^[\d*\-,/]+$", t) for t in tokens[:5]):
            return " ".join(tokens[:5]), " ".join(tokens[5:])

        # 尝试 "every Xm/h/d" 格式（2 个 token）
        if len(tokens) >= 3 and tokens[0].lower() == "every":
            return f"{tokens[0]} {tokens[1]}", " ".join(tokens[2:])

        # 单 token schedule（30m, 2h 等）
        if len(tokens) >= 2:
            return tokens[0], " ".join(tokens[1:])

        return "", ""

    def _process_cron_commands_from_response(
        self,
        response_text: str,
        conversation_id: str,
        staff_id: str,
        conversation_type: str,
    ) -> str:
        """从 Claude 响应中提取并执行 json:cron 指令块，返回清理后的文本。"""
        scheduler = self.cron_scheduler
        if not scheduler:
            return response_text

        cleaned, commands = extract_cron_commands(response_text)
        if not commands:
            return response_text

        for cmd in commands:
            action = cmd.get("action", "")
            try:
                if action == "create":
                    job = scheduler.create_job(
                        prompt=cmd.get("prompt", ""),
                        schedule=cmd.get("schedule", ""),
                        conversation_id=conversation_id,
                        staff_id=staff_id,
                        conversation_type=conversation_type,
                        name=cmd.get("name"),
                    )
                    self.logger.info(
                        "[%s] cron job created via natural language: id=%s",
                        self.bot_settings.name, job["id"],
                    )
                elif action == "list":
                    jobs = scheduler.list_jobs(staff_id=staff_id)
                    job_list = CronScheduler.format_job_list(jobs)
                    cleaned += f"\n\n{job_list}"
                elif action == "remove":
                    ref = cmd.get("id", "")
                    if scheduler.remove_job(ref, staff_id=staff_id):
                        self.logger.info("[%s] cron job removed via NL: %s", self.bot_settings.name, ref)
                    else:
                        cleaned += f"\n\n⚠️ 未找到任务: `{ref}`"
            except Exception as exc:
                self.logger.warning("[%s] cron command failed: %s", self.bot_settings.name, exc)
                cleaned += f"\n\n⚠️ 定时任务操作失败: {exc}"

        return cleaned

    async def _process_with_card(
        self,
        user_text: str,
        conversation_id: str,
        incoming: ChatbotMessage,
        card_template_id: str,
        content_key: str,
        holder: Optional[ProcessHolder] = None,
        staff_id: str = "",
        conversation_type: str = "1",
    ) -> bool:
        if incoming.hosting_context is None:
            incoming.hosting_context = HostingContext()
            incoming.hosting_context.user_id = (
                getattr(incoming, "sender_staff_id", None)
                or getattr(incoming, "senderStaffId", "")
            )
            incoming.hosting_context.nick = getattr(incoming, "sender_nick", None) or ""
        card = StreamingCardReplier(
            self.dingtalk_client,
            incoming,
            card_template_id=card_template_id,
            content_key=content_key,
        )
        try:
            await card.start(initial_text=f"🤔 正在思考：{user_text[:80]}")
        except CardPermissionError as exc:
            self.logger.warning(
                "[%s] card start failed (permission/template issue), fallback to markdown: %s",
                self.bot_settings.name,
                exc,
            )
            return False
        except Exception as exc:  # noqa: BLE001
            self.logger.warning(
                "[%s] card start failed with unexpected error, fallback to markdown: %s",
                self.bot_settings.name,
                exc,
            )
            return False

        if card._card_instance_id:
            self.regen_store.save(card._card_instance_id, {
                "conversation_id": conversation_id,
                "user_text": user_text,
                "sender_staff_id": getattr(incoming, "sender_staff_id", "") or "",
                "sender_nick": getattr(incoming, "sender_nick", "") or "",
                "conversation_type": getattr(incoming, "conversation_type", "1") or "1",
            })

        got_any = False
        try:
            async for chunk in self.bridge.stream_chat(conversation_id, user_text, holder=holder):
                got_any = True
                await card.append(chunk)
                if card.broken:
                    self.logger.warning(
                        "[%s] card link broken mid-stream, will fallback to markdown",
                        self.bot_settings.name,
                    )
                    break
            # 被用户主动中断
            if holder and holder.cancelled:
                try:
                    await card.append("\n\n⏹️ 已被用户中断。")
                    await card.finish()
                except Exception:  # noqa: BLE001
                    pass
                return True
        except Exception as exc:  # noqa: BLE001
            self.logger.exception("[%s] stream_chat failed on card path: %s", self.bot_settings.name, exc)
            try:
                await card.fail(f"处理异常：{exc}")
            except Exception:  # noqa: BLE001
                pass
            return True

        if card.broken:
            return False

        try:
            if not got_any:
                await card.fail("CLI 没有返回任何内容。")
            else:
                # 记录完整对话日志（含思考过程）
                raw_output = card._buffer or ""
                final_answer = _strip_thinking_blocks(raw_output)
                self.logger.info(
                    "[%s] conv=%s\n>>> USER: %s\n>>> THINKING: %s\n>>> ANSWER: %s",
                    self.bot_settings.name,
                    conversation_id,
                    user_text[:500],
                    _extract_thinking_text(raw_output)[:1000],
                    final_answer[:2000],
                )
                # 处理响应中的 cron 指令块
                final_answer = self._process_cron_commands_from_response(
                    final_answer, conversation_id, staff_id, conversation_type,
                )
                # 完成时移除思考过程，只保留最终回答
                card._buffer = final_answer
                await card.finish()
        except Exception as exc:  # noqa: BLE001
            self.logger.warning("[%s] card finish failed: %s", self.bot_settings.name, exc)

        self.logger.info("[%s] reply via card to conv=%s", self.bot_settings.name, conversation_id)
        return True

    async def _process_with_markdown(
        self,
        user_text: str,
        conversation_id: str,
        incoming: ChatbotMessage,
        holder: Optional[ProcessHolder] = None,
        staff_id: str = "",
        conversation_type: str = "1",
    ) -> None:
        chunks: list[str] = []
        try:
            async for chunk in self.bridge.stream_chat(conversation_id, user_text, holder=holder):
                chunks.append(chunk)
        except Exception as exc:  # noqa: BLE001
            self.logger.exception("[%s] handle message failed: %s", self.bot_settings.name, exc)
            self.reply_text(f"❌ 处理异常：{exc}", incoming)
            return

        full_text = "".join(chunks).strip()
        if not full_text:
            self.reply_text(
                "⚠️ CLI 没有返回任何内容，建议检查 Claude 登录态或查看服务端日志。",
                incoming,
            )
            return

        final_answer = _strip_thinking_blocks(full_text)
        # 处理响应中的 cron 指令块
        final_answer = self._process_cron_commands_from_response(
            final_answer, conversation_id, staff_id, conversation_type,
        )
        self.logger.info(
            "[%s] conv=%s\n>>> USER: %s\n>>> THINKING: %s\n>>> ANSWER: %s",
            self.bot_settings.name,
            conversation_id,
            user_text[:500],
            _extract_thinking_text(full_text)[:1000],
            final_answer[:2000],
        )
        self._reply_markdown_paged(
            title=_build_title(user_text),
            body=final_answer,
            incoming=incoming,
        )

    def _reply_markdown_paged(
        self,
        title: str,
        body: str,
        incoming: ChatbotMessage,
    ) -> None:
        if len(body) <= MAX_MESSAGE_CHARS:
            self.reply_markdown(title, body, incoming)
            return

        segments = _split_markdown(body, MAX_MESSAGE_CHARS)
        total = len(segments)
        for index, segment in enumerate(segments, start=1):
            seg_title = f"{title} ({index}/{total})"
            self.reply_markdown(seg_title, segment, incoming)


_THINKING_BLOCK_RE = re.compile(r"💭 \*思考中\.\.\.\*\n\n.*?\n\n---\n\n", re.DOTALL)


def _strip_thinking_blocks(text: str) -> str:
    """移除流式阶段插入的思考过程块，只保留最终回答。"""
    return _THINKING_BLOCK_RE.sub("", text).lstrip("\n")


def _extract_thinking_text(text: str) -> str:
    """从完整输出中提取思考过程原文（去掉格式装饰）。"""
    blocks = _THINKING_BLOCK_RE.findall(text)
    if not blocks:
        return ""
    # 去掉前缀和分隔线，只留思考原文
    result = []
    for block in blocks:
        content = block.replace("💭 *思考中...*\n\n", "").replace("\n\n---\n\n", "")
        if content.strip():
            result.append(content.strip())
    return "\n".join(result)


def _build_title(user_text: str) -> str:
    preview = user_text.strip().replace("\n", " ")
    if len(preview) > 30:
        preview = preview[:30] + "…"
    return f"💬 {preview}"


def _split_markdown(text: str, limit: int) -> list[str]:
    lines = text.split("\n")
    segments: list[str] = []
    buf: list[str] = []
    buf_len = 0
    in_code = False
    code_fence = "```"

    def flush(open_new: bool) -> None:
        nonlocal buf, buf_len, in_code
        content = "\n".join(buf)
        if in_code:
            content += f"\n{code_fence}"
        segments.append(content)
        buf = []
        buf_len = 0
        if open_new and in_code:
            buf.append(code_fence)
            buf_len = len(code_fence) + 1

    for line in lines:
        line_len = len(line) + 1
        if buf_len + line_len > limit and buf:
            flush(open_new=True)
        buf.append(line)
        buf_len += line_len
        stripped = line.lstrip()
        if stripped.startswith(code_fence):
            in_code = not in_code
    if buf:
        flush(open_new=False)
    return segments


class CronJobExecutor:
    """Cron 任务执行器：用卡片流式展示结果，失败时降级为 markdown 推送。

    作为 CronScheduler 的 executor 注入，实现调度与执行解耦。
    """

    def __init__(
        self,
        bot_settings: BotSettings,
        bridge: ClaudeCliBridge,
        dingtalk_client: DingTalkStreamClient,
        logger: logging.Logger,
    ) -> None:
        self.bot_settings = bot_settings
        self.bridge = bridge
        self.dingtalk_client = dingtalk_client
        self.logger = logger
        # access_token 缓存（用于 markdown 降级推送）
        self._token_cache: tuple[str, float] = ("", 0.0)

    async def __call__(self, job: dict, scheduler: CronScheduler) -> None:
        """执行单个 cron job：卡片流式 → markdown 降级。"""
        job_name = job.get("name") or job["id"]
        prompt = job.get("prompt") or ""
        conversation_id = job.get("conversation_id", "")
        staff_id = job.get("staff_id", "")
        conversation_type = job.get("conversation_type", "1")

        self.logger.info("running cron job: id=%s name=%s", job["id"], job_name)

        # 用临时 conv_id 避免污染用户真实会话的 session resume
        cron_conv_id = f"__cron__{job['id']}_{uuid.uuid4().hex[:8]}"
        cron_prompt = f"[定时任务] {prompt}"

        card_ok = False
        card_template_id = self.bot_settings.card_template_id

        if card_template_id:
            card_ok = await self._execute_with_card(
                job=job,
                scheduler=scheduler,
                cron_conv_id=cron_conv_id,
                cron_prompt=cron_prompt,
                card_template_id=card_template_id,
            )

        if not card_ok:
            await self._execute_with_markdown(
                job=job,
                scheduler=scheduler,
                cron_conv_id=cron_conv_id,
                cron_prompt=cron_prompt,
            )

        # 清理临时 session
        self.bridge.session_store.reset(cron_conv_id)

    async def _execute_with_card(
        self,
        job: dict,
        scheduler: CronScheduler,
        cron_conv_id: str,
        cron_prompt: str,
        card_template_id: str,
    ) -> bool:
        """尝试用卡片流式执行并展示结果。成功返回 True，失败返回 False（调用方降级）。"""
        job_name = job.get("name") or job["id"]
        conversation_id = job.get("conversation_id", "")
        staff_id = job.get("staff_id", "")
        conversation_type = job.get("conversation_type", "1")

        fake_msg = self._build_cron_message(job)
        card = StreamingCardReplier(
            self.dingtalk_client,
            fake_msg,
            card_template_id=card_template_id,
            content_key=self.bot_settings.card_content_key,
        )

        try:
            await card.start(initial_text=f"⏰ 正在执行定时任务：{job_name}")
        except CardPermissionError as exc:
            self.logger.warning("cron card start failed, will fallback: %s", exc)
            return False
        except Exception as exc:
            self.logger.warning("cron card start unexpected error, will fallback: %s", exc)
            return False

        got_any = False
        try:
            async for chunk in self.bridge.stream_chat(cron_conv_id, cron_prompt):
                got_any = True
                await card.append(chunk)
                if card.broken:
                    self.logger.warning("cron card broken mid-stream, will fallback")
                    return False
        except Exception as exc:
            self.logger.exception("cron stream_chat failed: %s", exc)
            try:
                await card.fail(f"定时任务执行异常：{exc}")
            except Exception:
                pass
            scheduler.mark_job_run(job["id"], success=False, error=str(exc))
            return True  # 卡片已创建，不需要降级

        try:
            if not got_any:
                await card.fail("CLI 没有返回任何内容。")
                scheduler.mark_job_run(job["id"], success=False, error="empty response")
            else:
                raw_output = card._buffer or ""
                final_answer = _strip_thinking_blocks(raw_output)
                # 处理嵌套 cron 指令
                final_answer = self._process_cron_in_response(
                    final_answer, job, scheduler,
                )
                card._buffer = final_answer
                await card.finish()
                scheduler.mark_job_run(job["id"], success=True)
                self.logger.info("cron job delivered via card: id=%s", job["id"])
        except Exception as exc:
            self.logger.warning("cron card finish failed: %s", exc)
            scheduler.mark_job_run(job["id"], success=False, error=str(exc))

        return True

    async def _execute_with_markdown(
        self,
        job: dict,
        scheduler: CronScheduler,
        cron_conv_id: str,
        cron_prompt: str,
    ) -> None:
        """降级路径：收集完整输出后通过 oToMessages/groupMessages 推送 markdown。"""
        job_name = job.get("name") or job["id"]

        chunks: list[str] = []
        try:
            async for chunk in self.bridge.stream_chat(cron_conv_id, cron_prompt):
                chunks.append(chunk)
        except Exception as exc:
            self.logger.exception("cron markdown stream_chat failed: %s", exc)
            scheduler.mark_job_run(job["id"], success=False, error=str(exc))
            return

        full_text = "".join(chunks).strip()
        if not full_text:
            scheduler.mark_job_run(job["id"], success=False, error="empty response")
            return

        final_answer = _strip_thinking_blocks(full_text)
        final_answer = self._process_cron_in_response(final_answer, job, scheduler)

        wrapped = (
            f"⏰ **定时任务: {job_name}**\n"
            f"---\n\n"
            f"{final_answer}\n\n"
            f"---\n"
            f"*schedule: {job.get('schedule_display', '?')}*"
        )

        delivered = await self._deliver_markdown(job, wrapped)
        scheduler.mark_job_run(job["id"], success=delivered, error=None if delivered else "deliver failed")
        if delivered:
            self.logger.info("cron job delivered via markdown: id=%s", job["id"])

    def _process_cron_in_response(
        self, text: str, job: dict, scheduler: CronScheduler,
    ) -> str:
        """处理 cron 输出中嵌套的 json:cron 指令块。"""
        cleaned, commands = extract_cron_commands(text)
        if not commands:
            return text
        staff_id = job.get("staff_id", "")
        conversation_id = job.get("conversation_id", "")
        conversation_type = job.get("conversation_type", "1")
        for cmd in commands:
            action = cmd.get("action", "")
            try:
                if action == "create":
                    new_job = scheduler.create_job(
                        prompt=cmd.get("prompt", ""),
                        schedule=cmd.get("schedule", ""),
                        conversation_id=conversation_id,
                        staff_id=staff_id,
                        conversation_type=conversation_type,
                        name=cmd.get("name"),
                    )
                    self.logger.info("cron spawned sub-job: id=%s", new_job["id"])
                elif action == "remove":
                    scheduler.remove_job(cmd.get("id", ""), staff_id=staff_id)
            except Exception as exc:
                self.logger.warning("cron nested command failed: %s", exc)
        return cleaned

    def _build_cron_message(self, job: dict) -> ChatbotMessage:
        """构造用于卡片创建的虚拟 ChatbotMessage。"""
        msg = ChatbotMessage()
        msg.sender_staff_id = job.get("staff_id", "")
        msg.sender_nick = "定时任务"
        msg.conversation_type = job.get("conversation_type", "1")
        msg.conversation_id = job.get("conversation_id", "")
        msg.sender_id = job.get("staff_id", "")
        msg.sender_corp_id = ""
        msg.message_id = hashlib.md5(
            f"cron_{job['id']}_{time.time()}".encode()
        ).hexdigest()
        msg.hosting_context = HostingContext()
        msg.hosting_context.user_id = job.get("staff_id", "")
        msg.hosting_context.nick = "定时任务"
        return msg

    async def _get_access_token(self) -> str:
        """获取钉钉 access_token（用于 markdown 降级推送）。"""
        cached, expire_at = self._token_cache
        if cached and time.time() < expire_at:
            return cached
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.post(
                    "https://api.dingtalk.com/v1.0/oauth2/accessToken",
                    json={
                        "appKey": self.bot_settings.client_id,
                        "appSecret": self.bot_settings.client_secret,
                    },
                )
                if resp.status_code == 200:
                    token = resp.json().get("accessToken", "")
                    if token:
                        self._token_cache = (token, time.time() + 5400)
                    return token
        except Exception as exc:
            self.logger.warning("cron get access token failed: %s", exc)
        return ""

    async def _deliver_markdown(self, job: dict, content: str) -> bool:
        """通过钉钉 API 推送 markdown 消息（降级路径）。"""
        conversation_type = job.get("conversation_type", "1")
        conversation_id = job.get("conversation_id", "")
        staff_id = job.get("staff_id", "")
        job_name = job.get("name") or job["id"]

        token = await self._get_access_token()
        if not token:
            self.logger.error("cron deliver failed: no access token")
            return False

        try:
            async with httpx.AsyncClient(timeout=30) as client:
                if conversation_type == "1":
                    resp = await client.post(
                        "https://api.dingtalk.com/v1.0/robot/oToMessages/batchSend",
                        headers={"x-acs-dingtalk-access-token": token},
                        json={
                            "robotCode": self.bot_settings.robot_code,
                            "userIds": [staff_id],
                            "msgKey": "sampleMarkdown",
                            "msgParam": json.dumps(
                                {"title": f"⏰ {job_name}", "text": content},
                                ensure_ascii=False,
                            ),
                        },
                    )
                else:
                    resp = await client.post(
                        "https://api.dingtalk.com/v1.0/robot/groupMessages/send",
                        headers={"x-acs-dingtalk-access-token": token},
                        json={
                            "robotCode": self.bot_settings.robot_code,
                            "openConversationId": conversation_id,
                            "msgKey": "sampleMarkdown",
                            "msgParam": json.dumps(
                                {"title": f"⏰ {job_name}", "text": content},
                                ensure_ascii=False,
                            ),
                        },
                    )

                if resp.status_code != 200:
                    self.logger.warning("cron deliver failed: %d %s", resp.status_code, resp.text[:300])
                    return False
                return True

        except Exception as exc:
            self.logger.warning("cron deliver exception: %s", exc)
            return False


class CardCallbackHandler(CallbackHandler):
    """处理 AI 卡片上的按钮回调（如"重新回答"、"新对话"）。"""

    def __init__(
        self,
        bot_settings: BotSettings,
        bridge: ClaudeCliBridge,
        session_store: SessionStore,
        regen_store: RegenStore,
        logger: logging.Logger,
        cron_scheduler: CronScheduler | None = None,
    ) -> None:
        super().__init__()
        self.bot_settings = bot_settings
        self.bridge = bridge
        self.session_store = session_store
        self.cron_scheduler = cron_scheduler
        self.regen_store = regen_store
        self.logger = logger

    async def process(self, callback) -> tuple[int, str]:
        try:
            data = callback.data if isinstance(callback.data, dict) else {}
            card_instance_id = data.get("outTrackId", "")
            content = data.get("content", "")
            if isinstance(content, str):
                content = json.loads(content) if content else {}
            params = content.get("cardPrivateData", {}).get("params", {})
            action = params.get("action", "")

            self.logger.info(
                "[%s] card callback: card=%s action=%s", self.bot_settings.name, card_instance_id[:16], action
            )

            if action == "regenerate" and card_instance_id:
                ctx = self.regen_store.get(card_instance_id)
                if ctx:
                    asyncio.create_task(self._regenerate(ctx))
                else:
                    self.logger.warning("[%s] regen context not found for card=%s", self.bot_settings.name, card_instance_id)
            elif action == "new_chat" and card_instance_id:
                ctx = self.regen_store.get(card_instance_id)
                if ctx:
                    self._clear_session(ctx["conversation_id"])
                    asyncio.create_task(self._notify_new_chat(ctx))
                    self.logger.info("[%s] session cleared for conv=%s", self.bot_settings.name, ctx["conversation_id"])
        except Exception as exc:  # noqa: BLE001
            self.logger.exception("[%s] card callback processing failed: %s", self.bot_settings.name, exc)

        return AckMessage.STATUS_OK, "OK"

    @staticmethod
    def _build_fake_message(ctx: dict, msg_id_seed: str = "") -> ChatbotMessage:
        """根据回调上下文构建一个虚拟的 ChatbotMessage，用于卡片创建。"""
        msg = ChatbotMessage()
        msg.sender_staff_id = ctx["sender_staff_id"]
        msg.sender_nick = ctx["sender_nick"]
        msg.conversation_type = ctx["conversation_type"]
        msg.conversation_id = ctx["conversation_id"]
        msg.sender_id = ctx["sender_staff_id"]
        msg.sender_corp_id = ""
        msg.message_id = hashlib.md5(
            f"{msg_id_seed}_{asyncio.get_event_loop().time()}".encode()
        ).hexdigest()
        return msg

    async def _regenerate(self, ctx: dict) -> None:
        conversation_id = ctx["conversation_id"]
        user_text = ctx["user_text"]

        fake_msg = self._build_fake_message(ctx, f"{conversation_id}_{user_text}")

        card_template_id = self.bot_settings.card_template_id
        if not card_template_id:
            return

        card = StreamingCardReplier(
            self.dingtalk_client,
            fake_msg,
            card_template_id=card_template_id,
            content_key=self.bot_settings.card_content_key,
        )
        try:
            await card.start(initial_text=f"🔄 重新回答：{user_text[:60]}")
        except CardPermissionError as exc:
            self.logger.warning("[%s] regen card start failed: %s", self.bot_settings.name, exc)
            return

        if card._card_instance_id:
            self.regen_store.save(card._card_instance_id, ctx)

        try:
            async for chunk in self.bridge.stream_chat(conversation_id, user_text):
                await card.append(chunk)
                if card.broken:
                    break
        except Exception as exc:  # noqa: BLE001
            self.logger.exception("[%s] regen stream_chat failed: %s", self.bot_settings.name, exc)
            try:
                await card.fail(f"重新回答异常：{exc}")
            except Exception:  # noqa: BLE001
                pass
            return

        try:
            raw_output = card._buffer or ""
            final_answer = _strip_thinking_blocks(raw_output)
            # 处理响应中的 cron 指令块
            if self.cron_scheduler:
                staff_id = ctx.get("sender_staff_id", "")
                conv_type = ctx.get("conversation_type", "1")
                cleaned, commands = extract_cron_commands(final_answer)
                if commands:
                    for cmd in commands:
                        action = cmd.get("action", "")
                        try:
                            if action == "create":
                                job = self.cron_scheduler.create_job(
                                    prompt=cmd.get("prompt", ""),
                                    schedule=cmd.get("schedule", ""),
                                    conversation_id=conversation_id,
                                    staff_id=staff_id,
                                    conversation_type=conv_type,
                                    name=cmd.get("name"),
                                )
                                self.logger.info(
                                    "[%s] cron job created via regen: id=%s",
                                    self.bot_settings.name, job["id"],
                                )
                            elif action == "list":
                                jobs = self.cron_scheduler.list_jobs(staff_id=staff_id)
                                cleaned += f"\n\n{CronScheduler.format_job_list(jobs)}"
                            elif action == "remove":
                                self.cron_scheduler.remove_job(cmd.get("id", ""), staff_id=staff_id)
                        except Exception as exc:
                            self.logger.warning("[%s] regen cron command failed: %s", self.bot_settings.name, exc)
                    final_answer = cleaned
            card._buffer = final_answer
            await card.finish()
        except Exception as exc:  # noqa: BLE001
            self.logger.warning("[%s] regen card finish failed: %s", self.bot_settings.name, exc)

        self.logger.info("[%s] regen reply via card to conv=%s", self.bot_settings.name, conversation_id)

    def _clear_session(self, conversation_id: str) -> None:
        self.session_store.reset(conversation_id)

    async def _notify_new_chat(self, ctx: dict) -> None:
        fake_msg = self._build_fake_message(ctx, f"new_chat_{ctx['conversation_id']}")

        card_template_id = self.bot_settings.card_template_id
        if not card_template_id:
            return

        card = StreamingCardReplier(
            self.dingtalk_client,
            fake_msg,
            card_template_id=card_template_id,
            content_key=self.bot_settings.card_content_key,
        )
        try:
            await card.start(initial_text="🧹 正在清空上下文…")
            if card._card_instance_id:
                self.regen_store.save(card._card_instance_id, ctx)
            await card.finish(tail_text="✅ 上下文已清空，下次提问将开启全新对话。")
        except Exception as exc:  # noqa: BLE001
            self.logger.warning("[%s] notify new_chat card failed: %s", self.bot_settings.name, exc)


# ─── 启动 ─────────────────────────────────────────────────────────────────────


def _run_bot_client(client: DingTalkStreamClient, bot_name: str, logger: logging.Logger) -> None:
    """在独立线程中运行单个 DingTalkStreamClient。"""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        client.start_forever()
    except Exception:
        logger.exception("[%s] bot client crashed", bot_name)


def _run_cron_scheduler(scheduler: CronScheduler, logger: logging.Logger) -> None:
    """在独立线程中运行 cron 调度器的 tick 循环。"""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(scheduler.start(interval=5))
    except Exception:
        logger.exception("cron scheduler crashed")


def main() -> None:
    settings = Settings()
    logger = _setup_logging(settings.log_level)

    def _handle_sighup(signum, frame):
        try:
            settings.reload()
            logger.info("config reloaded via SIGHUP, %d bot(s)", len(settings.bots))
        except Exception:
            logger.exception("config reload failed")

    signal.signal(signal.SIGHUP, _handle_sighup)

    # 使用旧格式时给出迁移提示
    cfg = configparser.ConfigParser()
    cfg.read(str(Path(__file__).parent / "config.ini"), encoding="utf-8")
    if cfg.has_section("dingtalk") and not any(s.startswith("bot:") for s in cfg.sections()):
        logger.warning(
            "⚠️ 检测到旧的 [dingtalk] 配置格式，建议迁移到 [bot:name] 格式以支持多机器人。"
            "详见 config.ini.example。"
        )

    logger.info("starting ding-bridge with %d bot(s)", len(settings.bots))

    threads: list[threading.Thread] = []

    for bot in settings.bots:
        # SessionStore 按 bot name 隔离
        store_dir = str(Path(settings.session_store_dir) / bot.name)
        store = SessionStore(store_dir)

        bridge = ClaudeCliBridge(
            cli_path=bot.cli_path,
            cwd=bot.cwd,
            session_store=store,
            timeout_seconds=bot.cli_timeout_seconds,
            permission_mode=bot.permission_mode,
            allowed_tools=bot.allowed_tools,
            disallowed_tools=bot.disallowed_tools,
            model=bot.model,
            append_system_prompt=bot.append_system_prompt,
        )

        regen_store = RegenStore()

        # 创建 cron 调度器（每个 bot 独立）
        cron_dir = Path(settings.session_store_dir) / bot.name / "cron"
        scheduler = CronScheduler(jobs_dir=cron_dir)

        if not bot.allowed_staff_ids:
            logger.warning(
                "[%s] ⚠️ 未配置白名单（allowed_staff_ids 为空），任何能 @ 该机器人的人都能触发 CLI。",
                bot.name,
            )

        logger.info(
            "[%s] robot_code=%s cwd=%s model=%s card_template=%s",
            bot.name,
            bot.robot_code,
            bot.cwd,
            bot.model or "<default>",
            bot.card_template_id or "<disabled>",
        )

        credential = Credential(bot.client_id, bot.client_secret)
        client = DingTalkStreamClient(credential)
        handler = BridgeHandler(bot, bridge, logger, regen_store, cron_scheduler=scheduler)
        client.register_callback_handler(ChatbotMessage.TOPIC, handler)
        client.register_callback_handler(
            Card_Callback_Router_Topic,
            CardCallbackHandler(bot, bridge, store, regen_store, logger, cron_scheduler=scheduler),
        )

        # 注入 cron 执行器（解耦：scheduler 只管调度，executor 负责执行+投递）
        cron_executor = CronJobExecutor(bot, bridge, client, logger)
        scheduler.set_executor(cron_executor)

        t = threading.Thread(
            target=_run_bot_client,
            args=(client, bot.name, logger),
            name=f"bot-{bot.name}",
            daemon=True,
        )
        threads.append(t)

        # 启动 cron 调度器线程
        cron_thread = threading.Thread(
            target=_run_cron_scheduler,
            args=(scheduler, logger),
            name=f"cron-{bot.name}",
            daemon=True,
        )
        threads.append(cron_thread)

    # 启动所有线程
    for t in threads:
        t.start()
        logger.info("thread started: %s", t.name)

    # 主线程等待（响应 Ctrl+C）
    try:
        for t in threads:
            while t.is_alive():
                t.join(timeout=1)
    except KeyboardInterrupt:
        logger.info("bye.")


if __name__ == "__main__":
    main()
