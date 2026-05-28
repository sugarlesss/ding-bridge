"""Claude Code CLI 桥接模块。

职责：
1. 以 stream-json 格式启动 `claude -p`，流式产出文本增量
2. 维护每个钉钉会话（conversationId）到 Claude session_id 的映射，实现多轮上下文续接
3. 屏蔽 CLI 进程细节，对外只暴露 `stream_chat` 异步生成器
"""

from __future__ import annotations

import asyncio
import collections
import json
import logging
import os
import signal
from pathlib import Path
from typing import AsyncIterator, Optional

logger = logging.getLogger(__name__)


class ProcessHolder:
    """持有当前活跃的 CLI 子进程引用，供外部中断使用。"""

    def __init__(self) -> None:
        self.proc: Optional[asyncio.subprocess.Process] = None
        self.cancelled: bool = False

    async def interrupt(self) -> None:
        """优雅中断：先 SIGINT，等 2s 不退出则 KILL。"""
        self.cancelled = True
        if self.proc is None or self.proc.returncode is not None:
            return
        try:
            self.proc.send_signal(signal.SIGINT)
            await asyncio.sleep(2)
            if self.proc.returncode is None:
                self.proc.kill()
        except (ProcessLookupError, OSError):
            pass


class SessionStore:
    """将 conversationId -> claude session_id 的映射持久化到本地文件。

    放在文件里的原因：桥接服务重启后仍能续接上次聊到一半的会话。
    """

    def __init__(self, store_dir: str) -> None:
        self.store_path = Path(store_dir)
        self.store_path.mkdir(parents=True, exist_ok=True)
        self.index_file = self.store_path / "sessions.json"
        self._cache: dict[str, str] = {}
        self._load()

    def _load(self) -> None:
        if self.index_file.exists():
            try:
                self._cache = json.loads(self.index_file.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError) as exc:
                logger.warning("session store load failed, start fresh: %s", exc)
                self._cache = {}

    def _flush(self) -> None:
        tmp = self.index_file.with_suffix(".tmp")
        tmp.write_text(json.dumps(self._cache, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(self.index_file)

    def get(self, conversation_id: str) -> Optional[str]:
        return self._cache.get(conversation_id)

    def set(self, conversation_id: str, session_id: str) -> None:
        if self._cache.get(conversation_id) == session_id:
            return
        self._cache[conversation_id] = session_id
        self._flush()

    def reset(self, conversation_id: str) -> None:
        if self._cache.pop(conversation_id, None) is not None:
            self._flush()


class ClaudeCliBridge:
    """包装 `claude -p --output-format stream-json` 的异步流式调用。"""

    def __init__(
        self,
        cli_path: str,
        cwd: str,
        session_store: SessionStore,
        timeout_seconds: int = 300,
        permission_mode: str = "bypassPermissions",
        allowed_tools: str = "",
        disallowed_tools: str = "",
        model: str = "",
        append_system_prompt: str = "",
    ) -> None:
        self.cli_path = cli_path
        self.cwd = cwd
        self.session_store = session_store
        self.timeout_seconds = timeout_seconds
        self.permission_mode = permission_mode
        self.allowed_tools = allowed_tools
        self.disallowed_tools = disallowed_tools
        self.model = model
        self.append_system_prompt = append_system_prompt
        # Session 级别累计统计：conversation_id -> {total_tokens, cached_tokens, cost_usd}
        # 上限 500 条，超出时淘汰最久未使用的（LRU）
        self._session_stats: collections.OrderedDict[str, dict] = collections.OrderedDict()
        self._session_stats_max = 500

    async def stream_chat(
        self,
        conversation_id: str,
        user_text: str,
        holder: Optional[ProcessHolder] = None,
    ) -> AsyncIterator[str]:
        """对某个会话发起一轮对话，异步产出文本增量片段。

        支持的特殊指令：
        - /clear, /new, 新开会话, 重置 → 清空会话上下文
        - /goal <text> → 设定当前会话目标（重置会话并以目标 prompt 开启新对话）
        """
        stripped = user_text.strip()

        if stripped in {"/clear", "/new", "新开会话", "重置"}:
            self.session_store.reset(conversation_id)
            self._session_stats.pop(conversation_id, None)
            yield "🔄 已重置该会话的上下文，下一条消息将开启新的对话。"
            return

        if stripped.startswith("/goal"):
            goal_text = stripped[5:].strip()
            if not goal_text:
                yield "⚠️ 用法：/goal <目标描述>\n例如：/goal 帮我重构 auth 模块，提升可测试性"
                return
            self.session_store.reset(conversation_id)
            user_text = (
                f"我为本次对话设定了一个目标，请围绕这个目标来协助我，"
                f"每次回复结束时简要说明当前进展和下一步建议。\n\n"
                f"目标：{goal_text}"
            )
            logger.info("goal set for conv=%s: %s", conversation_id, goal_text[:100])

        if stripped == "/compact":
            resume_session = self.session_store.get(conversation_id)
            if not resume_session:
                yield "当前没有活跃的会话上下文，无需压缩。"
                return
            # 用 /compact 指令触发 Claude 的上下文压缩
            user_text = "/compact"
            logger.info("compact requested for conv=%s session=%s", conversation_id, resume_session)
            async for chunk in self._run_cli(conversation_id, user_text, resume_session, holder):
                yield chunk
            yield "\n\n✅ 上下文压缩完成。"
            return

        resume_session = self.session_store.get(conversation_id)
        success = False
        async for chunk in self._run_cli(conversation_id, user_text, resume_session, holder):
            success = True
            yield chunk

        # resume 失败时，清掉旧 session 重试一次
        if not success and resume_session:
            logger.info("resume failed, retrying without --resume: conv=%s", conversation_id)
            self.session_store.reset(conversation_id)
            async for chunk in self._run_cli(conversation_id, user_text, None, holder):
                success = True
                yield chunk

        if not success:
            yield "❌ CLI 调用失败，请稍后重试或发送 /clear 重置会话。"

    async def _run_cli(
        self,
        conversation_id: str,
        user_text: str,
        resume_session: Optional[str],
        holder: Optional[ProcessHolder] = None,
    ) -> AsyncIterator[str]:
        """实际执行 claude CLI 的内部方法。

        实时流式 yield 文本增量（让卡片能边思考边更新），
        CLI 成功退出时保存 session_id，失败时 yield 错误提示。
        """
        args = [
            self.cli_path,
            "-p",
            user_text,
            "--output-format",
            "stream-json",
            "--verbose",
        ]
        if self.model:
            args.extend(["--model", self.model])
        if self.append_system_prompt:
            args.extend(["--append-system-prompt", self.append_system_prompt])
        if self.permission_mode:
            args.extend(["--permission-mode", self.permission_mode])
        if self.allowed_tools:
            args.extend(["--allowed-tools", self.allowed_tools])
        if self.disallowed_tools:
            args.extend(["--disallowed-tools", self.disallowed_tools])
        if resume_session:
            args.extend(["--resume", resume_session])

        logger.info(
            "spawn claude cli: conv=%s resume=%s permission=%s text_len=%d",
            conversation_id,
            resume_session,
            self.permission_mode or "default",
            len(user_text),
        )

        env = os.environ.copy()
        proc = await asyncio.create_subprocess_exec(
            *args,
            cwd=self.cwd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=env,
            limit=16 * 1024 * 1024,
        )

        if holder is not None:
            holder.proc = proc

        assert proc.stdout is not None

        got_any = False
        new_session_id: Optional[str] = None
        result_meta: dict = {}
        has_thinking = False

        async def _read_stream() -> AsyncIterator[str]:
            nonlocal new_session_id, result_meta, has_thinking
            while True:
                try:
                    line = await proc.stdout.readline()
                except asyncio.LimitOverrunError as exc:
                    logger.warning(
                        "claude stdout line exceeded buffer limit (%d bytes consumed), skip this event",
                        exc.consumed,
                    )
                    try:
                        await proc.stdout.readexactly(exc.consumed)
                    except (asyncio.IncompleteReadError, asyncio.LimitOverrunError):
                        pass
                    continue
                except ValueError as exc:
                    logger.warning("claude stdout readline ValueError, skip: %s", exc)
                    continue

                if not line:  # EOF
                    break

                raw = line.decode("utf-8", errors="ignore").strip()
                if not raw:
                    continue
                try:
                    event = json.loads(raw)
                except json.JSONDecodeError:
                    logger.debug("non-json line from claude: %s", raw[:200])
                    continue

                sid = event.get("session_id")
                if sid and new_session_id is None:
                    new_session_id = sid

                # 检测是否有 thinking 内容
                if event.get("type") == "assistant" and not has_thinking:
                    for item in (event.get("message") or {}).get("content") or []:
                        if isinstance(item, dict) and item.get("type") == "thinking":
                            has_thinking = True
                            break

                # 捕获 result 事件中的 usage 信息
                if event.get("type") == "result":
                    result_meta = event
                    continue

                chunk = self._extract_text(event)
                if chunk:
                    yield chunk

        try:
            async for chunk in _timeout_iterator(_read_stream(), self.timeout_seconds):
                if holder is not None and holder.cancelled:
                    break
                got_any = True
                yield chunk
        except asyncio.TimeoutError:
            proc.kill()
            yield f"\n\n⚠️ CLI 执行超过 {self.timeout_seconds}s 被终止。"
            got_any = True
        finally:
            if proc.returncode is None:
                try:
                    await asyncio.wait_for(proc.wait(), timeout=5)
                except asyncio.TimeoutError:
                    proc.kill()
                    await proc.wait()

        if proc.returncode == 0:
            if resume_session:
                self.session_store.set(conversation_id, resume_session)
            elif new_session_id:
                self.session_store.set(conversation_id, new_session_id)
            # yield 上下文用量提示
            if got_any and result_meta:
                usage_hint = self._format_usage_hint(result_meta, conversation_id, has_thinking)
                if usage_hint:
                    yield usage_hint
        else:
            stderr_bytes = b""
            if proc.stderr is not None:
                try:
                    stderr_bytes = await proc.stderr.read()
                except Exception:  # noqa: BLE001
                    stderr_bytes = b""
            err_msg = stderr_bytes.decode("utf-8", errors="ignore").strip()
            if err_msg:
                logger.error("claude cli exit=%s stderr=%s", proc.returncode, err_msg)
            if not got_any:
                # 没产出任何内容时不 yield，让 stream_chat 判断是否重试
                return

    @staticmethod
    def _extract_text(event: dict) -> str:
        """从 claude stream-json 的 event 里抽取要展示给用户的文本增量。

        Claude Code 的 stream-json 事件类型大致有：
        - system/init: 初始化
        - assistant: assistant 消息（content 是数组，里面有 type=text/thinking 的块）
        - user: tool_result 之类
        - result: 最终结果
        """
        etype = event.get("type")

        if etype == "assistant":
            message = event.get("message") or {}
            content = message.get("content") or []
            parts: list[str] = []
            for item in content:
                if not isinstance(item, dict):
                    continue
                if item.get("type") == "text":
                    text = item.get("text") or ""
                    if text:
                        parts.append(text)
                elif item.get("type") == "thinking":
                    thinking = item.get("thinking") or ""
                    if thinking:
                        parts.append(f"💭 *思考中...*\n\n{thinking}\n\n---\n\n")
            return "".join(parts)

        if etype == "result":
            return ""

        return ""

    def _format_usage_hint(self, result_event: dict, conversation_id: str, has_thinking: bool = False) -> str:
        """从 result 事件中提取上下文用量，格式化为末尾提示。

        字段来源:
        - result_event.modelUsage.<model_name>.{inputTokens, outputTokens, ...}
        - result_event.{duration_ms, duration_api_ms, total_cost_usd}
        """
        model_usage = result_event.get("modelUsage") or {}
        if not model_usage:
            return ""

        model_name = next(iter(model_usage.keys()), "")
        model_stats = model_usage.get(model_name) or {}

        input_tokens = model_stats.get("inputTokens") or 0
        output_tokens = model_stats.get("outputTokens") or 0
        cache_creation = model_stats.get("cacheCreationInputTokens") or 0
        cache_read = model_stats.get("cacheReadInputTokens") or 0
        context_window = model_stats.get("contextWindow") or 0

        # 本次 token 总量和 cached 量
        turn_total = input_tokens + output_tokens + cache_creation + cache_read
        turn_cached = cache_creation + cache_read

        # 累计 Session 统计（LRU 淘汰）
        if conversation_id in self._session_stats:
            self._session_stats.move_to_end(conversation_id)
        elif len(self._session_stats) >= self._session_stats_max:
            self._session_stats.popitem(last=False)

        stats = self._session_stats.setdefault(conversation_id, {
            "total_tokens": 0, "cached_tokens": 0, "cost_usd": 0.0,
        })
        stats["total_tokens"] += turn_total
        stats["cached_tokens"] += turn_cached
        stats["cost_usd"] += result_event.get("total_cost_usd") or 0.0

        # Context 用量（本次请求实际占用）
        context_used = input_tokens + cache_creation + cache_read
        if not context_window:
            return ""

        pct = context_used * 100 / context_window

        # Speed: output_tokens / api 耗时
        duration_api_ms = result_event.get("duration_api_ms") or 0
        speed = output_tokens / (duration_api_ms / 1000) if duration_api_ms > 0 else 0

        thinking_mode = "max" if has_thinking else "off"
        block = (
            f"\n\n---\n\n"
            f"```\n"
            f"📊 Context  : {_fmt_tokens(context_used)}/{_fmt_tokens(context_window)} ({pct:.0f}%)\n"
            f"🤖 Model    : {model_name}\n"
            f"💭 Thinking : {thinking_mode}\n"
            f"📥 In/Out   : {_fmt_tokens(input_tokens)} / {_fmt_tokens(output_tokens)}\n"
            f"📦 Total    : {_fmt_tokens(stats['total_tokens'])}  Cached: {_fmt_tokens(stats['cached_tokens'])}\n"
            f"💰 Cost     : ${stats['cost_usd']:.2f}  ⚡ Speed: {speed:.1f} t/s\n"
            f"```"
        )
        return block


def _fmt_tokens(n: int) -> str:
    """格式化 token 数量为易读字符串。"""
    if n >= 1_000_000:
        return f"{n / 1_000_000:.1f}M"
    if n >= 1_000:
        return f"{n / 1_000:.1f}k"
    return str(n)


async def _timeout_iterator(it: AsyncIterator[str], timeout: int) -> AsyncIterator[str]:
    """为异步生成器加整体超时：从首条消息起算，总时长不得超过 timeout。"""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    aiter_ = it.__aiter__()
    while True:
        remaining = deadline - loop.time()
        if remaining <= 0:
            raise asyncio.TimeoutError()
        try:
            chunk = await asyncio.wait_for(aiter_.__anext__(), timeout=remaining)
        except StopAsyncIteration:
            return
        yield chunk
