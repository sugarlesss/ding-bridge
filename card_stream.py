"""钉钉 AI 卡片的流式打字机渲染封装。

依赖前置：
- 必须先在钉钉卡片平台（https://open-dev.dingtalk.com/fe/card）创建一张模板，
  拿到形如 `xxxx.schema` 的 card_template_id；
- 模板里需要有一个承接正文的文本字段，字段 key 要和 `content_key` 参数对应（默认 `content`）。
- 应用在开放平台要开通两个 API 权限：`Card.Instance.Write`、`Card.Streaming.Write`，
  否则调用时会返回 HTTP 403 / `AccessTokenPermissionDenied`。

职责：
- 首次写入时通过 `async_create_and_deliver_card` 创建卡片并投递到用户会话；
- 后续增量通过 `async_streaming` 覆盖整段文本 + 节流控制刷新频率；
- 单条文本过长时自动开新卡（钉钉卡片 schema 对单字段长度有上限，保守切分）；
- 检测到权限类错误（403）后立即熔断，告知上层走 markdown 降级路径。
"""

from __future__ import annotations

import logging
import time
from typing import Optional

from dingtalk_stream import AICardReplier, ChatbotMessage

logger = logging.getLogger(__name__)

# 钉钉单张卡片 content 字段的保守字符上限，超过就收尾当前卡并新开一张
MAX_CARD_CHARS = 15000
# 两次 async_streaming 调用之间的最小间隔，避免触达钉钉接口频控
MIN_UPDATE_INTERVAL_SEC = 0.4


class CardPermissionError(RuntimeError):
    """应用缺少卡片相关 API 权限（如 Card.Instance.Write / Card.Streaming.Write）。

    上层捕获到这个异常时应立即降级到 markdown 路径，不要继续调用后续 async_streaming。
    """


class StreamingCardReplier:
    """用钉钉 AI 卡片分块展示 Claude 的增量输出，表现为打字机效果。"""

    def __init__(
        self,
        dingtalk_client,
        incoming_message: ChatbotMessage,
        card_template_id: str,
        content_key: str = "content",
    ) -> None:
        self._client = dingtalk_client
        self._incoming = incoming_message
        self._card_template_id = card_template_id
        self._content_key = content_key

        self._replier: Optional[AICardReplier] = None
        self._card_instance_id: Optional[str] = None
        # 当前卡片累计的「真实回复」文本（作为 async_streaming 的整段覆盖值）
        # 注意：占位文本不进 buffer，避免"占位文本 + 真实回复"拼在一起给用户看
        self._buffer: str = ""
        # 创建卡片时使用的占位文本。第一次写入真实内容前，flush 时会用它展示；
        # 一旦有真实内容，占位文本就被整段覆盖掉。
        self._placeholder: str = ""
        self._last_flush_at: float = 0.0
        self._finished: bool = False
        # 一旦遇到终止性错误（如 403），熔断所有后续调用
        self._broken: bool = False

    async def start(self, initial_text: str = "🤔 思考中…") -> None:
        """初始化卡片：创建 + 投递；若创建阶段即失败则抛 CardPermissionError。

        - 仅在此处创建卡片，不做额外的 streaming 试探调用（SDK 对 HTTP 错误只
          写日志不抛异常，试探调用拿不到可靠信号，徒增延迟）；
        - 若创建阶段就抛异常 → 视为 Card.Instance.Write 权限/模板问题；
        - 若 SDK 吞了 HTTP 错误并返回空 card_instance_id → 同样视为失败；
        - 后续 streaming 阶段的失败由 _flush 捕获异常后置 _broken=True 熔断，
          上层通过 `card.broken` 判断是否需要降级到 markdown。
        """
        self._replier = AICardReplier(self._client, self._incoming)
        # 占位文本只用于首次创建时的视觉占位，不计入真实回复 buffer
        self._placeholder = initial_text
        self._buffer = ""

        try:
            self._card_instance_id = await self._replier.async_create_and_deliver_card(
                card_template_id=self._card_template_id,
                card_data={self._content_key: initial_text},
            )
        except Exception as exc:  # noqa: BLE001
            raise CardPermissionError(f"创建卡片失败: {exc}") from exc

        # SDK 里 async_create_and_deliver_card 对 HTTP 错误只打日志不抛异常，返回空 id。
        # 这里用空 id 作为信号视为失败。
        if not self._card_instance_id:
            raise CardPermissionError(
                "async_create_and_deliver_card 返回空 card_instance_id，"
                "通常是应用缺少 Card.Instance.Write 权限或模板 ID/字段名不对"
            )

        # 第一次 append 立即 flush（而非等节流窗口），以便尽早让用户看到 AI 的真实回复替换占位文本
        self._last_flush_at = 0.0

    async def append(self, chunk: str) -> None:
        """追加文本增量，按节流策略推送到卡片。"""
        if self._finished or self._broken or not chunk:
            return

        # 当前卡片容量不够，先收尾再开新卡
        if len(self._buffer) + len(chunk) > MAX_CARD_CHARS:
            await self._flush(finished=True)
            if self._broken:
                return
            await self._open_next_card()

        self._buffer += chunk

        now = time.monotonic()
        if now - self._last_flush_at >= MIN_UPDATE_INTERVAL_SEC:
            await self._flush(finished=False)
            self._last_flush_at = now

    async def finish(self, tail_text: str = "") -> None:
        """结束回复：把剩余 buffer 推上去，并把卡片标记为完成。"""
        if self._finished:
            return
        if tail_text:
            self._buffer += tail_text
        if not self._broken:
            await self._flush(finished=True)
        self._finished = True

    async def fail(self, reason: str) -> None:
        """以失败态收尾，把错误原因追加到正文里。"""
        if self._finished:
            return
        self._buffer = (self._buffer or "") + f"\n\n❌ {reason}"
        if not self._broken:
            await self._flush(finished=True, failed=True)
        self._finished = True

    @property
    def broken(self) -> bool:
        """卡片链路是否已因权限等原因彻底失效。上层据此判断是否降级。"""
        return self._broken

    async def _flush(self, finished: bool, failed: bool = False) -> None:
        if self._replier is None or self._card_instance_id is None or self._broken:
            return
        # 还没收到任何真实内容时显示占位文本，收到后整段覆盖掉占位文本
        content = self._buffer or self._placeholder or "（空回复）"
        try:
            await self._replier.async_streaming(
                self._card_instance_id,
                content_key=self._content_key,
                content_value=content,
                append=False,
                finished=finished,
                failed=failed,
            )
        except Exception as exc:  # noqa: BLE001
            # SDK 对 HTTP 错误多半是记日志而不抛，但保底处理下
            logger.warning("card streaming flush raised: %s", exc)
            self._broken = True

    async def _open_next_card(self) -> None:
        """当前卡片已写满，另起一张新卡继续写。"""
        assert self._replier is not None
        # 新卡从"（接上一条）"开头，这是真实正文的一部分，算进 buffer
        self._buffer = "（接上一条）\n"
        self._placeholder = ""
        try:
            new_id = await self._replier.async_create_and_deliver_card(
                card_template_id=self._card_template_id,
                card_data={self._content_key: self._buffer},
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("open next card failed: %s", exc)
            self._broken = True
            return
        if not new_id:
            self._broken = True
            return
        self._card_instance_id = new_id
        self._last_flush_at = time.monotonic()
