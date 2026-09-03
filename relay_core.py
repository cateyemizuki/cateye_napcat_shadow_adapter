"""影子适配器核心逻辑（不依赖 MaiBot SDK，可离线单测）。

约定：两侧的事件身份比对统一走 :func:`event_digest`：

- WS 侧：协议端（NapCat / SnowLuma 本体）广播的原始 notice 事件；
- Hook 侧：Napcat 适配器注入消息里的 ``additional_config.napcat_notice_payload``
  （适配器对原始事件的逐字段拷贝）。

本体对通知类事件不做逐连接改写，两侧载荷字段一致；摘要只取事件语义字段
（不含 post_type/self_id 等通道字段），因此可跨通道比对。
"""

from __future__ import annotations

from hashlib import sha1
from typing import Any, Mapping

import json
import time

# 本插件接管的四类“易丢信息”（与 NapCat 适配器去重键缺陷相关的通知类型）。
NOTICE_TYPES = ("group_msg_emoji_like", "group_recall", "friend_recall", "essence")

# 注入副本的标记字段：写入 additional_config，用于区分“本插件补投的副本”与
# “适配器注入的原始版本”，防止 Hook 把自己的副本再次登记、形成循环。
SHADOW_MARKER_KEY = "napcat_shadow_relay"

# 参与事件身份摘要的字段。必须满足：
# 1) 事件级唯一——time（QQ 事件秒级时间戳）+ sub_type/likes 使同一消息上的
#    不同操作（贴 A / 取消 A / 精华加 / 精华删）摘要必然不同；
# 2) 通道无关——不含 post_type / self_id 等传输层字段；
# 3) 两侧同在——适配器的 napcat_notice_payload 与 WS 原始事件都携带。
DIGEST_FIELDS = (
    "notice_type",
    "sub_type",
    "group_id",
    "user_id",
    "operator_id",
    "sender_id",
    "target_id",
    "message_id",
    "message_seq",
    "likes",
    "time",
)


def canonical_event(payload: Mapping[str, Any]) -> dict[str, Any]:
    """提取参与摘要的规范字段（缺失/None 视为不存在，便于跨通道对齐）。"""

    return {
        key: payload[key]
        for key in DIGEST_FIELDS
        if key in payload and payload[key] is not None
    }


def event_digest(payload: Mapping[str, Any]) -> str:
    """计算事件身份摘要（规范化 JSON 的 SHA-1）。

    注意：匹配粒度是“事件”而不是“消息”。同一被引用消息上的多个操作
    （贴表情 A → 取消 A）message_id 相同但摘要不同——若按 message_id 匹配，
    适配器送达了“贴 A”后，“取消 A”会被误判为已送达而丢失，重新引入缺陷。
    """

    canonical = canonical_event(payload)
    serialized = json.dumps(
        canonical,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return sha1(serialized.encode("utf-8")).hexdigest()


def resolve_actor_user_id(payload: Mapping[str, Any]) -> str:
    """解析通知的操作者 QQ 号（与官方适配器口径一致：operator 优先，"0" 视为空）。"""

    if bool(payload.get("is_natural_lift", False)):
        return ""
    actor = str(payload.get("operator_id") or payload.get("user_id") or "").strip()
    return "" if actor == "0" else actor


def collect_enrich_user_ids(payload: Mapping[str, Any]) -> list[str]:
    """列出需要查昵称的用户号（按文本渲染所需，去重、保序）。"""

    notice_type = str(payload.get("notice_type") or "").strip()
    ids: list[str] = []

    def _push(value: Any) -> None:
        normalized = str(value or "").strip()
        if normalized and normalized != "0" and normalized not in ids:
            ids.append(normalized)

    if notice_type == "essence":
        _push(payload.get("operator_id"))
        _push(payload.get("sender_id"))
        _push(payload.get("user_id"))
    else:
        _push(resolve_actor_user_id(payload))
    return ids


def render_notice_text(payload: Mapping[str, Any], names: Mapping[str, str]) -> str | None:
    """把通知事件渲染为可读文本（与官方适配器口径相近，未查到的名字回退 QQ 号）。

    names 约定键：actor（操作者）/ operator（精华操作者）/ sender（精华被设者）。
    group_msg_emoji_like 的文本通常会被下游翻译插件（如 cateye_set_msg_emoji_like）
    通过 Hook 改写为更详细的表达，这里保证无下游时也入库可读。
    """

    notice_type = str(payload.get("notice_type") or "").strip()
    sub_type = str(payload.get("sub_type") or "").strip()
    message_id = str(payload.get("message_id") or "").strip()
    actor = str(names.get("actor") or "").strip() or resolve_actor_user_id(payload) or "有人"

    if notice_type == "group_msg_emoji_like":
        likes = payload.get("likes")
        emoji_ids: list[str] = []
        if isinstance(likes, list):
            for item in likes:
                if isinstance(item, Mapping) and item.get("emoji_id") is not None:
                    emoji_ids.append(str(item.get("emoji_id")))
        verb = "取消了" if sub_type == "remove" else "贴上了"
        suffix = f"：{ '、'.join(emoji_ids) }" if emoji_ids else ""
        target = f"消息({message_id})" if message_id else "一条消息"
        return f"{actor} 对{target} {verb}表情回应{suffix}"

    if notice_type == "group_recall":
        return f"{actor} 撤回了一条消息"

    if notice_type == "friend_recall":
        return f"{actor} 撤回了一条消息"

    if notice_type == "essence":
        operator = str(names.get("operator") or "").strip() or actor
        sender = str(names.get("sender") or "").strip() or "有人"
        if sub_type == "add":
            return f"{operator} 将 {sender} 的消息设为了精华"
        if sub_type == "delete":
            return f"{operator} 移除了 {sender} 的精华消息"
        return f"{operator} 触发了精华消息事件"

    return None


def is_managed_notice(payload: Mapping[str, Any], enabled_types: Mapping[str, bool]) -> bool:
    """判断 WS 载荷是否为本插件接管且已启用的通知类型（其余事件一律忽略）。"""

    if str(payload.get("post_type") or "").strip() != "notice":
        return False
    notice_type = str(payload.get("notice_type") or "").strip()
    if notice_type not in NOTICE_TYPES:
        return False
    return bool(enabled_types.get(notice_type, True))


def extract_adapter_notice(message_dict: Mapping[str, Any]) -> tuple[str, Mapping[str, Any]] | None:
    """从 Hook 消息中提取“适配器注入的受管通知”。

    返回 (notice_type, 原始事件载荷)；以下情形返回 None：
    - 不是通知消息（is_notify 为假）；
    - napcat_notice_type 不在受管四类中；
    - 缺少 napcat_notice_payload；
    - 带有本插件标记（是自己的补投副本，不应再登记）。
    """

    if not isinstance(message_dict, Mapping) or not bool(message_dict.get("is_notify", False)):
        return None
    message_info = message_dict.get("message_info")
    if not isinstance(message_info, Mapping):
        return None
    additional = message_info.get("additional_config")
    if not isinstance(additional, Mapping):
        return None
    if additional.get(SHADOW_MARKER_KEY):
        return None
    notice_type = str(additional.get("napcat_notice_type") or "").strip()
    if notice_type not in NOTICE_TYPES:
        return None
    payload = additional.get("napcat_notice_payload")
    if not isinstance(payload, Mapping):
        return None
    return notice_type, payload


class TTLSet:
    """带过期时间的摘要登记表（内存态；进程重启后自然重建，无需持久化）。"""

    def __init__(self, ttl_seconds: float) -> None:
        self._ttl = float(ttl_seconds)
        self._entries: dict[str, float] = {}

    def add(self, digest: str) -> None:
        self._sweep()
        self._entries[digest] = time.time() + self._ttl

    def contains(self, digest: str) -> bool:
        expiry = self._entries.get(digest)
        if expiry is None:
            return False
        if expiry <= time.time():
            self._entries.pop(digest, None)
            return False
        return True

    def __len__(self) -> int:
        self._sweep()
        return len(self._entries)

    def _sweep(self) -> None:
        if not self._entries:
            return
        now = time.time()
        expired = [key for key, expiry in self._entries.items() if expiry <= now]
        for key in expired:
            self._entries.pop(key, None)
