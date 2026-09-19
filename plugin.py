"""NapCat 影子适配器：监控 Napcat 适配器的四类易丢通知，缺位时补注入站。

背景：MaiBot-Napcat-Adapter 的通知去重键误用被回应消息的 message_id
（https://github.com/Mai-with-u/MaiBot-Napcat-Adapter/issues/97），导致
group_msg_emoji_like / group_recall / friend_recall / essence 四类通知在宿主
300 秒去重窗口内除首次外全部被静默丢弃（含跨类型互撞：消息入站后 5 分钟内
的撤回/设精华、新消息上的首次表情回应）。

影子工作方式（不改写、不拦截、不依赖适配器配置）：
1. 通过独立 OneBot 正向 WebSocket 连接协议端，只订阅四类 notice 事件
   （message 事件一律忽略，普通消息仍由适配器独家注入）；
2. 通过 chat.receive.before_process 只读 Hook，登记“适配器版本已成功入站”
   的事件摘要（适配器把原始事件完整保留在 additional_config.napcat_notice_payload）；
3. 事件到达后等待去抖窗口再决策：摘要已登记 → 适配器已送达，跳过；
   未登记 → 以自有 receive 网关注入 is_notify 合成通知补投，
   dedupe_key 用事件摘要（与适配器的 message_id 键空间完全隔离）。

补投决策（去抖后）依次经过：适配器送达登记 → 范围过滤（自动镜像官方适配器
[chat] 名单，快照只在成功读到新名单时替换，读取失败保留旧值继续生效）→
可选内容过滤（默认关闭，见 filter.block_at_official_bot / block_at_banned_user：
目标消息——被回应/被撤回/被设精华的那条消息——@ 了 QQ 官方机器人（群成员
资料 is_robot 字段，与官方适配器 ban_qq_bot 同口径）或 @ 了全局屏蔽用户时，
该通知不补投）→ 注入。

失败模式分析：最坏情况是适配器副本晚于去抖窗口入站，产生一条重复通知
（仅观感问题）；显著优于修复前的系统性丢失。上游修复 issue #97 后，适配器
版本永远先行入站，本插件自动退化为 no-op，可安全卸载。
"""

from __future__ import annotations

import asyncio
import time
import uuid
from typing import Any, ClassVar, Literal, Mapping

from maibot_sdk import Field, HookHandler, MaiBotPlugin, MessageGateway, PluginConfigBase
from maibot_sdk.types import CONFIG_RELOAD_SCOPE_SELF, ErrorPolicy, HookMode, HookOrder

from . import onebot_client, relay_core
from .relay_core import SHADOW_MARKER_KEY

SUPPORTED_CONFIG_VERSION = "0.3.0"
GATEWAY_NAME = "napcat_shadow_gateway"
PLUGIN_DISPLAY_NAME = "NapCat 影子适配器"

# 镜像失败后的最小重试间隔（秒）：避免每条事件都打一次跨插件 RPC。
MIRROR_RETRY_INTERVAL_SEC = 10.0
# 镜像连续失败时的告警限流间隔（秒）：保证问题可见，但不刷屏。
MIRROR_FAIL_LOG_INTERVAL_SEC = 300.0
# 目标消息 @ 列表缓存：消息内容不可变，成功结果长缓存（TTL 只为控制内存）；
# 查询失败短缓存，避免同一目标消息上的连发事件（连续贴/取消表情）反复打协议端。
MESSAGE_AT_CACHE_TTL_SEC = 900.0
MESSAGE_AT_FAIL_TTL_SEC = 30.0
# 官方机器人判定缓存（群成员资料 is_robot）：机器人身份基本不变，成功结果长缓存；
# 查询失败按非机器人处理并短缓存（内容过滤是降噪开关，误放行优于误屏蔽）。
ROBOT_CHECK_TTL_SEC = 600.0
ROBOT_CHECK_FAIL_TTL_SEC = 60.0


def _coerce_message_id(message_id: str) -> Any:
    """OneBot message_id 期望整数；无法转 int 时按原样传（兼容字符串消息 id 实现）。"""

    try:
        return int(message_id)
    except (TypeError, ValueError):
        return message_id


class PluginSectionConfig(PluginConfigBase):
    """插件基础配置。"""

    __ui_label__ = "插件"
    __ui_icon__ = "ghost"
    __ui_order__ = 0

    enabled: bool = Field(
        default=True,
        description="是否启用影子适配器",
        json_schema_extra={
            "label": "启用插件",
            "hint": "插件总开关",
        },
    )
    config_version: str = Field(
        default=SUPPORTED_CONFIG_VERSION,
        description="配置版本（与插件版本同步）",
        json_schema_extra={
            "hidden": True,
            "disabled": True,
            "label": "配置版本",
            "hint": "配置版本，勿改",
        },
    )


class ServerSectionConfig(PluginConfigBase):
    """协议端连接配置（与 Napcat 适配器 [napcat_server] 保持一致）。"""

    __ui_label__ = "协议端连接"
    __ui_icon__ = "cable"
    __ui_order__ = 1

    host: str = Field(
        default="127.0.0.1",
        description="协议端地址",
        json_schema_extra={
            "label": "协议端地址",
            "hint": "协议端机器地址",
        },
    )
    port: int = Field(
        default=3005, ge=1, le=65535,
        description="协议端正向 WS 端口",
        json_schema_extra={
            "label": "正向 WS 端口",
            "hint": "正向 WS 端口",
        },
    )
    path: str = Field(
        default="",
        description="WS 路径（通常留空，如需可填 /ws）",
        json_schema_extra={
            "label": "WS 路径",
            "hint": "WS 路径，通常留空",
        },
    )
    token: str = Field(
        default="",
        description="访问令牌（与适配器共用同一协议端 token）",
        json_schema_extra={
            "label": "访问令牌",
            "hint": "协议端访问令牌",
        },
    )
    reconnect_delay_sec: float = Field(
        default=5.0, ge=1.0, le=300.0,
        description="断线重连间隔（秒）",
        json_schema_extra={
            "label": "断线重连间隔（秒）",
            "hint": "断线重连间隔秒数",
        },
    )
    action_timeout_sec: float = Field(
        default=5.0, ge=1.0, le=60.0,
        description="查昵称等 action 超时（秒）",
        json_schema_extra={
            "label": "action 超时（秒）",
            "hint": "查昵称等操作超时",
        },
    )


class RelaySectionConfig(PluginConfigBase):
    """接管范围与补投行为。"""

    __ui_label__ = "接管与补投"
    __ui_icon__ = "bell_ring"
    __ui_order__ = 2

    enable_group_msg_emoji_like: bool = Field(
        default=True,
        description="接管：表情回应通知",
        json_schema_extra={
            "label": "接管表情回应通知",
            "hint": "漏投表情回应则补投",
        },
    )
    enable_group_recall: bool = Field(
        default=True,
        description="接管：群消息撤回通知",
        json_schema_extra={
            "label": "接管群消息撤回通知",
            "hint": "漏投群消息撤回则补投",
        },
    )
    enable_friend_recall: bool = Field(
        default=True,
        description="接管：好友消息撤回通知",
        json_schema_extra={
            "label": "接管好友消息撤回通知",
            "hint": "漏投好友撤回则补投",
        },
    )
    enable_essence: bool = Field(
        default=True,
        description="接管：精华消息通知",
        json_schema_extra={
            "label": "接管精华消息通知",
            "hint": "漏投精华消息则补投",
        },
    )
    debounce_seconds: float = Field(
        default=2.0, ge=0.5, le=30.0,
        description="去抖窗口（秒）：等待适配器版本入站后再决策是否补投",
        json_schema_extra={
            "label": "去抖窗口（秒）",
            "hint": "等待适配器送达的秒数",
        },
    )
    seen_ttl_seconds: float = Field(
        default=120.0, ge=10.0, le=3600.0,
        description="适配器已送达登记的保留时长（秒）",
        json_schema_extra={
            "label": "送达登记保留时长（秒）",
            "hint": "送达登记保留秒数",
        },
    )
    resolve_nicknames: bool = Field(
        default=True,
        description="补投前通过协议端查询昵称/群名（失败回退 QQ 号）",
        json_schema_extra={
            "label": "补投前查询昵称/群名",
            "hint": "补投前查昵称群名",
        },
    )
    nickname_cache_ttl_sec: float = Field(
        default=600.0, ge=10.0, le=86400.0,
        description="昵称缓存时长（秒）",
        json_schema_extra={
            "label": "昵称缓存时长（秒）",
            "hint": "昵称缓存时长秒数",
        },
    )


class FilterSectionConfig(PluginConfigBase):
    """补投范围过滤与可选内容过滤。

    范围过滤：开启 sync_from_adapter（默认）后，本插件在加载、配置热更新以及
    镜像过期时自动从官方 Napcat 适配器（maibot-team.napcat-adapter）的 [chat]
    配置节镜像名单（群/私聊白黑名单 + ban_user_id），无需手动维护，防止把
    适配器名单外的群/用户的通知越权补投；关闭时退回使用下方手动填写的名单。

    内容过滤（block_at_* 两项，默认关闭）：目标消息——被回应/被撤回/被设精华的
    那条消息——@ 了 QQ 官方机器人或全局屏蔽用户时，该通知不补投。属于额外
    降噪开关，只影响本插件的补投路径，适配器已正常送达的通知不受影响。
    """

    __ui_label__ = "范围过滤"
    __ui_icon__ = "filter_alt"
    __ui_order__ = 3

    sync_from_adapter: bool = Field(
        default=True,
        description="自动镜像官方 Napcat 适配器 [chat] 名单（群/私聊白黑名单 + ban_user_id）",
        json_schema_extra={
            "label": "自动镜像适配器名单",
            "hint": "自动镜像适配器名单",
        },
    )
    adapter_plugin_id: str = Field(
        default="maibot-team.napcat-adapter",
        description="名单来源适配器的插件 id（一般无需改动）",
        json_schema_extra={
            "hidden": True,
            "label": "名单来源适配器 id",
            "hint": "名单来源适配器 id",
        },
    )
    group_list_mode: Literal["disabled", "whitelist", "blacklist"] = Field(
        default="disabled",
        description="群名单模式（sync_from_adapter=false 时生效；disabled=不过滤）",
        json_schema_extra={
            "label": "群名单模式",
            "hint": "群名单手动过滤方式",
        },
    )
    group_list: list[str] = Field(
        default_factory=list,
        description="群号列表（whitelist/blacklist 模式下生效）",
        json_schema_extra={
            "label": "群号列表",
            "hint": "群号列表，一行一个",
        },
    )
    private_list_mode: Literal["disabled", "whitelist", "blacklist"] = Field(
        default="disabled",
        description="私聊名单模式（sync_from_adapter=false 时生效；disabled=不过滤）",
        json_schema_extra={
            "label": "私聊名单模式",
            "hint": "私聊名单手动过滤方式",
        },
    )
    private_list: list[str] = Field(
        default_factory=list,
        description="私聊用户号列表（whitelist/blacklist 模式下生效）",
        json_schema_extra={
            "label": "私聊用户列表",
            "hint": "用户 QQ 号，一行一个",
        },
    )
    ban_user_id: list[str] = Field(
        default_factory=list,
        description="屏蔽用户：其相关通知一律不补投",
        json_schema_extra={
            "label": "屏蔽用户",
            "hint": "这些用户不补投通知",
        },
    )
    mirror_refresh_seconds: float = Field(
        default=60.0, ge=0.0, le=3600.0,
        description=(
            "镜像名单的刷新间隔（秒）：到点后的下一次补投决策会重新读取适配器名单，"
            "使适配器侧新增/移除的屏蔽用户与群名单自动生效；0 表示只在加载与配置热更新时镜像一次"
        ),
        json_schema_extra={
            "label": "名单刷新间隔（秒）",
            "hint": "0 = 只在加载时镜像一次",
        },
    )
    mirror_fail_closed: bool = Field(
        default=False,
        description=(
            "镜像读不到适配器名单时是否拒绝一切补投（fail-closed）：开启后宁可漏投也不越权；"
            "关闭则回退上方手动名单（默认，行为与旧版一致）"
        ),
        json_schema_extra={
            "label": "镜像失败即拒投",
            "hint": "宁漏投不越权",
        },
    )
    block_at_official_bot: bool = Field(
        default=False,
        description=(
            "屏蔽目标消息 @ 了 QQ 官方机器人的通知（如机器人对@它的消息贴表情回应）；"
            "判定走群成员资料 is_robot 字段，与官方适配器 ban_qq_bot 同口径。默认关闭"
        ),
        json_schema_extra={
            "label": "屏蔽@官方机器人",
            "hint": "被@官方机器人不补投",
        },
    )
    block_at_banned_user: bool = Field(
        default=False,
        description=(
            "屏蔽目标消息 @ 了全局屏蔽用户的通知；名单与范围过滤同源"
            "（镜像适配器 [chat].ban_user_id，或上方手动名单）。默认关闭"
        ),
        json_schema_extra={
            "label": "屏蔽@全局屏蔽用户",
            "hint": "被@屏蔽用户不补投",
        },
    )


class ShadowAdapterConfig(PluginConfigBase):
    """插件完整配置。"""

    plugin: PluginSectionConfig = Field(default_factory=PluginSectionConfig)
    server: ServerSectionConfig = Field(default_factory=ServerSectionConfig)
    relay: RelaySectionConfig = Field(default_factory=RelaySectionConfig)
    filter: FilterSectionConfig = Field(default_factory=FilterSectionConfig)


class NapCatShadowAdapterPlugin(MaiBotPlugin):
    """影子中继插件：观测适配器入站情况，缺位时以自有网关补投四类通知。"""

    config_model: ClassVar[type[PluginConfigBase] | None] = ShadowAdapterConfig

    async def on_load(self) -> None:
        # 状态容器先于 enabled 判断初始化，保证 Hook 在任何配置下都安全。
        self._client: onebot_client.OneBotWSClient | None = None
        self._self_id = ""
        self._seen_adapter = relay_core.TTLSet(float(self.config.relay.seen_ttl_seconds))
        self._injected = relay_core.TTLSet(300.0)  # 与宿主去重 TTL 对齐
        self._pending_digests: set[str] = set()
        self._decision_tasks: set[asyncio.Task] = set()
        self._name_cache: dict[tuple[str, str], tuple[float, str, str]] = {}
        self._group_name_cache: dict[str, tuple[float, str]] = {}
        # 内容过滤缓存：目标消息 @ 列表 / 官方机器人判定（均为 (group_id, user_id/message_id) 键）
        self._msg_at_cache: dict[tuple[str, str], tuple[float, frozenset[str] | None]] = {}
        self._robot_check_cache: dict[tuple[str, str], tuple[float, bool]] = {}
        self._mirrored_filter: dict[str, Any] | None = None
        # 镜像时序状态：成功时刻 / 上次尝试时刻（失败退避）/ 上次失败告警时刻（限流）
        self._mirror_at: float = 0.0
        self._mirror_attempt_at: float = 0.0
        self._mirror_fail_log_at: float = 0.0

        if not self.config.plugin.enabled:
            self.ctx.logger.info("%s 已加载，但 enabled=false，保持待机", PLUGIN_DISPLAY_NAME)
            return
        self._warn_config_sanity()
        await self._sync_filter_from_adapter()
        self._start_client()

    async def on_unload(self) -> None:
        await self._shutdown_client()
        try:
            await self.ctx.gateway.update_state(GATEWAY_NAME, ready=False)
        except Exception:
            pass
        self.ctx.logger.info("%s 已卸载，后台任务与连接已清理", PLUGIN_DISPLAY_NAME)

    async def on_config_update(self, scope: str, config_data: dict[str, Any], version: str) -> None:
        if scope != CONFIG_RELOAD_SCOPE_SELF:
            return
        if not self.config.plugin.enabled:
            await self._shutdown_client()
            self.ctx.logger.info("%s 已按新配置停用", PLUGIN_DISPLAY_NAME)
            return
        self.ctx.logger.info("%s 配置热更新，重建协议端连接", PLUGIN_DISPLAY_NAME)
        await self._shutdown_client()
        self._seen_adapter = relay_core.TTLSet(float(self.config.relay.seen_ttl_seconds))
        self._warn_config_sanity()
        # 重新镜像适配器名单（sync_from_adapter 变更 / 开关切换后立即生效）
        self._mirror_attempt_at = 0.0
        self._mirror_fail_log_at = 0.0
        await self._sync_filter_from_adapter()
        self._start_client()

    def _warn_config_sanity(self) -> None:
        """启动/热更新时对易错配置组合做一次性提示（不阻断运行）。"""

        seen_ttl = float(self.config.relay.seen_ttl_seconds)
        debounce = float(self.config.relay.debounce_seconds)
        if seen_ttl <= debounce:
            self.ctx.logger.warning(
                "relay.seen_ttl_seconds(%s) ≤ relay.debounce_seconds(%s)：适配器副本的"
                "送达登记可能在决策前过期并造成必然性重复补投，建议 seen_ttl_seconds "
                "明显大于 debounce_seconds",
                seen_ttl, debounce,
            )

    # ==================== Hook：登记适配器已成功入站的通知（只读） ====================

    @HookHandler(
        "chat.receive.before_process",
        name="shadow_seen_recorder",
        description="登记适配器已成功入站的四类通知事件摘要（只读，不改写不拦截）",
        mode=HookMode.BLOCKING,
        order=HookOrder.EARLY,
        error_policy=ErrorPolicy.SKIP,
    )
    async def hook_record_adapter_notice(self, message: Mapping[str, Any] | None = None, **kwargs: Any) -> None:
        """影子模式的观测端：适配器版本通过宿主去重、真正进入入站链时登记摘要。

        登记动作在去抖窗口（默认 2 秒）内完成即可命中，WS 侧决策据此跳过补投。
        本 Hook 无任何副作用：不改 kwargs、不 abort，登记失败也绝不影响主链。
        """

        try:
            if not self.config.plugin.enabled:
                return
            extracted = relay_core.extract_adapter_notice(message or {})
            if extracted is None:
                return
            _notice_type, payload = extracted
            self._seen_adapter.add(relay_core.event_digest(payload))
        except Exception:
            return
        return None

    # ==================== 网关：补投载体（receive-only，宿主不会向其投递出站） ====================

    @MessageGateway("receive", name=GATEWAY_NAME, platform="qq", protocol="napcat",
                    description="NapCat 影子适配器补投网关（仅用于注入 is_notify 合成通知）")
    async def gateway_shadow(self, **kwargs: Any) -> None:
        return None

    # ==================== WS 事件侧：过滤 → 去抖决策 → 补投 ====================

    def _enabled_types(self) -> dict[str, bool]:
        cfg = self.config.relay
        return {
            "group_msg_emoji_like": bool(cfg.enable_group_msg_emoji_like),
            "group_recall": bool(cfg.enable_group_recall),
            "friend_recall": bool(cfg.enable_friend_recall),
            "essence": bool(cfg.enable_essence),
        }

    async def _on_ws_event(self, payload: dict[str, Any]) -> None:
        if not relay_core.is_managed_notice(payload, self._enabled_types()):
            return  # message 事件与其他通知一律忽略：普通消息仍由适配器独家注入
        digest = relay_core.event_digest(payload)
        if digest in self._pending_digests or self._injected.contains(digest):
            return
        self._pending_digests.add(digest)
        task = asyncio.create_task(self._decide_and_maybe_inject(payload, digest))
        self._decision_tasks.add(task)
        task.add_done_callback(self._decision_tasks.discard)

    async def _decide_and_maybe_inject(self, payload: dict[str, Any], digest: str) -> None:
        try:
            await asyncio.sleep(max(0.5, float(self.config.relay.debounce_seconds)))
        finally:
            self._pending_digests.discard(digest)

        if not self.config.plugin.enabled or self._client is None or not self._client.is_connected:
            return
        if self._seen_adapter.contains(digest):
            self.ctx.logger.info("适配器已送达该通知，跳过补投 notice_type=%s digest=%s",
                                 payload.get("notice_type"), digest[:12])
            return
        if self._injected.contains(digest):
            return
        # 决策前按需（重）镜像适配器名单：兜住「启动时适配器尚未就绪」的竞态，
        # 也让适配器侧名单改动在 mirror_refresh_seconds 内自动生效。
        await self._ensure_filter_fresh()
        if not self._filter_allows(payload):
            self.ctx.logger.debug("补投范围过滤拒绝该通知 notice_type=%s", payload.get("notice_type"))
            return
        if not await self._content_filter_allows(payload):
            return
        # 注入前最后复查：范围/内容过滤中的跨插件 RPC 与 get_msg、成员资料查询
        # 可能耗时（受 action 超时约束），期间适配器副本可能已入站登记——
        # 此时放弃补投，把「重复通知」压回文档声明的最坏情形以内。
        if self._seen_adapter.contains(digest) or self._injected.contains(digest):
            self.ctx.logger.debug(
                "决策期间适配器版本已入站，放弃补投 notice_type=%s digest=%s",
                payload.get("notice_type"), digest[:12],
            )
            return
        await self._inject(payload, digest)

    # ==================== 范围过滤：自动镜像官方适配器 [chat] 名单 ====================

    async def _sync_filter_from_adapter(self) -> None:
        """从官方 Napcat 适配器的 [chat] 配置节同步补投范围名单。

        通过 config.get_plugin 能力读取 maibot-team.napcat-adapter 的运行时
        config.toml（Host 侧磁盘实时内容），把其入站名单口径镜像为本插件的过滤
        状态。镜像结果保存在实例状态 _mirrored_filter 中（不写回本插件
        config.toml）。

        语义对齐官方 NapCatChatFilter（filters.py）：
        - enable_chat_list_filter=false  → 群/私聊名单整体不生效（mode=disabled），
          但 ban_user_id 仍全局生效；
        - 群消息按 group_list_type/group_list，私聊按 private_list_type/private_list；
        - ban_user_id 优先级最高。

        快照替换策略：**只有成功读到完整名单时才整体替换 _mirrored_filter**。
        读取失败（适配器尚未就绪 / 未安装 / 停用 / 瞬时异常）时保留上一份快照、
        只记录状态、不抛异常，由 :meth:`_ensure_filter_fresh` 在后续补投决策时
        自动重试。这一点很关键，历史上踩过两个坑：

        - 本插件的 on_load 早于同组 Napcat 适配器注册完成，首次镜像必然失败
          （旧实现不重试，于是永久回退到（默认为空的）手动名单，全局屏蔽用户
          被照常补投）；
        - 旧实现在每次刷新开头就清空快照，读取一旦失败（协议端/宿主瞬时抖动），
          已镜像的 ban_user_id 随之失效，过滤整体退化为空手动名单——被全局
          屏蔽用户的通知重新被放行补投。
        """

        self._mirror_attempt_at = time.time()
        if not self.config.filter.sync_from_adapter:
            self._mirrored_filter = None
            return
        adapter_id = str(self.config.filter.adapter_plugin_id or "").strip() or "maibot-team.napcat-adapter"
        try:
            adapter_config = await self.ctx.config.get_plugin(adapter_id)
        except Exception as exc:
            self._log_mirror_failure(
                "读取适配器 %s 配置失败（%s），暂时无法镜像补投名单；"
                "适配器就绪后会在下一次补投决策时自动重试",
                adapter_id, exc,
            )
            return
        if not isinstance(adapter_config, dict):
            self._log_mirror_failure(
                "适配器 %s 配置读取结果不是字典（%r），暂时无法镜像补投名单；"
                "适配器就绪后会自动重试",
                adapter_id, adapter_config,
            )
            return
        chat_cfg = adapter_config.get("chat")
        if not isinstance(chat_cfg, dict):
            # 两种常见成因，用读到的顶层键区分：
            # ① 本插件 on_load 早于同组适配器注册完成 → 宿主解析不到适配器目录，返回 {}；
            # ② filter.adapter_plugin_id 填错 → 读回的是本插件自己的配置（含 relay/filter 等键）。
            self._log_mirror_failure(
                "适配器 %s 配置缺少 [chat] 节（读到顶层键 %s），暂时无法镜像补投名单；"
                "适配器就绪后会自动重试。若上面列出的是本插件自己的配置键，"
                "请检查 filter.adapter_plugin_id 是否写错",
                adapter_id, sorted(adapter_config.keys()),
            )
            return

        # 非法模式值的回退与官方适配器 _normalize_list_mode 对齐：
        # DEFAULT_CHAT_LIST_TYPE = "whitelist"（宁严勿松，避免把适配器实际按
        # 白名单过滤的群当成不过滤而越权补投）；名单过滤整体未启用才是 disabled。
        def _norm_mode(value: Any, enabled: bool) -> str:
            mode = str(value or "").strip().lower()
            if not enabled:
                return "disabled"
            if mode not in ("whitelist", "blacklist"):
                return "whitelist"
            return mode

        def _norm_ids(raw: Any) -> list[str]:
            if not isinstance(raw, list):
                return []
            return [str(item).strip() for item in raw if str(item or "").strip()]

        list_filter_enabled = bool(chat_cfg.get("enable_chat_list_filter", True))
        group_mode = _norm_mode(chat_cfg.get("group_list_type"), list_filter_enabled)
        private_mode = _norm_mode(chat_cfg.get("private_list_type"), list_filter_enabled)
        self._mirrored_filter = {
            "group_list_mode": group_mode,
            "group_list": _norm_ids(chat_cfg.get("group_list")),
            "private_list_mode": private_mode,
            "private_list": _norm_ids(chat_cfg.get("private_list")),
            "ban_user_id": _norm_ids(chat_cfg.get("ban_user_id")),
        }
        self._mirror_at = time.time()
        self._mirror_fail_log_at = 0.0
        self.ctx.logger.info(
            "已自动镜像适配器 %s 的 [chat] 名单: group=%s(%s) private=%s(%s) ban=%s",
            adapter_id,
            self._mirrored_filter["group_list_mode"],
            len(self._mirrored_filter["group_list"]),
            self._mirrored_filter["private_list_mode"],
            len(self._mirrored_filter["private_list"]),
            len(self._mirrored_filter["ban_user_id"]),
        )

    def _log_mirror_failure(self, message: str, *args: Any) -> None:
        """限流记录镜像失败：问题必须可见，但不能每条补投都刷屏。"""

        now = time.time()
        if now - self._mirror_fail_log_at < MIRROR_FAIL_LOG_INTERVAL_SEC:
            return
        self._mirror_fail_log_at = now
        self.ctx.logger.warning(message, *args)

    def _mirror_is_stale(self) -> bool:
        """镜像缺失或已过刷新间隔时需要重新读取适配器名单。"""

        if self._mirrored_filter is None:
            return True
        refresh = float(self.config.filter.mirror_refresh_seconds)
        if refresh <= 0:
            return False
        return (time.time() - self._mirror_at) >= refresh

    async def _ensure_filter_fresh(self) -> None:
        """补投决策前按需（重）镜像适配器名单。

        - **从未镜像成功**（启动竞态最常见）：每次决策都重试，直到拿到适配器名单为止——
          开启 ``mirror_fail_closed`` 时绝不放行任何补投（默认关闭时回退手动名单，
          默认手动名单为空即放行，属文档明示的 fail-open 语义）；
        - **已有快照但过期**：到 ``mirror_refresh_seconds`` 后重新读取，使适配器侧
          新增/移除的屏蔽用户与群名单自动生效；
        - **刷新失败但有旧快照**：按 ``MIRROR_RETRY_INTERVAL_SEC`` 退避，期间继续用旧快照过滤
          （而不是退化成「不过滤」）。
        """

        if not self.config.filter.sync_from_adapter:
            return
        if not self._mirror_is_stale():
            return
        if (
            self._mirrored_filter is not None
            and time.time() - self._mirror_attempt_at < MIRROR_RETRY_INTERVAL_SEC
        ):
            return
        await self._sync_filter_from_adapter()

    def _manual_filter(self) -> dict[str, Any]:
        """手动名单（sync_from_adapter=false 或镜像未就绪且未开启 fail-closed 时使用）。"""

        cfg = self.config.filter
        return {
            "group_list_mode": cfg.group_list_mode,
            "group_list": list(cfg.group_list or []),
            "private_list_mode": cfg.private_list_mode,
            "private_list": list(cfg.private_list or []),
            "ban_user_id": list(cfg.ban_user_id or []),
        }

    def _active_filter(self) -> dict[str, Any] | None:
        """返回当前生效的过滤配置；``None`` 表示镜像未就绪且要求 fail-closed。"""

        if self._mirrored_filter is not None:
            return self._mirrored_filter
        if self.config.filter.sync_from_adapter and self.config.filter.mirror_fail_closed:
            return None
        return self._manual_filter()

    def _filter_allows(self, payload: Mapping[str, Any]) -> bool:
        f = self._active_filter()
        if f is None:
            self._log_mirror_failure(
                "补投名单尚未镜像成功且已开启 filter.mirror_fail_closed，暂不补投任何通知"
                "（宁漏投不越权）；适配器就绪后会自动恢复"
            )
            return False
        group_id = str(payload.get("group_id") or "").strip()
        actor_id = relay_core.resolve_actor_user_id(payload)
        ids = {str(x).strip() for x in (f["ban_user_id"] or []) if str(x).strip()}
        if actor_id and actor_id in ids:
            return False
        if group_id:
            mode = f["group_list_mode"]
            listed = group_id in {str(x).strip() for x in (f["group_list"] or []) if str(x).strip()}
        else:
            mode = f["private_list_mode"]
            listed = actor_id in {str(x).strip() for x in (f["private_list"] or []) if str(x).strip()}
        if mode == "whitelist" and not listed:
            return False
        if mode == "blacklist" and listed:
            return False
        return True

    # ==================== 内容过滤：目标消息 @ 名单（默认关闭） ====================

    async def _content_filter_allows(self, payload: Mapping[str, Any]) -> bool:
        """目标消息 @ 过滤（block_at_* 两项，默认关闭）。

        针对通知所指向的“目标消息”（被回应/被撤回/被设精华的那条消息）：
        其 @ 列表命中 QQ 官方机器人或全局屏蔽用户时，跳过该通知的补投。

        设计约束：

        - 两个开关都关闭时零开销（不产生任何协议端请求）；
        - 仅群聊通知参与（表情回应/群撤回/精华均携带 group_id + message_id；
          私聊 friend_recall 无 @ 语义，直接放行）；
        - 目标消息查不到（撤回后过期/协议端不支持）或 @ 列表为空时放行——
          本过滤是降噪开关而非越权防线，宁可多投也不误伤正常通知；
        - 全局屏蔽名单与 :meth:`_filter_allows` 同源（镜像或手动），保证口径一致。
        """

        block_bot = bool(self.config.filter.block_at_official_bot)
        block_banned = bool(self.config.filter.block_at_banned_user)
        if not block_bot and not block_banned:
            return True
        group_id = str(payload.get("group_id") or "").strip()
        message_id = str(payload.get("message_id") or "").strip()
        if not group_id or not message_id or message_id == "0":
            return True
        at_ids = await self._fetch_message_at_ids(group_id, message_id)
        if not at_ids:
            # None = 查询失败（放行）；空集 = 目标消息没有 @ 任何具体用户
            return True
        if block_banned:
            active = self._active_filter()
            ban_ids = {
                str(item).strip()
                for item in ((active or {}).get("ban_user_id") or [])
                if str(item).strip()
            }
            hit_banned = ban_ids & at_ids
            if hit_banned:
                self.ctx.logger.debug(
                    "目标消息 @ 了全局屏蔽用户，跳过补投 notice_type=%s targets=%s",
                    payload.get("notice_type"), sorted(hit_banned),
                )
                return False
        if block_bot:
            for at_id in sorted(at_ids):
                if await self._is_official_bot(group_id, at_id):
                    self.ctx.logger.debug(
                        "目标消息 @ 了 QQ 官方机器人，跳过补投 notice_type=%s robot=%s",
                        payload.get("notice_type"), at_id,
                    )
                    return False
        return True

    async def _fetch_message_at_ids(self, group_id: str, message_id: str) -> frozenset[str] | None:
        """查询目标消息的 @ 用户号集合（get_msg + 缓存）。

        返回 ``None`` 表示查询失败（连接断开/消息已过期/响应异常），调用方应放行；
        空 frozenset 表示查询成功但目标消息没有 @ 任何具体用户。
        """

        key = (group_id, message_id)
        now = time.time()
        cached = self._msg_at_cache.get(key)
        if cached is not None and cached[0] > now:
            return cached[1]
        at_ids: frozenset[str] | None = None
        ttl = MESSAGE_AT_FAIL_TTL_SEC
        try:
            if self._client is not None and self._client.is_connected:
                response = await self._client.call_action(
                    "get_msg", {"message_id": _coerce_message_id(message_id)},
                )
                data = response.get("data") if isinstance(response, dict) else None
                if isinstance(data, Mapping):
                    at_ids = frozenset(relay_core.extract_at_user_ids(data))
                    ttl = MESSAGE_AT_CACHE_TTL_SEC
        except Exception as exc:
            self.ctx.logger.debug("查询目标消息 @ 列表失败 (%s/%s): %s", group_id, message_id, exc)
        relay_core.sweep_expired_cache(self._msg_at_cache)
        self._msg_at_cache[key] = (now + ttl, at_ids)
        return at_ids

    async def _is_official_bot(self, group_id: str, user_id: str) -> bool:
        """判断用户在该群是否为 QQ 官方机器人（与官方适配器 ban_qq_bot 同口径）。

        判定来自群成员资料的 ``is_robot`` 字段（NapCat 扩展字段，官方适配器的
        NapCatOfficialBotGuard 同款）。查询失败按非机器人处理并短缓存；成功结果
        长缓存——机器人身份基本不变。
        """

        key = (group_id, user_id)
        now = time.time()
        cached = self._robot_check_cache.get(key)
        if cached is not None and cached[0] > now:
            return cached[1]
        is_robot = False
        ttl = ROBOT_CHECK_FAIL_TTL_SEC
        try:
            if self._client is not None and self._client.is_connected:
                response = await self._client.call_action(
                    "get_group_member_info",
                    {"group_id": int(group_id), "user_id": int(user_id), "no_cache": False},
                )
                data = response.get("data") if isinstance(response, dict) else None
                if isinstance(data, Mapping):
                    is_robot = bool(data.get("is_robot"))
                    ttl = ROBOT_CHECK_TTL_SEC
        except Exception as exc:
            self.ctx.logger.debug("查询群成员资料失败 (%s/%s): %s", group_id, user_id, exc)
        relay_core.sweep_expired_cache(self._robot_check_cache)
        self._robot_check_cache[key] = (now + ttl, is_robot)
        return is_robot

    async def _inject(self, payload: dict[str, Any], digest: str) -> None:
        notice_type = str(payload.get("notice_type") or "").strip()
        sub_type = str(payload.get("sub_type") or "").strip()
        group_id = str(payload.get("group_id") or "").strip()
        actor_id = relay_core.resolve_actor_user_id(payload)

        names: dict[str, str] = {}
        actor_nickname, actor_cardname = "", ""
        if self.config.relay.resolve_nicknames and actor_id:
            actor_nickname, actor_cardname = await self._lookup_member(group_id, actor_id)
        names["actor"] = actor_nickname
        if notice_type == "essence":
            operator_id = str(payload.get("operator_id") or "").strip()
            sender_id = str(payload.get("sender_id") or payload.get("user_id") or "").strip()
            if self.config.relay.resolve_nicknames:
                if operator_id and operator_id != "0":
                    names["operator"] = (await self._lookup_member(group_id, operator_id))[0]
                if sender_id and sender_id != "0":
                    names["sender"] = (await self._lookup_member(group_id, sender_id))[0]

        text = relay_core.render_notice_text(payload, names)
        if not text:
            self.ctx.logger.debug("通知渲染为空，跳过 notice_type=%s", notice_type)
            return

        group_name = ""
        if group_id:
            group_name = (await self._lookup_group_name(group_id)) if self.config.relay.resolve_nicknames else ""
            group_name = group_name or f"群{group_id}"

        user_info = {
            "user_id": actor_id or "0",
            "user_nickname": actor_nickname or actor_id or "系统通知",
            "user_cardname": actor_cardname or None,
        }
        # self_id 对齐官方 Napcat 适配器口径：优先取事件载荷自带的 self_id
        # （协议端广播的事件必带，与普通消息/通知走同一账号），缺失时才回退
        # 到连接期 get_login_info 查询结果 self._self_id。避免因查询未完成/失败
        # 导致 account_id 缺失，使注入消息被归入一个独立的 WebUI 聊天流。
        event_self_id = str(payload.get("self_id") or "").strip()
        self_id = event_self_id or self._self_id
        additional_config: dict[str, Any] = {
            "self_id": self_id,
            "napcat_notice_type": notice_type,
            "napcat_notice_sub_type": sub_type,
            "napcat_notice_payload": dict(payload),
            SHADOW_MARKER_KEY: True,
        }
        if group_id:
            additional_config["platform_io_target_group_id"] = group_id
        elif actor_id:
            additional_config["platform_io_target_user_id"] = actor_id

        message_info: dict[str, Any] = {"user_info": user_info, "additional_config": additional_config}
        if group_id:
            message_info["group_info"] = {"group_id": group_id, "group_name": group_name}

        event_time = payload.get("time")
        if not isinstance(event_time, (int, float)) or event_time <= 0:
            event_time = time.time()

        message_dict: dict[str, Any] = {
            "message_id": f"napcat-shadow-{uuid.uuid4().hex}",
            "timestamp": str(float(event_time)),
            "platform": "qq",
            "message_info": message_info,
            "raw_message": [{"type": "text", "data": text}],
            "is_mentioned": False,
            "is_at": False,
            "is_emoji": False,
            "is_picture": False,
            "is_command": False,
            "is_notify": True,
            "session_id": "",
            "processed_plain_text": text,
            # 与官方适配器通知注入口径对齐（codecs/notice/message_codec.py 同带此字段）
            "display_message": text,
        }

        route_metadata: dict[str, Any] = {}
        if self_id:
            route_metadata["self_id"] = self_id
        try:
            accepted = await self.ctx.gateway.route_message(
                GATEWAY_NAME,
                message_dict,
                route_metadata=route_metadata,
                external_message_id=str(payload.get("message_id") or ""),
                dedupe_key=f"shadow:{digest}",
            )
        except Exception as exc:
            self.ctx.logger.warning("补投 %s 通知失败: %s", notice_type, exc)
            return
        if accepted:
            self._injected.add(digest)
            self.ctx.logger.info(
                "已补投 %s 通知（适配器版本未入站）digest=%s", notice_type, digest[:12],
            )
        else:
            self.ctx.logger.warning(
                "宿主拒绝了补投（可能被去重或网关未就绪）notice_type=%s digest=%s", notice_type, digest[:12],
            )

    # ==================== 昵称/群名查询（走自有 WS，缓存控制频率） ====================

    async def _lookup_member(self, group_id: str, user_id: str) -> tuple[str, str]:
        key = (group_id, user_id)
        now = time.time()
        cached = self._name_cache.get(key)
        if cached and cached[0] > now:
            return cached[1], cached[2]
        nickname, card = "", ""
        try:
            if self._client is not None and self._client.is_connected:
                if group_id:
                    response = await self._client.call_action(
                        "get_group_member_info",
                        {"group_id": int(group_id), "user_id": int(user_id), "no_cache": False},
                    )
                else:
                    response = await self._client.call_action("get_stranger_info", {"user_id": int(user_id)})
                data = response.get("data") if isinstance(response, dict) else None
                if isinstance(data, Mapping):
                    nickname = str(data.get("nickname") or "").strip()
                    card = str(data.get("card") or "").strip()
        except Exception as exc:
            self.ctx.logger.debug("查询昵称失败 (%s/%s): %s", group_id, user_id, exc)
        ttl = float(self.config.relay.nickname_cache_ttl_sec) if (nickname or card) else 30.0
        relay_core.sweep_expired_cache(self._name_cache)
        self._name_cache[key] = (now + ttl, nickname, card)
        return nickname, card

    async def _lookup_group_name(self, group_id: str) -> str:
        now = time.time()
        cached = self._group_name_cache.get(group_id)
        if cached and cached[0] > now:
            return cached[1]
        name = ""
        try:
            if self._client is not None and self._client.is_connected:
                response = await self._client.call_action("get_group_info", {"group_id": int(group_id)})
                data = response.get("data") if isinstance(response, dict) else None
                if isinstance(data, Mapping):
                    name = str(data.get("group_name") or "").strip()
        except Exception as exc:
            self.ctx.logger.debug("查询群名失败 (%s): %s", group_id, exc)
        ttl = float(self.config.relay.nickname_cache_ttl_sec) if name else 30.0
        relay_core.sweep_expired_cache(self._group_name_cache)
        self._group_name_cache[group_id] = (now + ttl, name)
        return name

    # ==================== 连接生命周期 ====================

    def _start_client(self) -> None:
        server = self.config.server
        self._client = onebot_client.OneBotWSClient(
            host=server.host,
            port=server.port,
            token=server.token,
            path=server.path,
            reconnect_delay_sec=server.reconnect_delay_sec,
            action_timeout_sec=server.action_timeout_sec,
            on_event=self._on_ws_event,
            on_connected=self._on_ws_connected,
            on_disconnected=self._on_ws_disconnected,
            logger=self.ctx.logger,
        )
        self._client.start()

    async def _shutdown_client(self) -> None:
        for task in list(self._decision_tasks):
            task.cancel()
        self._decision_tasks.clear()
        self._pending_digests.clear()
        # 协议端连接相关的查询缓存一并失效：重连后（可能换了账号/协议端）重新查询
        self._msg_at_cache.clear()
        self._robot_check_cache.clear()
        client = self._client
        self._client = None
        if client is not None:
            await client.stop()

    async def _on_ws_connected(self) -> None:
        try:
            response = await self._client.call_action("get_login_info", {})
            data = response.get("data") if isinstance(response, dict) else None
            self_id = str(data.get("user_id") or "").strip() if isinstance(data, Mapping) else ""
            if self_id:
                self._self_id = self_id
        except Exception as exc:
            self.ctx.logger.warning("获取机器人账号失败（补投仍可用，路由元数据缺 self_id）: %s", exc)
        try:
            await self.ctx.gateway.update_state(
                GATEWAY_NAME,
                ready=True,
                platform="qq",
                account_id=self._self_id,
            )
            self.ctx.logger.info("影子网关已就绪（account_id=%s）", self._self_id or "未知")
        except Exception as exc:
            self.ctx.logger.warning("上报网关就绪状态失败: %s", exc)

    async def _on_ws_disconnected(self) -> None:
        # 协议端断开重连可能换了账号/协议端实例，查询类缓存全部失效重查
        self._name_cache.clear()
        self._group_name_cache.clear()
        self._msg_at_cache.clear()
        self._robot_check_cache.clear()
        try:
            await self.ctx.gateway.update_state(GATEWAY_NAME, ready=False)
        except Exception:
            pass


def create_plugin() -> NapCatShadowAdapterPlugin:
    """Runner 加载入口。"""
    return NapCatShadowAdapterPlugin()
