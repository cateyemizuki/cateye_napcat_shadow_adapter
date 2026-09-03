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

SUPPORTED_CONFIG_VERSION = "0.1.0"
GATEWAY_NAME = "napcat_shadow_gateway"
PLUGIN_DISPLAY_NAME = "NapCat 影子适配器"


class PluginSectionConfig(PluginConfigBase):
    """插件基础配置。"""

    __ui_label__ = "插件"
    __ui_icon__ = "ghost"
    __ui_order__ = 0

    enabled: bool = Field(default=True, description="是否启用影子适配器")
    config_version: str = Field(
        default=SUPPORTED_CONFIG_VERSION,
        description="配置版本（与插件版本同步）",
        json_schema_extra={"hidden": True, "disabled": True},
    )


class ServerSectionConfig(PluginConfigBase):
    """协议端连接配置（与 Napcat 适配器 [napcat_server] 保持一致）。"""

    __ui_label__ = "协议端连接"
    __ui_icon__ = "cable"
    __ui_order__ = 1

    host: str = Field(default="127.0.0.1", description="协议端地址")
    port: int = Field(default=3005, ge=1, le=65535, description="协议端正向 WS 端口")
    path: str = Field(default="", description="WS 路径（通常留空，如需可填 /ws）")
    token: str = Field(default="", description="访问令牌（与适配器共用同一协议端 token）")
    reconnect_delay_sec: float = Field(default=5.0, ge=1.0, le=300.0, description="断线重连间隔（秒）")
    action_timeout_sec: float = Field(default=5.0, ge=1.0, le=60.0, description="查昵称等 action 超时（秒）")


class RelaySectionConfig(PluginConfigBase):
    """接管范围与补投行为。"""

    __ui_label__ = "接管与补投"
    __ui_icon__ = "bell_ring"
    __ui_order__ = 2

    enable_group_msg_emoji_like: bool = Field(default=True, description="接管：表情回应通知")
    enable_group_recall: bool = Field(default=True, description="接管：群消息撤回通知")
    enable_friend_recall: bool = Field(default=True, description="接管：好友消息撤回通知")
    enable_essence: bool = Field(default=True, description="接管：精华消息通知")
    debounce_seconds: float = Field(
        default=2.0, ge=0.5, le=30.0,
        description="去抖窗口（秒）：等待适配器版本入站后再决策是否补投",
    )
    seen_ttl_seconds: float = Field(
        default=120.0, ge=10.0, le=3600.0,
        description="适配器已送达登记的保留时长（秒）",
    )
    resolve_nicknames: bool = Field(default=True, description="补投前通过协议端查询昵称/群名（失败回退 QQ 号）")
    nickname_cache_ttl_sec: float = Field(default=600.0, ge=10.0, le=86400.0, description="昵称缓存时长（秒）")


class FilterSectionConfig(PluginConfigBase):
    """补投范围过滤（若适配器配置了名单，请在此镜像，防止越权范围的通知被补投）。"""

    __ui_label__ = "范围过滤"
    __ui_icon__ = "filter_alt"
    __ui_order__ = 3

    group_list_mode: Literal["disabled", "whitelist", "blacklist"] = Field(
        default="disabled", description="群名单模式（disabled=不过滤；与适配器 [chat] 名单保持一致）",
    )
    group_list: list[str] = Field(default_factory=list, description="群号列表（whitelist/blacklist 模式下生效）")
    ban_user_id: list[str] = Field(default_factory=list, description="屏蔽用户：其相关通知一律不补投")


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

        if not self.config.plugin.enabled:
            self.ctx.logger.info("%s 已加载，但 enabled=false，保持待机", PLUGIN_DISPLAY_NAME)
            return
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
        self._start_client()

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
        if not self._filter_allows(payload):
            self.ctx.logger.debug("补投范围过滤拒绝该通知 notice_type=%s", payload.get("notice_type"))
            return
        await self._inject(payload, digest)

    def _filter_allows(self, payload: Mapping[str, Any]) -> bool:
        cfg = self.config.filter
        group_id = str(payload.get("group_id") or "").strip()
        actor_id = relay_core.resolve_actor_user_id(payload)
        if group_id and cfg.group_list_mode in ("whitelist", "blacklist"):
            listed = group_id in {str(x).strip() for x in (cfg.group_list or []) if str(x).strip()}
            if cfg.group_list_mode == "whitelist" and not listed:
                return False
            if cfg.group_list_mode == "blacklist" and listed:
                return False
        if actor_id and actor_id in {str(x).strip() for x in (cfg.ban_user_id or []) if str(x).strip()}:
            return False
        return True

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
        try:
            await self.ctx.gateway.update_state(GATEWAY_NAME, ready=False)
        except Exception:
            pass


def create_plugin() -> NapCatShadowAdapterPlugin:
    """Runner 加载入口。"""
    return NapCatShadowAdapterPlugin()
