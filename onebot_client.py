"""OneBot v11 正向 WebSocket 客户端（不依赖 MaiBot SDK，可离线导入测试）。

与官方 Napcat 适配器同款通信形态：插件作为 WS 客户端连协议端（NapCat /
SnowLuma 本体），事件由本体广播给所有连接（含 action 响应按 echo 配对），
因此本连接与适配器的连接互不干扰。

鉴权：access_token 走 URL query（OneBot v11 规范支持，NapCat / SnowLuma 均接受）。
websockets 包为 manifest 声明的 python_package 依赖，此处延迟探测以支持离线导入。
"""

from __future__ import annotations

import asyncio
import json
import uuid
from typing import Any, Awaitable, Callable
from urllib.parse import quote

try:  # 延迟探测：离线测试环境可无 websockets 导入本模块
    import websockets  # type: ignore
except ImportError:  # pragma: no cover
    websockets = None  # type: ignore

EventHandler = Callable[[dict[str, Any]], Awaitable[None]]
StateCallback = Callable[[], Awaitable[None]]


def split_payloads(data: Any) -> list[Any]:
    """把一帧载荷拆成条目列表（OneBot 允许单帧推送事件数组）。"""

    if isinstance(data, list):
        return [item for item in data if isinstance(item, dict)]
    if isinstance(data, dict):
        return [data]
    return []


def is_action_response(item: dict[str, Any]) -> bool:
    """判断条目是否为 action 响应（有 echo 字段；OneBot 事件不含 echo）。"""

    return item.get("echo") is not None


class OneBotWSClient:
    """带自动重连与 echo 配对的最小 OneBot 正向 WS 客户端。"""

    def __init__(
        self,
        *,
        host: str,
        port: int,
        token: str = "",
        path: str = "",
        reconnect_delay_sec: float = 5.0,
        action_timeout_sec: float = 5.0,
        on_event: EventHandler | None = None,
        on_connected: StateCallback | None = None,
        on_disconnected: StateCallback | None = None,
        logger: Any = None,
    ) -> None:
        self._host = str(host or "127.0.0.1").strip()
        self._port = int(port or 3001)
        self._token = str(token or "").strip()
        self._path = str(path or "").strip()
        self._reconnect_delay = max(1.0, float(reconnect_delay_sec))
        self._action_timeout = max(1.0, float(action_timeout_sec))
        self._on_event = on_event
        self._on_connected = on_connected
        self._on_disconnected = on_disconnected
        self._logger = logger

        self._ws: Any = None
        self._task: asyncio.Task | None = None
        self._pending: dict[str, asyncio.Future] = {}
        self._closing = False
        self._missing_dependency_logged = False

    # ---------- 生命周期 ----------

    @property
    def is_connected(self) -> bool:
        return self._ws is not None and not self._closing

    def start(self) -> None:
        if self._task is not None and not self._task.done():
            return
        self._closing = False
        self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        self._closing = True
        ws = self._ws
        self._ws = None
        if ws is not None:
            try:
                await ws.close()
            except Exception:
                pass
        self._fail_pending(ConnectionError("影子适配器连接已关闭"))
        task = self._task
        self._task = None
        if task is not None and not task.done():
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass

    def _build_uri(self) -> str:
        path = self._path or "/"
        if not path.startswith("/"):
            path = f"/{path}"
        uri = f"ws://{self._host}:{self._port}{path}"
        if self._token:
            uri = f"{uri}?access_token={quote(self._token, safe='')}"
        return uri

    async def _run(self) -> None:
        if websockets is None:
            if not self._missing_dependency_logged:
                self._missing_dependency_logged = True
                self._log("error", "缺少 websockets 依赖，无法连接协议端（manifest 已声明，请检查安装）")
            return

        while not self._closing:
            try:
                async with websockets.connect(
                    self._build_uri(),
                    open_timeout=10,
                    close_timeout=5,
                    ping_interval=20,
                    ping_timeout=20,
                    max_size=2**23,
                ) as ws:
                    self._ws = ws
                    self._log("info", "已连接协议端 %s:%s", self._host, self._port)
                    await self._safe_state_callback(self._on_connected)
                    async for raw in ws:
                        await self._handle_raw(raw)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if not self._closing:
                    self._log("warning", "协议端连接异常，%s 秒后重连: %s", self._reconnect_delay, exc)
            finally:
                self._ws = None
                self._fail_pending(ConnectionError("影子适配器连接断开"))
                if not self._closing:
                    await self._safe_state_callback(self._on_disconnected)
            if self._closing:
                break
            await asyncio.sleep(self._reconnect_delay)

    # ---------- 接收与 action ----------

    async def _handle_raw(self, raw: Any) -> None:
        try:
            data = json.loads(raw if isinstance(raw, str) else bytes(raw).decode("utf-8"))
        except Exception:
            self._log("debug", "忽略无法解析的 WS 帧")
            return
        for item in split_payloads(data):
            echo = item.get("echo")
            if echo is not None:
                future = self._pending.pop(str(echo), None)
                if future is not None and not future.done():
                    future.set_result(item)
                continue
            if self._on_event is not None:
                try:
                    await self._on_event(item)
                except Exception as exc:
                    self._log("warning", "事件回调异常: %s", exc)

    async def call_action(self, action: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        """调用 OneBot action 并等待响应（echo 配对；超时抛 TimeoutError）。"""

        ws = self._ws
        if ws is None or self._closing:
            raise ConnectionError("影子适配器尚未连接协议端")
        echo = uuid.uuid4().hex
        loop = asyncio.get_running_loop()
        future: asyncio.Future = loop.create_future()
        self._pending[echo] = future
        try:
            await ws.send(json.dumps({"action": action, "params": params or {}, "echo": echo}, ensure_ascii=False))
        except Exception:
            self._pending.pop(echo, None)
            raise
        try:
            response = await asyncio.wait_for(future, self._action_timeout)
        except asyncio.TimeoutError:
            self._pending.pop(echo, None)
            raise
        return response if isinstance(response, dict) else {}

    # ---------- 内部 ----------

    def _fail_pending(self, error: Exception) -> None:
        for future in self._pending.values():
            if not future.done():
                future.set_exception(error)
        self._pending.clear()

    async def _safe_state_callback(self, callback: StateCallback | None) -> None:
        if callback is None:
            return
        try:
            await callback()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._log("warning", "连接状态回调异常: %s", exc)

    def _log(self, level: str, message: str, *args: Any) -> None:
        if self._logger is not None:
            getattr(self._logger, level)(message, *args)
