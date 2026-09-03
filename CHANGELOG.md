# 更新日志

## 0.2.0（2026-09-04）

- **自动镜像官方 Napcat 适配器 `[chat]` 黑白名单**：新增 `sync_from_adapter`（默认开启），插件加载/自身配置热更新时通过 `config.get_plugin` 读取 `maibot-team.napcat-adapter` 的 `[chat]` 节（群/私聊白黑名单 + `ban_user_id`），消除手动镜像名单的越权补投风险；失败自动回退手动名单。
- 范围过滤新增**私聊名单维度**（`private_list_mode` / `private_list`），`friend_recall` 补投遵循适配器私聊名单口径。
- manifest：新增 `config.get_plugin` 能力声明；适配器名单读取为软依赖（未装官方适配器时回退手动名单）。**升级需完整重启 MaiBot**。

## 0.1.0（2026-09-03）

- 首个版本：影子中继上线。
- 独立 OneBot WS 连接协议端，只订阅四类易丢通知（表情回应/群撤回/好友撤回/精华）。
- `chat.receive.before_process` 只读 Hook 登记适配器已送达的事件摘要；去抖窗口后缺位即补投。
- 补投 `dedupe_key` 使用事件摘要（`shadow:{sha1}`），与适配器键空间隔离。
- 昵称/群名富化（走自有 WS 查询 + 缓存），兼容下游表情回应翻译插件。
- 上游修复 #97 后自动退化为 no-op；支持 SnowLuma 适配器兼容字段与"独占中继"模式。
