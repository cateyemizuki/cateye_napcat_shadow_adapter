# NapCat 影子适配器（cateye.napcat-shadow-adapter）

一个"影子中继"插件：**监控 NapCat 适配器的四类易丢通知，适配器没送到的自动补进 MaiBot，送到的自动跳过**。不改写、不拦截、不修改官方适配器的任何配置与代码。

- 作者：cateye
- 背景 Issue：[MaiBot-Napcat-Adapter #97 — group_msg_emoji_like 通知去重键误用被回应消息 message_id，同一条消息 300 秒内多次贴表情被忽略](https://github.com/Mai-with-u/MaiBot-Napcat-Adapter/issues/97)

---

## 背景：为什么需要这个插件

MaiBot-Napcat-Adapter 为通知事件构造去重键时直接使用了载荷里的 `message_id`，但对本文接管的四类通知而言，这个 `message_id` 是"被引用消息"的 ID 而非事件自身的唯一标识。宿主入站去重（TTL 300 秒）因此会把后续事件误判为重复而静默丢弃。受影响的通知（下文合称**误丢信息**）：

| 通知类型 | 丢失方式 |
|---|---|
| `group_msg_emoji_like` 表情回应 | 同一条消息 300 秒内换表情/取消/重贴、甚至**不同用户**的回应，除首次外全丢 |
| `group_recall` 群消息撤回 | 与消息事件、表情回应、精华共享键空间：消息进站后 5 分钟内被撤回 → 撤回通知被丢 |
| `friend_recall` 好友消息撤回 | 私聊消息进站后 5 分钟内被撤回 → 撤回通知被丢 |
| `essence` 精华消息 | 设精/移除/再设互撞；与消息、撤回、表情回应跨类型互撞 |

## ⚠️ 上游修复后本插件自动失效

[Issue #97](https://github.com/Mai-with-u/MaiBot-Napcat-Adapter/issues/97) 如果被官方修复（通知去重键改为事件级摘要），适配器的版本将永远先行入站，本插件观测到"已送达"后不再补投，**自动退化为 no-op**——行为上等于不存在，可随时安全卸载，无需急着删。

## 工作原理

```
NapCat/SnowLuma 本体 ──WS广播──┬─> Napcat 适配器 ──route_message(裸 message_id 键)──> 宿主去重(300s)
                               │                                                        │
                               └─> 本插件 WS 连接 ──2 秒去抖决策───────────────────────>│ 入站链
                                                                                        ▼
                                     chat.receive.before_process（只读 Hook：登记"适配器已送达"的事件摘要）
                                                                                        ▼
                                     适配器已送达 → 跳过 ｜ 未见 → 本插件 receive 网关补投（is_notify 合成通知）
```

- **匹配粒度是"事件"而不是"消息"**：摘要取 `notice_type / sub_type / group_id / user_id / operator_id / sender_id / message_id / message_seq / likes / time` 的规范化 JSON SHA-1。同一消息上"贴表情 A"与"取消 A"是两个事件、两个摘要——适配器送达了前者，后者仍会被正确补投。
- **键空间隔离**：补投的 `dedupe_key` 为 `shadow:{摘要}`，与适配器的裸 `message_id` 键互不相交，补投永远不会被适配器的历史键误伤，也不会污染适配器的去重。
- **下游兼容**：补投消息的 `additional_config` 完整携带 `napcat_notice_type / napcat_notice_sub_type / napcat_notice_payload`（原始事件）与 `is_notify=True`，与适配器注入的口径一致。表情回应翻译插件（如 `cateye_set_msg_emoji_like`）的 Hook 无需任何改动即可正常处理补投副本。
- **失败模式**：最坏情况是适配器副本晚于去抖窗口（默认 2 秒）才入站，产生一条重复通知（仅观感问题）；显著优于修复前"通知系统性丢失"。本插件 WS 断连期间自动退回"适配器版本兜底"。

### 设计上的三个额外好处

1. **上游修复后自动失效、零维护**：适配器修复 #97 后其版本永远先行入站，本插件自动 no-op，无需跟进发版、无需手动关闭。
2. **兼容 SnowLuma 适配器**：SnowLuma 适配器的通知同时写入 `napcat_notice_*` 兼容字段，本插件原样工作；且 SnowLuma 适配器本身不存在该去重缺陷，此时本插件恒为 no-op（所有事件都会被"已送达"登记命中）。
3. **支持"独占中继"模式**：如果你愿意改适配器配置（`config.toml` 中 `[notice]` 的 `enable_group_msg_emoji_like / enable_group_recall / enable_friend_recall / enable_essence` 置 `false`），适配器将不再注入这四类通知，本插件 Hook 永远观测不到"已送达"，自动成为这四类信息的**唯一来源**——两种模式无需改本插件任何代码。

## 配置说明

运行时 `config.toml` 由 Runner 自动生成（WebUI 也可改），各节如下：

### `[plugin]`

| 字段 | 默认 | 说明 |
|---|---|---|
| `enabled` | `true` | 总开关（关闭则不连协议端、不补投） |
| `config_version` | `0.1.0` | 配置版本（自动维护，勿改） |

### `[server]` 协议端连接（与适配器 `[napcat_server]` 填同一组值）

| 字段 | 默认 | 说明 |
|---|---|---|
| `host` / `port` | `127.0.0.1` / `3005` | 协议端正向 WS 地址 |
| `path` | 空 | WS 路径（通常留空） |
| `token` | 空 | 访问令牌（与适配器共用同一协议端 token） |
| `reconnect_delay_sec` | `5.0` | 断线重连间隔 |
| `action_timeout_sec` | `5.0` | 查昵称/群名 action 超时 |

### `[relay]` 接管与补投

| 字段 | 默认 | 说明 |
|---|---|---|
| `enable_group_msg_emoji_like` | `true` | 接管表情回应通知 |
| `enable_group_recall` | `true` | 接管群撤回通知 |
| `enable_friend_recall` | `true` | 接管好友撤回通知 |
| `enable_essence` | `true` | 接管精华消息通知 |
| `debounce_seconds` | `2.0` | 去抖窗口：等待适配器版本入站后再决策 |
| `seen_ttl_seconds` | `120.0` | "适配器已送达"登记保留时长 |
| `resolve_nicknames` | `true` | 补投前查询昵称/群名（走本插件自己的 WS 连接，带缓存；失败回退 QQ 号） |
| `nickname_cache_ttl_sec` | `600.0` | 昵称缓存时长 |

### `[filter]` 补投范围过滤

| 字段 | 默认 | 说明 |
|---|---|---|
| `group_list_mode` | `disabled` | `whitelist` / `blacklist` / `disabled` |
| `group_list` | `[]` | 群号列表 |
| `ban_user_id` | `[]` | 屏蔽用户（其相关通知一律不补投） |

> ⚠️ 如果适配器侧配置了群名单/屏蔽用户（`[chat]` 节），请在本节**镜像相同配置**，否则适配器过滤掉的群的通知会被本插件越权补投。适配器未用名单（默认全放行）时保持 `disabled` 即可。

## 安装与验证

1. 把 `cateye_napcat_shadow_adapter` 目录复制到 MaiBot 安装目录的 `plugins/` 下；
2. 确认 `[server]` 的 host/port/token 与 Napcat 适配器 `[napcat_server]` 一致；
3. 重启 MaiBot，观察日志：`已连接协议端` → `影子网关已就绪（account_id=...）`；
4. **验证补投**：对同一条群消息在 300 秒内连续贴两个不同表情——日志应出现一次 `已补投 group_msg_emoji_like 通知`（第一个表情由适配器送达，被跳过；第二个被适配器去重丢弃，由本插件补投），WebUI 聊天记录能看到两条回应记录；
5. **验证跳过**：对一条 5 分钟前的旧消息贴表情（适配器版本可正常入站）——日志应出现 `适配器已送达该通知，跳过补投`，且不会出现重复记录。

## 注意事项

- 本插件**只补四类通知**，普通消息事件一律不碰——消息入站仍由适配器独家负责，不会出现双份消息。
- 本插件声明了**零宿主能力**（manifest `capabilities` 为空）：`route_message` / `update_state` 走宿主专用 RPC 免声明，Hook 免声明，昵称查询走本插件自己的 WS 连接而非适配器 API。
- 机器人自己贴表情的通知也会被照常处理（与适配器行为一致）；若安装了表情回应翻译插件，其自带的"机器人自身回应跳过翻译"逻辑不受影响。
- 修改 `[server]` 配置后无需重启，插件会自动重建连接；manifest 变更（如能力声明）仍需完整重启 MaiBot。

## 故障排查

| 现象 | 处理 |
|---|---|
| 日志反复出现"协议端连接异常" | 核对 `[server]` 的 host/port/token 是否与适配器 `[napcat_server]` 一致；协议端正向 WS 是否开启 |
| 日志出现"缺少 websockets 依赖" | 删除插件目录后重新放入让 Runner 重装依赖，或手动 `pip install websockets` |
| 补投的通知没进 WebUI 聊天记录 | 查主进程日志是否出现"宿主拒绝了补投"（网关未就绪/被去重）；确认 `[filter]` 未误过滤 |
| 完全看不到补投日志 | 确认 `[plugin] enabled=true`、对应类型开关开启；确认事件确实发生在接管四类内 |
| 适配器与插件同时报某通知 | 属预期内的偶发重复（适配器副本晚于去抖窗口），可适当调大 `debounce_seconds` |
