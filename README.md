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

> **该修复已落地，本插件正式退役（2026-09-27）**：合并版适配器（SnowLuma Adapter 1.0.x，随
> MaiBot 1.3.0 发布，已合并 NapCat 适配器）把通知去重键改为了事件级 SHA-1 摘要，#97 缺陷不复存在。
> 自 **0.3.1** 起，manifest 将宿主版本限制在 **1.2.x**（`max_version: 1.2.99`）：在
> **MaiBot 1.3.0 及以上**，本插件会因 manifest 版本校验不通过而**注册失败，无法加载**，
> 这是预期行为，请直接停用/卸载本插件；1.2.x 环境仍可安装使用。
>
> **1.2.x 环境的额外提醒**：合并版适配器（host 区间 1.2.0 ~ 1.3.99，也可装在 1.2.x）已把聊天
> 黑白名单迁移到宿主统一的 `config/adapter_policy.toml`，适配器配置不再有 `[chat]` 节——
> 本插件的「镜像适配器名单」功能（`filter.sync_from_adapter`）在合并版适配器下无法工作。
> 若在 1.2.x 配合合并版适配器使用「独占中继」模式，请关闭 `sync_from_adapter` 并手动填写名单；
> 默认模式（适配器送达四类通知）下本插件恒为 no-op，镜像永不参与决策，不受影响。

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
| `config_version` | `0.3.2` | 配置版本（自动维护，勿改） |

### `[server]` 协议端连接（与适配器 `[napcat_server]` 填同一组值）

| 字段 | 默认 | 说明 |
|---|---|---|
| `host` / `port` | `127.0.0.1` / `3005` | 协议端正向 WS 地址 |
| `path` | 空 | WS 路径（通常留空） |
| `token` | 空 | 访问令牌（与适配器共用同一协议端 token） |
| `reconnect_delay_sec` | `5.0` | 断线重连初始间隔（秒）：连续失败按指数退避（倍增，上限 300 秒），连接成功后重置 |
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
| `stale_event_max_age_sec` | `600.0` | 断线恢复后积压事件的新鲜度上限（秒）：事件时间早于断线时刻该秒数以上视为过期、放弃补投；`0` = 不检查 |

### `[filter]` 补投范围过滤与可选内容过滤

| 字段 | 默认 | 说明 |
|---|---|---|
| `sync_from_adapter` | `true` | **自动镜像**官方 Napcat 适配器 `[chat]` 名单（群/私聊白黑名单 + `ban_user_id`）：加载、配置热更新、以及快照过期时各同步一次 |
| `adapter_plugin_id` | `maibot-team.napcat-adapter` | 名单来源适配器的插件 id（一般无需改动） |
| `group_list_mode` | `disabled` | 手动模式：`whitelist` / `blacklist` / `disabled`（`sync_from_adapter=false` 时生效） |
| `group_list` | `[]` | 手动模式群号列表 |
| `private_list_mode` | `disabled` | 手动模式私聊名单模式 |
| `private_list` | `[]` | 手动模式私聊用户号列表 |
| `ban_user_id` | `[]` | 手动模式屏蔽用户（其相关通知一律不补投） |
| `mirror_refresh_seconds` | `60` | 镜像名单刷新间隔（秒）：到点后的下一次补投决策会重读适配器名单，适配器侧新增/移除的屏蔽用户与群名单自动生效；`0` = 只在加载与配置热更新时镜像一次 |
| `mirror_fail_closed` | `true` | 镜像读不到适配器名单时是否拒绝一切补投（宁漏投不越权，**0.3.2 起默认开启**）；显式关闭则回退上方手动名单（升级注意：0.3.1 及以前默认关闭，升级后若依赖旧的 fail-open 行为需手动改回 `false`） |
| `block_at_official_bot` | `false` | **内容过滤**：目标消息 @ 了 QQ 官方机器人的通知不补投（判定走群成员资料 `is_robot`，与适配器 `ban_qq_bot` 同口径） |
| `block_at_banned_user` | `false` | **内容过滤**：目标消息 @ 了全局屏蔽用户的通知不补投（名单与范围过滤同源） |

> ✅ **默认开启自动镜像**：插件会读取官方 Napcat 适配器（`maibot-team.napcat-adapter`）`config.toml` 的 `[chat]` 节，把 `enable_chat_list_filter` / `group_list_type` / `group_list` / `private_list_type` / `private_list` / `ban_user_id` 的生效口径镜像为补投范围——适配器名单过滤掉的群/用户的通知不会被越权补投。
>
> ⚠️ **镜像不再只在启动时尝试一次**（0.2.2 修复）：本插件的 `on_load` 早于同一 Runner 组里的 Napcat 适配器注册完成，**首次镜像必然失败**。旧实现失败后不重试，于是永久回退到（默认为空的）手动名单，`ban_user_id` 形同虚设——被全局屏蔽的用户贴表情 / 撤回 / 设精华的通知会被照常补投。现在改为**每次补投决策前按需重试**：
> - 从未镜像成功 → 每次决策都重试，绝不在「名单为空」状态下放行；
> - 快照过期（`mirror_refresh_seconds`）→ 重新读取，适配器侧改名单无需重载本插件；
> - 刷新失败但有旧快照 → 退避重试并继续用旧快照过滤，不会退化成「不过滤」。
>
> 镜像失败会打印**限流告警**并附上读到的顶层键：若列出的是本插件自己的键（`relay` / `filter` 等），说明 `adapter_plugin_id` 填错；若为空则确认是启动竞态，适配器就绪后会自动恢复。**0.3.2 起 `mirror_fail_closed` 默认开启**：镜像不可用期间宁可漏投也不越权补投被屏蔽用户/名单外群的通知（漏投仅观感损失，越权投递是真实的策略绕过）；确认要回退手动名单的行为可显式关闭。
> 关闭 `sync_from_adapter` 后回退到本节的**手动名单**（供无官方适配器/自研适配器场景使用）。

> ✅ **0.3.0 修复：镜像刷新失败不再打穿全局屏蔽**——旧版每次刷新开头就清空快照，读取一旦失败（瞬时抖动/适配器重载窗口），已镜像的 `ban_user_id` 随之失效、过滤退化为空手动名单，被全局屏蔽用户的通知重新被补投。现在快照**只在成功读到完整名单时才整体替换**，失败期间旧快照继续生效并按 10 秒退避重试；镜像遇到非法的名单模式值时与官方适配器同样回退为 `whitelist`（宁严勿松）。

### 可选内容过滤：目标消息 @ 名单（`block_at_*`，默认关闭）

补投决策时可通过 `get_msg` 查看通知指向的**目标消息**（被回应/被撤回/被设精华的那条消息）的 @ 列表，命中即跳过补投：

- `block_at_official_bot`：目标消息 @ 了 **QQ 官方机器人**。判定走 `get_group_member_info` 返回的 `is_robot` 字段，与官方适配器 `ban_qq_bot`（`NapCatOfficialBotGuard`）同口径。典型场景：官方机器人对@它的消息自动贴表情回应，通知刷屏。
- `block_at_banned_user`：目标消息 @ 了**全局屏蔽用户**（名单与范围过滤同源：镜像适配器 `[chat].ban_user_id` 或手动名单）。

行为约定：

- **默认关闭，关闭时零开销**（不产生任何协议端请求）；开启后每个候选补投多 1 次 `get_msg` 查询（带缓存：成功 900 秒/失败 30 秒；机器人判定成功 600 秒/失败 60 秒，连接断开或配置热更新后失效）；
- 仅群聊通知参与（表情回应/群撤回/精华）；私聊 `friend_recall` 无 @ 语义，不参与；
- 目标消息查不到（撤回后过期、协议端不支持 `get_msg`）或 @ 列表为空时**放行**——内容过滤是降噪开关而非越权防线，宁可多投不误伤；
- 两个开关只影响**本插件的补投路径**：适配器已正常送达的通知（跳过补投）与适配器正常注入的普通消息均不受影响。

## 安装与验证

1. 把 `cateye_napcat_shadow_adapter` 目录复制到 MaiBot 安装目录的 `plugins/` 下；
2. 确认 `[server]` 的 host/port/token 与 Napcat 适配器 `[napcat_server]` 一致；
3. 重启 MaiBot，观察日志：`已连接协议端` → `影子网关已就绪（account_id=...）`；
4. **验证补投**：对同一条群消息在 300 秒内连续贴两个不同表情——日志应出现一次 `已补投 group_msg_emoji_like 通知`（第一个表情由适配器送达，被跳过；第二个被适配器去重丢弃，由本插件补投），WebUI 聊天记录能看到两条回应记录；
5. **验证跳过**：对一条 5 分钟前的旧消息贴表情（适配器版本可正常入站）——日志应出现 `适配器已送达该通知，跳过补投`，且不会出现重复记录。

## 注意事项

- 本插件**只补四类通知**，普通消息事件一律不碰——消息入站仍由适配器独家负责，不会出现双份消息。
- 本插件声明的宿主能力仅 `config.get_plugin`（用于自动镜像官方适配器名单）：`route_message` / `update_state` 走宿主专用 RPC 免声明，Hook 免声明，昵称查询走本插件自己的 WS 连接而非适配器 API。**manifest 已新增能力与依赖声明，升级本插件后需完整重启 MaiBot 一次**（manifest 变更不做热重载）。
- 自动镜像为**软依赖**：官方适配器 `maibot-team.napcat-adapter` 未安装/停用时，镜像告警并回退手动名单，其余功能不受影响；随后 napcat 加载后，重载本插件（或改本插件任意配置）即可重新镜像。
- 机器人自己贴表情的通知也会被照常处理（与适配器行为一致）；若安装了表情回应翻译插件，其自带的"机器人自身回应跳过翻译"逻辑不受影响。
- 修改 `[server]` 配置后无需重启，插件会自动重建连接；修改名单后无需重载——镜像最多 `mirror_refresh_seconds`（默认 60 秒）后自动跟上，想立刻生效可重载本插件或改本插件任意配置触发热更新。
- **断线恢复不补投陈年通知**（0.3.2 起）：断线期间 WS 帧只积压不丢失，重连后会一次性涌入；事件时间早于断线时刻超过 `relay.stale_event_max_age_sec`（默认 600 秒）的积压事件直接放弃补投（实时消息仍由适配器兜底），避免断线恢复后冒出一批"迟到的撤回/表情回应"。重连按指数退避（初始 `reconnect_delay_sec`，倍增，上限 300 秒，连接成功后重置）。
- **access_token 走 URL query**：OneBot v11 规范允许，NapCat/SnowLuma 普遍如此，属协议生态现状。token 可能进入协议端 access log / 中间代理日志，请注意不要在日志系统记录完整连接 URL；本插件自身的日志与异常文本已对 token/URI query 脱敏。

## 故障排查

| 现象 | 处理 |
|---|---|
| 日志反复出现"协议端连接异常" | 核对 `[server]` 的 host/port/token 是否与适配器 `[napcat_server]` 一致；协议端正向 WS 是否开启 |
| 日志出现"缺少 websockets 依赖" | 删除插件目录后重新放入让 Runner 重装依赖，或手动 `pip install websockets` |
| 补投的通知没进 WebUI 聊天记录 | 查主进程日志是否出现"宿主拒绝了补投"（网关未就绪/被去重）；确认 `[filter]` 未误过滤 |
| **被屏蔽用户的通知仍被补投** | 先确认部署目录里的插件版本 ≥ 0.2.2（0.2.0 没有名单重试机制，必然复现）；再看日志有没有 `配置缺少 [chat] 节`：0.2.2 起会自动重试并打印读到的顶层键，0.3.0 起刷新失败还会保留旧快照继续过滤，0.3.2 起 `mirror_fail_closed` **默认开启**（镜像不可用期间直接拒投）。若顶层键是本插件自己的（`relay` / `filter`），说明 `filter.adapter_plugin_id` 填错；若想恢复旧版"回退手动名单"的 fail-open 行为，把 `filter.mirror_fail_closed` 显式改为 `false`，并在 `[filter]` 手动名单里填好要屏蔽的 QQ |
| 完全看不到补投日志 | 确认 `[plugin] enabled=true`、对应类型开关开启；确认事件确实发生在接管四类内 |
| 适配器与插件同时报某通知 | 属预期内的偶发重复（适配器副本晚于去抖窗口），可适当调大 `debounce_seconds` |
