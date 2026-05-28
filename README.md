# ding-bridge

用钉钉当输入输出终端，在手机上跟本地的 **Claude Code CLI** 对话。

```
手机/PC 钉钉  @MyAI 问题
        │
        │ WebSocket Stream（无需公网）
        ▼
  本机 ding-bridge (Python)
        │
        │ claude -p --output-format stream-json --resume <session>
        ▼
   Claude Code CLI
        │
        │ 流式 JSON 事件
        ▼
  钉钉 AI 卡片打字机效果回显
```

## 1. 前置条件

- 钉钉企业内部应用 + 机器人（**开启 Stream 模式**）
- 拿到 `ClientID` / `ClientSecret` / `RobotCode`
- 本机安装了可用的 Claude Code CLI，`which claude` 有输出
- 本机 Python ≥ 3.10

## 2. 安装

```bash
cd ding-bridge

# 建议用虚拟环境
python3 -m venv .venv
source .venv/bin/activate

pip install -r requirements.txt
```

## 3. 配置

复制示例配置文件并编辑：

```bash
cp config.ini.example config.ini
```

编辑 `config.ini`，至少填好 `[bot:my-ai]` 下的凭证：

```ini
[bot:my-ai]
client_id = your-client-id
client_secret = your-client-secret
robot_code = your-robot-code
allowed_staff_ids = your-staff-id
```

### 多机器人配置

支持在同一进程中运行多个钉钉机器人，每个 `[bot:名称]` section 定义一个机器人，可独立配置 Claude 参数（工作目录、模型、工具白名单等），也可继承 `[claude]` section 的全局默认值。详见 `config.ini.example`。

## 4. 启动

### 方式 A：前台启动（调试用）

```bash
python main.py
```

日志直接打印到终端，Ctrl+C 退出。看到类似日志说明连接成功：

```
INFO [ding-bridge] starting ding-bridge, robot_code=dingxxx cwd=...
INFO [dingtalk_stream] WebSocket connected
```

### 方式 B：用管理脚本后台运行（推荐）

项目里有一个一键管理脚本 `scripts/ding-bridge-manager`，封装了 macOS `launchd` / Linux `nohup`，支持开机自启、崩溃自动拉起、日志落盘。

```bash
# 首次安装到 launchd（开机自启，macOS）
./scripts/ding-bridge-manager install

# 之后改了代码 / config.ini 重启
./scripts/ding-bridge-manager restart

# 看运行状态
./scripts/ding-bridge-manager status
```

详细命令见下一章「5. 服务管理」。

## 5. 服务管理（`ding-bridge-manager`）

> 仅在「方式 B」后台运行时使用。前台 `python main.py` 启动的进程不归这个脚本管。

### 命令速查

```bash
# === 生命周期 ===
./scripts/ding-bridge-manager install      # 首次安装到 launchd（开机自启）
./scripts/ding-bridge-manager start        # 启动（未安装会自动 install）
./scripts/ding-bridge-manager stop         # 优雅停止（10s 未退出会强杀）
./scripts/ding-bridge-manager restart      # 重启（改了代码 / config.ini 后用）
./scripts/ding-bridge-manager reload       # unload + load（改了 plist 本身才需要）
./scripts/ding-bridge-manager uninstall    # 卸载（移除开机自启）

# === 状态与日志 ===
./scripts/ding-bridge-manager status       # PID / CPU / 运行时长 / 最近 5 条日志
./scripts/ding-bridge-manager logs [N]     # 看最近 N 行业务日志（默认 100，不跟踪）
./scripts/ding-bridge-manager follow       # 实时跟踪业务日志（= tail-err）
./scripts/ding-bridge-manager tail-err     # 实时跟踪 stderr.log（业务主日志）
./scripts/ding-bridge-manager tail-out     # 实时跟踪 stdout.log
./scripts/ding-bridge-manager clear-logs   # 清空两份日志
```

> 命令支持简写：`status`→`st`，`logs`→`log`，`follow`→`f`，`tail-err`→`err`，`tail-out`→`out`，`clear-logs`→`clear`。

### 实时看日志（最常用）

业务日志走 Python `logging`，全部写到 `logs/stderr.log`。

```bash
# 推荐：用脚本
./scripts/ding-bridge-manager follow

# 只看关键事件
./scripts/ding-bridge-manager follow | grep -E "incoming|spawn claude|session"

# 只看错误
./scripts/ding-bridge-manager follow | grep -iE "error|warn|exception|traceback"
```

### 常用场景速查

| 场景 | 命令 |
|---|---|
| 改了 `.py` 代码 / `config.ini` | `./scripts/ding-bridge-manager restart` |
| 改了 `plist` 本身 | `./scripts/ding-bridge-manager reload` |
| 机器人没反应，先排查 | `./scripts/ding-bridge-manager status && ./scripts/ding-bridge-manager logs 50` |
| 一边在钉钉发消息一边盯日志 | `./scripts/ding-bridge-manager follow` |
| 日志太大想清掉 | `./scripts/ding-bridge-manager clear-logs` |

## 6. 使用

1. 把机器人拉进任意一个群（或单聊）
2. 发送 `@MyAI 你好`
3. 卡片会先显示"思考中"，然后打字机效果刷新答案

### 特殊指令

| 指令 | 说明 |
|---|---|
| `/clear` / `/new` / `新开会话` / `重置` | 清空当前会话上下文 |
| `/stop` / `停止` / `中断` | 中断正在执行的 CLI 进程 |
| `/goal <目标>` | 设定当前会话目标（重置并以目标开启新对话） |
| `/compact` | 压缩当前会话上下文 |
| `/cron ...` | 管理定时任务（详见定时任务文档） |

## 7. 配置白名单（强烈建议）

首次 @ 一次机器人后，你会在服务端日志看到：

```
INFO [ding-bridge] incoming: staffId=xxxxxxx nick=你的名字 conv=... text='你好'
```

把这个 `staffId` 填到 `config.ini` 对应 bot section 的 `allowed_staff_ids`：

```ini
[bot:my-ai]
allowed_staff_ids = your-staff-id
```

多个人用逗号分隔，支持 `名字:staffId` 格式。重启服务后生效。

## 8. 项目结构

```
ding-bridge/
├── main.py               # Stream 主入口 + 消息分发 + 白名单
├── cli_bridge.py         # 调用 Claude CLI、管理 session_id
├── card_stream.py        # 钉钉 AI 卡片流式渲染
├── cron_scheduler.py     # 定时任务调度器
├── scripts/
│   └── ding-bridge-manager  # 服务管理脚本（start/stop/restart/follow…）
├── logs/                 # 后台运行时的日志输出
│   ├── stderr.log        #   ← 业务主日志（看这里）
│   └── stdout.log
├── requirements.txt
├── config.ini.example    # 配置模板
├── config.ini            # 你的配置（不要提交到 git）
├── .gitignore
└── .sessions/            # 每个会话的 claude session_id 记录（自动生成）
```

## 9. 常见问题

**Q: 连上了但 @ 没反应**
A: 检查三件事：
1. 钉钉开发者后台里，消息推送模式是 **Stream**，不是 HTTP
2. 机器人已发布、已授权、应用状态正常
3. 机器人确实被拉进了你发消息的那个群/单聊

**Q: 卡片显示"CLI 没有返回任何内容"**
A: 命令行里单独跑一下验证 CLI 本身能不能出东西：
```bash
claude -p "hello" --output-format stream-json --verbose
```
如果它自己都不出内容，说明 CLI 登录态或网络有问题，先解决这个。

**Q: 想让机器人在某个项目目录里工作**
A: 把 `config.ini` 里 `[claude]` 的 `cwd` 或对应 bot section 的 `cwd` 改到那个目录。

**Q: 超时**
A: 默认 300s。调大 `[runtime]` 的 `cli_timeout_seconds` 或对应 bot section 的 `cli_timeout_seconds`。

**Q: 后台运行时日志在哪里看？**
A: 全在 `logs/stderr.log`。最方便的方式是 `./scripts/ding-bridge-manager follow` 实时跟踪。

**Q: 改了代码 / `config.ini` 后没生效？**
A: 后台运行的进程不会自动 reload，必须 `./scripts/ding-bridge-manager restart`。

## 10. 安全提醒 ⚠️

Claude Code 能**读写本机文件、执行命令**，把它接到钉钉等于把半个 shell 暴露给群。务必：

- **配置 `allowed_staff_ids` 白名单**，只允许你自己
- **不要**把机器人拉进陌生人多的大群
- 如果要让别人用，建议把 `cwd` 指向一个沙箱目录

## License

MIT
