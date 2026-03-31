# st-cli

Sensor Tower 命令行工具：**Cookie 提取**对齐 rdt-cli：`uv run --with browser-cookie3` 子进程读库（避免 SQLite 锁）。**HTTP 头**按 Sensor Tower 网页里对 `/api/*` 的 XHR（带 `Origin`、`Referer`、`X-Requested-With`），与 Reddit 的只读接口头不完全相同。Cookie 写入 `~/.config/st-cli/credential.json`，请求走 **httpx**。

## 依赖

- Python 3.10+
- 本机已用浏览器登录过 [Sensor Tower](https://app.sensortower.com)（Chrome / Firefox / Edge / Brave 之一，供 `st login` 读 Cookie）

## 安装

推荐给他人使用（全局安装，像 rdt-cli 一样直接 `st ...`）：

```bash
uv tool install st-cli
```

## 认证

1. 在浏览器中打开并登录 `https://app.sensortower.com`。
2. 在同一台机器上执行：

```bash
st login --json
```

会话文件：`~/.config/st-cli/credential.json`（权限 `600`）。过期策略见 `st_cli/auth.py`（默认可尝试从浏览器自动刷新）。

退出登录：

```bash
st logout
```

### 已登录但 `st login` 仍报 403 / `api_ok: false`

1. **先完全退出 Chrome（或你用来登录 ST 的浏览器）再执行 `st login`**，与 rdt-cli 一样用子进程读 Cookie，避免 SQLite 锁导致读到旧/空数据。
2. **Chrome 非 Default 配置**：`export ST_CHROME_COOKIES_DB="$HOME/Library/Application Support/Google/Chrome/Profile 1/Cookies"`（路径按本机 Profile 名），再 `uv run st login`。
3. 提取到的 Cookie 必须至少包含 `sessionToken`、`sensor_tower_session`、`.ASPXAUTH` 之一（见 `constants.REQUIRED_COOKIES`），否则不会保存。
4. 仍 403 时看 `details.body_preview`；若为空，多为边缘/WAF，可换网络或稍后重试。

## 命令

| 命令 | 说明 |
|------|------|
| `st login` | 从浏览器提取并保存 Cookie，并探测 autocomplete API |
| `st logout` | 删除已保存的凭据文件 |
| `st status` | 用当前凭据探测 ST API 是否可用 |
| `st fetch "<URL 或 应用名>"` | 自动完成 → `internal_entities` → **最近 36 个月** `/api/apps/facets` 收入 |
| `st batch -f queries.txt` | 对文件中每行执行与 `fetch` 相同的流水线（每行取自动完成第一条） |

多结果时 `fetch` 会返回 `needs_disambiguation` 与 `candidates`，请再执行：

```bash
st fetch "Duolingo" --pick 1 --json
```

## 输出

默认与 `rdt-cli` 类似：非 TTY 输出 YAML；`--json` 输出结构化 envelope：

```json
{ "ok": true, "schema_version": "1", "data": { ... } }
```

## 开发

```bash
uv run pytest
uv run ruff check st_cli tests
```

## 说明

- 月度收入来自 ST 的 **`/api/apps/facets`**，按自然月拆分请求（36 次），请勿并发过猛。
- 若 ST 改版导致字段变化，需调整 `st_cli/st_api.py` 中的解析逻辑。
