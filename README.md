# st-cli

源码仓库：<https://github.com/ronaldo123321/st-cli>（`git clone https://github.com/ronaldo123321/st-cli.git`）。

这是一个基于浏览器 Cookie 登录的 Sensor Tower 命令行工具（类似 rdt-cli 的登录体验）：

- **登录方式**：从本机浏览器提取 Cookie（不需要手动复制 token）
- **数据来源**：调用 Sensor Tower 网页端使用的 `/api/*` 接口（httpx）
- **凭据位置**：`~/.config/st-cli/credential.json`

## 依赖

- Python 3.10+
- 本机浏览器已登录过 [Sensor Tower](https://app.sensortower.com)（Chrome / Firefox / Edge / Brave 之一，供 `st login` 读取 Cookie）

## 安装

推荐给他人使用（全局安装，像 rdt-cli 一样直接 `st ...`）。

PyPI 上的包名是 **`sensortower-st-cli`**（`st-cli` 与已有项目名过于相似，无法占用）；安装后命令行仍是 **`st`**。

```bash
uv tool install sensortower-st-cli
```

## 认证

1. 在浏览器中打开并登录 `https://app.sensortower.com`
2. 在同一台机器上执行

```bash
st login --json
```

会话文件：`~/.config/st-cli/credential.json`（权限 `600`）。

退出登录：

```bash
st logout
```

### 已登录但 `st login` 仍报 403 / `api_ok: false`

按下面顺序排查（大多数情况第 1 条就能解决）：

1. **完全退出浏览器后重试**：先彻底退出 Chrome/Brave/Edge/Firefox，再执行 `st login`（避免 Cookie SQLite 锁导致读到旧/空数据）。
2. **指定 Chrome Cookies 路径**（多 Profile 场景常见）：

```bash
export ST_CHROME_COOKIES_DB="$HOME/Library/Application Support/Google/Chrome/Profile 1/Cookies"
st login --json
```

3. **看错误详情**：若是 403，重点看 `st status --json` 返回里的 `response_headers` / `body_preview`；如果出现 Cloudflare 相关头（例如 `cf-ray`），可能是网络/WAF 限制，换网络或稍后重试。

## 快速开始（复制粘贴即可）

```bash
# 1) 登录
st login --json

# 2) 检查会话可用
st status --json

# 3) 拉一个应用的“最近 12 个月”月度收入（估算）
st fetch "QuickBooks" --json
```

## 常用命令一览

| 命令 | 说明 |
|------|------|
| `st login` | 从浏览器提取并保存 Cookie，并探测 autocomplete API |
| `st logout` | 删除已保存的凭据文件 |
| `st status` | 用当前凭据探测 ST API 是否可用 |
| `st fetch "<URL 或 应用名>"` | 自动完成 → 解析 app → 拉取 **最近 12 个月**月度收入（估算） |
| `st batch -f queries.txt` | 对文件中每行执行与 `fetch` 相同的流水线（每行取自动完成第一条） |
| `st landscape ...` | 抓取竞品格局数据（JSON/YAML），可选 `--out` 直接渲染 Markdown 报告 |
| `st landscape-report ...` | **只做渲染**：把 `st landscape --json` 的输出渲染成 Markdown（不重复请求 ST） |

多结果时 `fetch` 会返回 `needs_disambiguation` 与 `candidates`，请再执行：

```bash
st fetch "Duolingo" --pick 1 --json
```

## 竞品格局（`st landscape`）

准备一个竞品名单文件（每行一个名称）：

```text
QuickBooks
Xero
Zoho Books
Wave
FreshBooks
Sage
```

然后运行（示例）：

```bash
ST_FACET_REGIONS=global st landscape \
  --names-file competitors_batch1.txt \
  --category 0 \
  --limit 6 \
  --json \
  --out report.md
```

- 输出的核心列（报告与 JSON 都会包含）：
  - **`{YYYY-MM} Revenue`**：最近一个月收入（USD，as-of 口径）
  - **`6M Growth`**：相比 6 个月前的增长率（百分比）
  - **`First Release`**：首次发布时间（US）

- **`ST_FACET_REGIONS=global`**：用于市场份额/Top apps 的分母口径（内部会展开成一组区域列表）
- **`--category`**：用于市场份额分母（SensorTower category id）。不确定时可先用 `0`，但这会让 “share” 变成“全市场 proxy”，解释时要小心
- **`--limit`**：最多输出 N 个竞品（会对输入名单做解析/匹配，个别名称可能解析失败）
- **`--out`**：生成 Markdown 报告路径

### 数据与渲染解耦用法（推荐）

先抓数据（产物可复用）：

```bash
st landscape --names-file competitors_batch1.txt --json > landscape.json
```

再单独渲染报告（不再请求 Sensor Tower）：

```bash
st landscape-report --in landscape.json --out report.md --json
```

也支持 stdin 管道：

```bash
st landscape --names-file competitors_batch1.txt --json | st landscape-report --out report.md --json
```

### `st landscape` JSON 字段补充说明

`st landscape --json` 的 `data.competitors[].st` 中新增/常用字段：

- **`revenue_as_of_current_month_usd`**：最近一个月收入（USD）
- **`revenue_6_months_ago_usd`**：6 个月前对应月份的收入（USD）
- **`growth_vs_6m_percent`**：相对 6 个月前的增长率（百分比数值；无法计算时为 `null`）
- **`first_release_date_us`**：首次发布时间（来自 facets/v2）

## 输出

默认与 `rdt-cli` 类似：非 TTY 输出 YAML；`--json` 输出结构化 envelope：

```json
{ "ok": true, "schema_version": "1", "data": { ... } }
```

## FAQ

### 为什么会很慢？

很多接口需要按月份窗口拆分请求（例如 12 个月就可能有 12 次请求），且 Sensor Tower 侧可能对请求频率敏感。

### 为什么会卡住/没有输出文件？

通常是某个请求被 WAF/网络阻断或长时间等待。建议先跑：

```bash
st status --json
```

并单独用 `st fetch "<name>" --json` 验证某个名称能否被正常解析与拉取。

## 开发

```bash
uv run pytest
uv run ruff check st_cli tests
```

## 说明

- 月度收入/份额相关数据来自 ST 的 `/api/*`，通常需要多次请求拼装结果，请避免并发过猛。
- 若 ST 改版导致字段变化，需调整 `st_cli/st_api.py` / `st_cli/pipeline.py` 的解析逻辑。
