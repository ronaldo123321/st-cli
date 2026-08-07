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

PyPI 上的包名是 **`sensortower-st-cli`**；安装后命令行仍是 **`st`**。

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

### `st status` / `st fetch` 报 422 或提示缺少 CSRF

Sensor Tower 近期开始要求部分 GET 接口也携带 `x-csrf-token`，包括 `st status` 与 `st fetch` / `st snapshot` 入口依赖的 autocomplete。`sensortower-st-cli>=0.2.1` 会在调用这些 GET 接口前从网页端抓取 CSRF token 并自动附加请求头。

如果你还在使用 `0.2.0` 或更早版本，遇到 `HTTP 422` 时请升级：

```bash
uv tool upgrade sensortower-st-cli
```

升级后重新执行：

```bash
st status --json
```

## 区域维度（`fetch` / `batch` / `snapshot` / `landscape`）

调用 Sensor Tower facets 时，工具会把一组 **国家/地区代码**（两字母，与 ST 一致）传给接口。默认行为如下：

- **默认**：使用内置的 **`GLOBAL_FACET_REGIONS`** 全表（与以前需要设置 `ST_FACET_REGIONS=global` 时等价），覆盖 README 对应源码 `st_cli/constants.py` 中的列表。
- **仅美国**：`export ST_FACET_REGIONS=US`（也可用 `ST_REGIONS`，二者取先设置的那个）。
- **自定义多个国家**：逗号分隔，例如 `export ST_FACET_REGIONS=US,GB,JP`。
- **显式「全球」关键字**（与默认列表相同）：`global`、`world`、`worldwide`、`ww`（不区分大小写）。

区域列表在进程启动时从环境变量解析一次；改环境变量后需重新执行命令。默认使用多区域时，单次请求体更大、总耗时通常比只查 US 更长。

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
| `st version "<URL / iOS 数字 id / 包名 / 应用名>"` | 拉取 ST **Update Timeline**（`/api/ios|android/app_update/get_app_update_history`），默认仅近 180 天 |
| `st batch -f queries.txt` | 对文件中每行执行与 `fetch` 相同的流水线（每行取自动完成第一条） |
| `st aso-keywords ...` | 自动化 Store Marketing → ASO Keywords：添加关键词、下载 CSV |
| `st snapshot ...` | 按 **任意起止日期** 拉取单 app 或竞品列表的区间快照，支持 raw / landscape / both 三种输出 |
| `st snapshot-report ...` | 把 `st snapshot --json` 产出的 JSON 渲染成 Markdown 摘要，不再重复请求 ST |
| `st landscape ...` | 抓取竞品格局数据（JSON/YAML），可选 `--out` 直接渲染 Markdown 报告 |
| `st landscape-report ...` | **只做渲染**：把 `st landscape --json` 的输出渲染成 Markdown（不重复请求 ST） |

多结果时 `fetch` 会返回 `needs_disambiguation` 与 `candidates`，请再执行：

```bash
st fetch "Duolingo" --pick 1 --json
```

## 版本更新时间线（`st version`）

对应网页 **Update Timeline**；数据来自 ST 的 `get_app_update_history` 接口（需有效登录与 CSRF，与网页一致）。

```bash
# iOS：数字 App Store id（或 App Store URL，与 fetch 同源解析）
st version 389801252 --country US --json

# Android：Play 链接或包名（`com.example.app`）
st version "https://play.google.com/store/apps/details?id=com.instagram.android" --json

```

成功时 `data.versions` 为时间线列表，每项包含 **`time`**、**`version`**、**`featured_user_feedback`**、**`name`**、**`description`**、**`subtitle`**、**`icon`**、**`screenshot`**、**`top_in_app_purchase`**、**`events`**。默认只保留 **`time` 落在最近 180 天（UTC，相对当前时间）** 内的记录；可用 `--max-age-days N` 调整窗口（例如 `365` 看近一年）。`data.max_age_days` 反映本次使用的窗口。`data.platform` / `data.app_id` 标明商店与 id。多结果时需加 `--pick N`（与 `fetch` 相同）。

## ASO Keywords（`st aso-keywords`）

对应网页 **Store Marketing → ASO Keywords**，用于把 ASO 关键词页面上的常用动作自动化，尤其适合批量维护关键词和把页面表格数据落成 CSV 给后续分析使用。

支持 4 个子命令：

| 命令 | 用途 | 是否会改动 ST 页面数据 |
|------|------|------------------------|
| `st aso-keywords user-apps` | 列出当前账号可用的 ASO app / keyword view | 否 |
| `st aso-keywords metadata` | 查看某个 ASO app/view 的元数据 | 否 |
| `st aso-keywords add` | 添加关键词，对应页面右侧 `+` / Add new keywords manually | 是 |
| `st aso-keywords export` | 下载 ASO Keywords 表格 CSV，对应右上角 `Download CSV` | 否 |

### App 定位方式

日常使用推荐直接传业务上熟悉的 app 标识，CLI 会自动从当前账号的 ASO app 列表里解析到 Sensor Tower 内部 keyword view id。

优先推荐：

- `--app-id`：iOS App Store id，例如 `6477533581`；Android 可传 package name，例如 `com.example.app`；也支持 ST unified app id。
- `--app-name`：按当前账号 ASO app 列表里的 app 名称模糊匹配，例如 `"AI Remodel"`。

高级兜底：

- `--user-app-id`：Sensor Tower ASO user app / keyword view id。
- `--keyword-view-id`：仅 `export` 使用，是 `--user-app-id` 的同义参数。

`--user-app-id` / `--keyword-view-id` 一般不需要用户手动获取。它们来自页面 URL 里的 `aso_keyword_view=...`，例如：

```text
https://app.sensortower.com/store-marketing/aso/performance-tracking?...&aso_keyword_view=69bd1448c906d57604ec067a
```

如果自动匹配失败，先查看当前账号有哪些 ASO app：

```bash
st aso-keywords user-apps --json
```

常见输出字段：

| 字段 | 含义 |
|------|------|
| `id` | Sensor Tower 内部 ASO keyword view id，可作为 `--user-app-id` / `--keyword-view-id` 兜底使用 |
| `appId` | iOS App Store id 或 Android package name |
| `appName` | App 名称 |
| `os` | `ios` / `android` |
| `country` | 默认国家 |
| `numTerms` | 当前 ASO view 里追踪的关键词数量 |

示例输出节选：

```json
{
  "id": "69bd1448c906d57604ec067a",
  "appId": 6477533581,
  "appName": "AI Remodel — Interior Design",
  "os": "ios",
  "country": "US",
  "numTerms": 85
}
```

### 添加关键词

```bash
st aso-keywords add \
  --app-id 6477533581 \
  --os ios \
  --country US \
  --keyword "bible note" \
  --keyword "sermon notes" \
  --json
```

入参说明：

| 参数 | 必填 | 说明 |
|------|------|------|
| `--app-id` / `--app-name` / `--user-app-id` | 三选一 | 定位要操作的 ASO app/view。推荐 `--app-id` |
| `--os` | 否 | `ios` 或 `android`，默认 `ios` |
| `--country` | 否 | 国家/地区代码，默认 `US` |
| `--keyword` | 与 `--keywords-file` 二选一 | 要添加的关键词，可重复传，也可逗号分隔 |
| `--keywords-file` | 与 `--keyword` 二选一 | 文本文件，每行一个关键词；空行和 `#` 开头行会被忽略 |
| `--json` / `--yaml` | 否 | 输出格式 |

输出说明：

| 字段 | 含义 |
|------|------|
| `user_app_id` | CLI 实际解析到的内部 ASO view id |
| `matched_app` | 匹配到的 app 摘要，包含 `appId` / `appName` / `os` / `country` / `numTerms` |
| `terms` | 实际提交给 ST 的关键词列表，已去重 |
| `result` | Sensor Tower 接口返回，例如 `{ "success": true }` |

文件批量添加示例：

```bash
st aso-keywords add \
  --app-name "AI Remodel" \
  --country US \
  --keywords-file ./keywords.txt \
  --json
```

`keywords.txt` 示例：

```text
bible note
sermon notes
# 这一行会被忽略
church notes
```

### 下载 CSV

```bash
st aso-keywords export \
  --app-id 6477533581 \
  --os ios \
  --country US \
  --device iphone \
  --start 2026-02-12 \
  --end 2026-05-12 \
  --out ./aso-keywords.csv \
  --json
```

入参说明：

| 参数 | 必填 | 说明 |
|------|------|------|
| `--app-id` / `--app-name` / `--keyword-view-id` / `--user-app-id` | 四选一 | 定位要导出的 ASO app/view。推荐 `--app-id` |
| `--os` | 否 | `ios` 或 `android`，默认 `ios` |
| `--country` | 否 | 国家/地区代码，默认 `US` |
| `--device` | 否 | 设备筛选，例如 `iphone` / `ipad` / `android`，默认 `iphone` |
| `--start` | 是 | 主周期开始日期，格式 `YYYY-MM-DD` |
| `--end` | 是 | 主周期结束日期，格式 `YYYY-MM-DD` |
| `--comparison-start` | 否 | 对比周期开始日期；不传则自动使用上一段等长周期 |
| `--comparison-end` | 否 | 对比周期结束日期；不传则自动使用上一段等长周期 |
| `--search-term` | 否 | 对表格关键词做搜索过滤，可重复传 |
| `--limit` | 否 | 导出行数上限，默认 `10000` |
| `--out` | 与 `--stdout` 二选一 | 写入 CSV 文件 |
| `--stdout` | 与 `--out` 二选一 | 直接把 CSV bytes 输出到 stdout |
| `--json` / `--yaml` | 否 | `--out` 模式下输出执行结果 |

日期说明：

- `--start` / `--end` 对应网页左上角日期范围。
- `--comparison-start` / `--comparison-end` 用于计算 PoP growth 等对比字段。
- 如果不传 comparison 日期，CLI 自动使用上一段等长周期。例如 `2026-02-12` 到 `2026-05-12` 会自动对比 `2025-11-14` 到 `2026-02-11`。

输出说明：

| 字段 | 含义 |
|------|------|
| `keyword_view_id` / `user_app_id` | CLI 实际解析到的内部 ASO view id |
| `matched_app` | 匹配到的 app 摘要 |
| `out_file` | CSV 写入路径 |
| `bytes` | 写入文件大小 |

CSV 内容说明：

- Sensor Tower 返回的是 UTF-16 LE 编码、Tab 分隔的 CSV/TSV 文件。
- 常见列包括 `Keyword`、`Rank (for App)`、`Rank (7-Day Change)`、`Traffic Score`、`Difficulty Score`、`Opportunity Score`、`KW Downloads (Absolute)`、`KW Downloads (% of Total)`、`KW Downloads (PoP % Growth)`、`App Count`、`Keyword Type`、`Paid Search State`。
- Excel / Numbers 一般可以直接打开；如果用脚本读取，注意指定 UTF-16。

Python 读取示例：

```python
import pandas as pd

df = pd.read_csv("./aso-keywords.csv", sep="\t", encoding="utf-16")
```

### 常见问题

如果 `--app-name` 匹配到多个 app，命令会返回候选摘要；这时改用更完整的 `--app-name`，或直接用 `--app-id`。

如果自动解析失败，先跑：

```bash
st aso-keywords user-apps --json
```

然后确认目标 app 是否已经在 Sensor Tower ASO Keywords 页面里创建过 view。如果不在列表里，CLI 不能凭空创建 ASO app view，只能操作当前账号已有的 view。

> 说明：这些接口与网页端一样依赖有效登录和 CSRF。若命令返回 403，请先重新执行 `st login --json` / `st status --json`。

## 竞品格局（`st landscape`）

准备一个竞品名单文件（每行：竞品名 + store URL，推荐 `TAB` 分隔）：

```text
QuickBooks	https://apps.apple.com/us/app/quickbooks-accounting/id584606479
Xero	https://apps.apple.com/us/app/xero-accounting/id422067011
Zoho Books	https://apps.apple.com/us/app/zoho-books-accounting/id1040049265
Wave	https://apps.apple.com/us/app/wave-accounting/id449637421
FreshBooks	https://apps.apple.com/us/app/freshbooks-invoice-tracking/id479925545
Sage	https://apps.apple.com/us/app/sage/id1470884689
```

然后运行（示例，`/path/to/competitors.txt` 换成你自己的文件路径；仓库里的 `competitors_batch1.txt` 只是示例文件）：

```bash
st landscape \
  --competitors-file /path/to/competitors.txt \
  --country US \
  --limit 6 \
  --json \
  --out report.md
```

- **`--country` / `--region`**：可选，指定收入、下载量、MAU 和市场份额的区域口径。可重复传，也可逗号分隔，例如 `--country US,JP,GB`；不传则使用默认全球区域列表（见“区域维度”）。
- 输出的核心列（报告与 JSON 都会包含）：
  - **`{YYYY-MM} Revenue`**：**上一个自然月**的收入（USD）
  - **`6M Growth`**：相对 **上一个自然月的 6 个月前** 的增长率（百分比）
  - **`First Release`**：首次发布时间（US）
  - **`Market share`**：市场份额（上一个自然月口径），两位小数百分比
  - **`Downloads`**：下载量（上一个自然月口径，absolute）
  - **`MAU`**：月活人数（上一个自然月口径，absolute）
  - **`competitors[].st.comments[]`**：Sensor Tower 用户评论样本（与 `st fetch` 同源时间窗口与条数上限）
  - **`competitors[].st.versions[]`**、**`competitors[].st.version_timeline`**：商店版本更新时间线，相对 JSON里 **`source.as_of`** 对应日期的 UTC 日末起 **近 30 天**（店面默认 **`US`**，与 `st snapshot` 一致）

- **`--limit`**：最多输出 N 个竞品（会对输入名单做解析/匹配，个别名称可能解析失败）
- **`--out`**：生成 Markdown 报告路径

### 数据与渲染解耦用法（推荐）

先抓数据（产物可复用）：

```bash
st landscape --competitors-file /path/to/competitors.txt --json > landscape.json
```

再单独渲染报告（不再请求 Sensor Tower）：

```bash
st landscape-report --in landscape.json --out report.md --json
```

也支持 stdin 管道：

```bash
st landscape --competitors-file /path/to/competitors.txt --json | st landscape-report --out report.md --json
```

### `st landscape` JSON 字段补充说明

`st landscape --json` 的 `data.competitors[].st` 中新增/常用字段：

- **`revenue_last_month_usd`**：上一个自然月收入（USD）
- **`revenue_trailing_12_months_usd`**：以上一个自然月为截止点的近 12 个月收入总和（USD）
- **`revenue_6_months_ago_usd`**：6 个月前对应月份的收入（USD）
- **`growth_vs_6m_percent`**：相对 6 个月前的增长率（百分比数值；无法计算时为 `null`）
- **`first_release_date_us`**：首次发布时间（来自 facets/v2）
- **`market_share_as_of_last_month.share_percent`**：市场份额（两位小数百分比）
- **`downloads_as_of_last_month.downloads_absolute`**：下载量（absolute）
- **`mau_as_of_last_month.mau_absolute`**：月活人数（absolute）

## 任意区间快照（`st snapshot`）

`st snapshot` 用于替代“上一个自然月”语义，按你传入的 `start_date` / `end_date` 直接取一个区间快照。

单 app raw 输出：

```bash
st snapshot "https://apps.apple.com/us/app/duolingo/id570060128" \
  --start-date 2026-01-01 \
  --end-date 2026-01-31 \
  --country US \
  --json
```

竞品文件，同时输出 raw 和 landscape 两种形态（`/path/to/competitors.txt` 换成你自己的文件路径）：

```bash
st snapshot \
  --competitors-file /path/to/competitors.txt \
  --start-date 2026-01-01 \
  --end-date 2026-01-31 \
  --country US \
  --shape both \
  --json
```

- **输入来源**：二选一，传 `QUERY` 或 `--competitors-file`
- **区域口径**：`--country` / `--region` 可选，可重复传或逗号分隔，例如 `--country US,JP`。它会影响 snapshot 里的收入、下载量、MAU、WAU，以及 `market_share_in_window` 的分母；不传则使用默认全球区域列表。
- **`--shape raw`**：返回 `data.raw.items[]`
- **`--shape landscape`**：返回 `data.landscape.competitors[]`
- **`--shape both`**：两种结构都返回，便于脚本和竞品分析共用

`st snapshot --json` 中的核心字段：

- **`source.start_date` / `source.end_date`**：本次快照的实际时间窗口
- **`raw.items[].snapshot.revenue_usd`**：窗口内收入（USD）
- **`raw.items[].snapshot.revenue_growth_vs_previous_window_percent`**：相对上一对齐窗口的收入增长率
- **`raw.items[].snapshot.downloads_absolute`**：窗口内下载量
- **`raw.items[].snapshot.downloads_growth_vs_previous_window_percent`**：相对上一对齐窗口的下载增长率
- **`raw.items[].snapshot.mau_absolute`**：窗口内 MAU 口径值
- **`raw.items[].snapshot.mau_growth_vs_previous_window_percent`**：相对上一对齐窗口的 MAU 增长率
- **`raw.items[].snapshot.wau_absolute`**：窗口内 WAU 口径值
- **`raw.items[].snapshot.wau_growth_vs_previous_window_percent`**：相对上一对齐窗口的 WAU 增长率
- **`raw.items[].market_share_in_window.share_percent`**：该窗口内的收入市场份额代理值
- **`raw.items[].comments[]`**：与快照窗口 `start_date`～`end_date` 对齐拉取的商店评论样本
- **`raw.items[].versions[]`**：商店版本更新时间线（与 `st version` 同源 API），按 `version_timeline` 中的 `max_age_days` 过滤；时间参考点为快照 **`end_date`** 当日 UTC 结束时刻（默认近 **30 天**、店面 **`US`**）
- **`raw.items[].version_timeline`**：`country`、`max_age_days`、`reference_end_date`（=快照 `end_date`）、`platform`（`ios` / `android` / `null`）
- **`landscape.competitors[].st.revenue_in_window_usd`**：竞品结构下的窗口收入
- **`landscape.competitors[].st.revenue_growth_vs_previous_window_percent`**：竞品结构下的窗口收入增长率
- **`landscape.competitors[].st.downloads_in_window.downloads_absolute`**：竞品结构下的窗口下载量
- **`landscape.competitors[].st.mau_in_window.mau_absolute`**：竞品结构下的窗口 MAU
- **`landscape.competitors[].st.wau_in_window.wau_absolute`**：竞品结构下的窗口 WAU
- **`landscape.competitors[].st.market_share_in_window.share_percent`**：竞品结构下的窗口市场份额代理值
- **`landscape.competitors[].st.reviews_in_window[]`**：竞品结构下的窗口评论样本
- **`landscape.competitors[].st.versions[]`** / **`version_timeline`**：与 raw 相同语义的版本更新时间线

其中 `market_share_in_window.share_percent` 是基于类目下 top-N apps 收入总和计算的 `top-N proxy market share`，不是全市场精确份额。

上一窗口的定义与当前窗口等长，紧邻当前窗口之前。例如：

- `2026-03-09 ~ 2026-03-22` 的 comparison window 会是 `2026-02-23 ~ 2026-03-08`

### `st snapshot` 数据与渲染解耦

先抓取快照数据：

```bash
st snapshot \
  --competitors-file /path/to/competitors.txt \
  --start-date 2026-03-09 \
  --end-date 2026-03-22 \
  --shape both \
  --json > snapshot.json
```

再单独渲染 Markdown 摘要：

```bash
st snapshot-report --in snapshot.json --out snapshot_report.md --json
```

也支持 stdin 管道：

```bash
st snapshot \
  --competitors-file /path/to/competitors.txt \
  --start-date 2026-03-09 \
  --end-date 2026-03-22 \
  --shape both \
  --json | st snapshot-report --out snapshot_report.md --json
```

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
- `0.2.1` 起会为 autocomplete/status 等 GET 入口自动附加 Sensor Tower 要求的 `x-csrf-token`。
- 若 ST 改版导致字段变化，需调整 `st_cli/st_api.py` / `st_cli/pipeline.py` 的解析逻辑。
