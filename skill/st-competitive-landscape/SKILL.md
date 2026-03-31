---
name: st-competitive-landscape
description: 用 st-cli + rdt-cli-research 输出，生成“市场规模 + 竞品优劣势 + AI 标签 + B2B/B2C”的竞争格局报告。触发词：竞争格局、竞品报告、SensorTower 竞品、landscape report
---

# st-competitive-landscape

## 你需要提供

- 二选一：
  - **方式 A（推荐）**：一份 `rdt-cli-research` 产出的 Markdown 报告路径（里面包含 **“二、竞品生态”** 表格）
    - 表头需要包含：`竞品 | 提及次数 | 正面 | 负面 | 核心评价`
  - **方式 B**：一串竞品名称（例如：`--name "Expensify" --name "QuickBooks"` 或 `--names-file competitors.txt`）

## 自动化步骤（由你执行命令）

1) 用 **全球口径** 抓取 Sensor Tower 数据（当前月 as-of，month facets）：

```bash
ST_FACET_REGIONS=global st landscape --rdt-report "<PATH_TO_RDT_REPORT>" --limit 12 --json > /tmp/st_landscape.json
```

或直接传竞品名：

```bash
ST_FACET_REGIONS=global st landscape --name "Expensify" --name "QuickBooks" --limit 12 --json > /tmp/st_landscape.json
```

或从文件读取（每行一个名字）：

```bash
ST_FACET_REGIONS=global st landscape --names-file competitors.txt --limit 12 --json > /tmp/st_landscape.json
```

2) 将 `/tmp/st_landscape.json` 的 `data.competitors` 作为证据集，让模型生成最终报告。

若使用一键 Markdown（`--out report.md`），`st landscape` 会基于 **rdt 核心评价 + ST 评论 sentiment/关键词** 自动生成每条竞品的 **Strengths / Weaknesses** 要点，并在疑似匹配错 app 时输出 `caveat`。

## 输出要求（模型生成报告时必须遵循）

- 报告目标：给出 **全球市场规模（Revenue）**，并按收入给竞品排序，逐个产出“优势/劣势/定位”。
- 时间口径：使用 `data.source.month` + `data.source.as_of`（当前月 as-of）。
- 每个竞品必须输出：
  - **Revenue（USD）**：`st.revenue_as_of_current_month_usd`
  - **Market share**：`st.market_share_as_of_current_month.share_percent`（如缺失需标注 unavailable）
  - **AI 标签**（必须给）：`AI-enabled` / `No AI features` / `AI-unclear`（三选一即可；可扩展更多）
  - **B2B/B2C**（必须给）：`B2B` / `B2C` / `Hybrid`
  - **优势/劣势**：基于 ST 评论片段（`st.comments`）的 sentiment/关键词进行总结；当可能存在 app 匹配不一致时，会输出 `App-match uncertainty` 之类的提示而不强行生成要点
- 任何无法从证据推导的结论，必须标注为“推断”并给出依据字段（不要胡编）。

## 判定指引（AI 标签 + B2B/B2C）

### AI 标签（优先级从高到低）

- 看到明确关键词：`AI`, `LLM`, `assistant`, `autopilot`, `智能`, `自动生成`, `自动分类`, `transcribe`, `summarize`, `chat` 等，并且与核心功能直接相关 → `AI-enabled`
- 如果核心评价/评论中明确表达“不涉及 AI / 只是传统工具” → `No AI features`
- 否则 → `AI-unclear`

### B2B/B2C

- **B2C**：个人用户订阅/个人工具（语言学习、密码管理、记账、健身等），评论多为个人使用场景
- **B2B**：公司/团队/企业流程（费用报销、发票、会计、审批、权限、合规、团队管理等）
- **Hybrid**：同时覆盖个人和企业（例如同时有个人版与企业团队版）

## 建议的报告结构（示例）

```markdown
# Competitive Landscape — {month} Mobile Revenue (Global, as-of {as_of})

## Market size
- **Total market revenue (proxy)**: ${sum_top_apps} (Top N proxy)
- **As-of window**: {month} (as-of {as_of})
- **Regions**: global preset list

## Leaders
（按 revenue 排序的竞品卡片列表）

### {Competitor}
- Revenue: $X
- Share: Y%
- AI label: ...
- Segment: B2B/B2C/Hybrid
- Strengths:
  - ...
- Weaknesses:
  - ...
- Notes (evidence):
  - rdt: "{core_review}"
  - st: sample reviews: "..."
```

