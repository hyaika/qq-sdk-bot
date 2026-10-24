# QQ机器人 (QQ Bot) —— Saika

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.8+](https://img.shields.io/badge/Python-3.8+-blue.svg)](https://www.python.org/downloads/release/python-380/)

## 🤖 项目简介

基于 **QQBot-Agent-SDK** 开发的功能齐全的QQ群机器人，支持群心跳回复、@提及检测、去重防刷屏、思考内容隐藏等核心功能。

## ✨ 核心特性

| 功能 | 说明 |
|------|------|
| **思考内容屏蔽** | 无论模型是 thinking 还是非 thinking，都在代码层面强制提取实际聊天内容，丢弃推理过程，防止思考过程泄露 |
| **反刷屏机制** | 30秒内最多3次回复，触发后冷却30秒，防止在群里疯狂刷屏 |
| **多气泡发送** | 第1条使用被动回复（`msg_id`），后续使用主动消息，规避 QQ 官方 API 限制 |
| **@检测优化** | 使用事件类型 `GROUP_AT_MESSAGE_CREATE` 而非文本扫描，更可靠 |
| **静默时段控制** | 通过 `QUIET_ENABLED=False` 关闭 23:00-07:00 的静默时段（全天响应） |
| **去重逻辑** | 群聊/私聊双向去重，防止连续重复回复 |
| **LLM 降级** | 主模型失败自动回退到 `FALLBACK_MODEL`，失败后给出友好提示 |
| **高频词配额** | 设置 10 个高频词每 30 轮最多出现 1 次，防止模型复读 |
| **正向表述** | prompt 采用正向引导而非大量 "禁止"，提升回复质量 |
| **群友档案** | 记录群友常用语、档案，提升人格化回复 |

## 🛠️ 技术栈

- **语言**：Python 3.8+
- **框架**：QQBot-Agent-SDK
- **模型**：DeepSeek V4/Flasher（ via OpenAI 兼容 API ）
- **配置**：通过 `qq_ai_bot.py` 顶部常量配置，无需编程基础即可修改参数

## 📁 项目结构

```
qq_ai_bot/
├── qq_ai_bot.py          # 主程序入口（核心逻辑、心跳、门控）
├── prompt_v2.md          # 重写的提示词（3779字符，无陈旧参考例）
├── OPTIMIZATION.md       # 169 行优化清单（P0-P2 问题）
├── README.md             # 项目说明（当前文件）
├── bot.log              # 运行日志（已脱敏，已提交前删除敏感信息）
├── memory/               # 记忆缓存目录
└── .hermes/              # Hermes Agent 配置
```

## 🚀 快速开始

### 1. 环境准备
```bash
# 克隆项目
git clone git@github.com:your-username/qq-bot.git
cd qq_ai_bot

# 安装依赖
pip install -r requirements.txt  # (如有 requirements 文件)
```

### 2. 配置修改
编辑 `qq_ai_bot.py` 顶部配置区域：
```python
# 模型配置
MODEL = "auto"                # 主模型(auto自动选择或deepseek-v4-flash)
FALLBACK_MODEL = "DeepSeek-V4-Flash"  # 失败时的降级模型

# 门控配置
QUIET_ENABLED = False         # 关闭静默时段(23:00-07:00)，全天响应
BOT_SPAM_LIMIT = 3            # 30秒内最多回复3次
BOT_SPAM_WINDOW = 30          # 反刷屏统计窗口(秒)
BOT_SPAM_COOLDOWN = 30        # 触发后冷却(秒)

# 其他配置见文件顶部注释
```

### 3. 运行 bot
```bash
python3 qq_ai_bot.py
```

## 🔧 主要修复历史

| 问题 | 解决方案 |
|------|----------|
| Thinking 模型思考过程泄露 | 在 `handle_group` 中统一屏蔽，只保留实际聊天内容 |
| 多气泡第二条 400错误 | 第1条用 `msg_id` 被动回复，后续不带 `msg_id` 主动发送 |
| RuntimeWarning（协程未 await） | `_schedule_group` 改为同步函数，直接 `asyncio.create_task` |
| 静默时段拦消息 | `QUIET_ENABLED=False`，全天响应 |
| @检测失效 | 改用事件类型 `GROUP_AT_MESSAGE_CREATE` 判断 |
| LLM 复读 | 删除 prompt 里的 17 条陈旧参考例，设高频词配额 + temperature 1.0 |
| 门控6连发 | 同一人30秒内连发自动等待，防止对方接不住 |
| 私聊/群聊去重 | `_last_group_replies` + `_last_c2c_replies` 防止重复 |

## 📜 许可证

MIT License - 免费用于个人和商业用途。

## 📧 联系方式

- 项目主页：https://github.com/your-username/qq-bot
- 问题反馈：请在 GitHub Issues 中提交

---

> **重要**：提交 GitHub 之前请确保 `bot.log` 中已删除所有敏感信息（API Key、用户ID、Access Token 等），当前项目已在 README 中注明需自行清理日志。