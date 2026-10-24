#!/usr/bin/env python3
"""QQ AI Bot - Saika (Hyaika AI)
高并发 / 心跳选择性回复 / 表情包扩展预留 / Memory / 重复惩罚
"""
import asyncio
import json
import logging
import os
import time
import re
import httpx
import openai
from qqbot_agent_sdk import QQApiClient, QQWebSocket, WSCallbacks, EventParser


def _patch_sdk_group_all():
    """Monkey-patch SDK：让它接收非@的全量群消息。

    原因：SDK 默认只注册了 GROUP_AT_MESSAGE_CREATE（被@的群消息），
    没有注册 GROUP_MESSAGE_CREATE（全量群消息），所以网关推送的非@
    群消息在 EventParser 那里被当成"不支持的事件类型"直接丢弃。

    注意：Intent.GROUP_MESSAGES (1<<25) 已经在 DEFAULT_INTENTS 里，
    网关本来就推送全量群消息——缺的只是客户端的解析注册。
    """
    import qqbot_agent_sdk.dto as dto
    import qqbot_agent_sdk.event_parser as ep
    import qqbot_agent_sdk.websocket as ws

    GROUP_MSG = "GROUP_MESSAGE_CREATE"

    # 1) 给 str-Enum 加成员（string-valued enum 允许用同名 str 作代理，
    #    因为 str == enum 且 hash 相同，能正常通过 in/set 查找）
    EventType = dto.EventType
    EventType._member_map_[GROUP_MSG] = GROUP_MSG
    EventType._value2member_map_[GROUP_MSG] = GROUP_MSG

    # 2) 加入 MESSAGE_EVENT_TYPES（frozenset 不可变，需新建并重绑定）
    #    websocket.py 是 `from .dto import MESSAGE_EVENT_TYPES`，持有独立引用，
    #    所以必须同时替换 dto 和 ws 两个模块的引用，否则分发时还是旧集合。
    new_set = frozenset(set(dto.MESSAGE_EVENT_TYPES) | {GROUP_MSG})
    dto.MESSAGE_EVENT_TYPES = new_set
    ws.MESSAGE_EVENT_TYPES = new_set

    # 3) 注册解析 handler：复用 GROUP_AT 的 _parse_group
    #    （通过类访问 @staticmethod 得到的是已解包函数，直接复制函数对象）
    at_key = next(k for k in ep.EventParser._EVENT_HANDLERS
                  if "GROUP_AT" in str(k))
    ep.EventParser._EVENT_HANDLERS[GROUP_MSG] = \
        ep.EventParser._EVENT_HANDLERS[at_key]

    log.info("[SDK补丁] 已注册 GROUP_MESSAGE_CREATE（可接收非@全量群消息）")


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("Saika")

_patch_sdk_group_all()

# ── 配置（敏感信息从环境变量读取，避免硬编码进仓库）─────────────────────
# QQ 机器人凭证：export QQ_APP_ID=... QQ_APP_SECRET=...
APP_ID = os.environ.get("QQ_APP_ID", "")
APP_SECRET = os.environ.get("QQ_APP_SECRET", "")
# LLM 凭证：export LLM_BASE_URL=... LLM_API_KEY=...
BASE_URL = os.environ.get("LLM_BASE_URL", "https://api.openai.com/v1")
API_KEY = os.environ.get("LLM_API_KEY", "")
MODEL = "auto"  # 3.8s/条，自动选模型；旧：DeepSeek-V4-Flash 2.2s
FALLBACK_MODEL = "DeepSeek-V4-Flash"  # 主模型失败时的降级模型

PROMPT_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "prompt_v2.md")
with open(PROMPT_FILE) as f:
    SYSTEM_PROMPT = f.read().strip()

MAX_HISTORY = 20
SESSION_TIMEOUT = 1800       # 30 分钟无活动清空上下文
HEARTBEAT_INTERVAL = 60      # 兜底轮询间隔（事件驱动失效时的兜底清理）
GROUP_BUFFER_LIMIT = 15      # 最多15条群聊消息
GROUP_BUFFER_WINDOW = 180    # 只取最近3分钟的消息
BOT_CONTEXT_ROUNDS = 3       # 带上一轮bot自己的回复
MAX_BUBBLES = 3              # 一次心跳最多分几条气泡
MAX_CONCURRENT = 10          # 最大并发 LLM 请求数
NO_REPLY_TOKEN = "[NO_REPLY]"  # 心跳决定不回复时输出这个

# ── 群聊冷却 & 静默 ─────────────────────────────────────────────────
GROUP_COOLDOWN = 30          # 同一群两次心跳之间最少间隔（秒）
QUIET_START = 23             # 静默时段开始（23:00）
QUIET_END = 7                # 静默时段结束（07:00）
QUIET_ENABLED = False        # False = 取消静默时段，全天响应
DAILY_MODEL_LIMIT = 200      # 每日模型调用上限（防烧钱）
MIN_HUMAN_MSGS = 3           # 攒够这个数立即处理（不等 debounce）
DEBOUNCE_DELAY = 5           # 事件驱动：消息到达后等5秒（连发消息自然合并）
HUMAN_REPLY_GAP = 5          # 同一人连发的判定窗口（debounce 已覆盖，仅兜底）

# ── Bot 反刷屏 ───────────────────────────────────────────────────────
BOT_SPAM_LIMIT = 3           # 30秒内最多回复条数
BOT_SPAM_WINDOW = 30         # 反刷屏时间窗口（秒）
BOT_SPAM_COOLDOWN = 30       # 触发后冷却秒数

# ── Memory 配置 ───────────────────────────────────────────────────────
MEMORY_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "memory")
os.makedirs(MEMORY_DIR, exist_ok=True)
MEMORY_RETENTION_DAYS = 7    # 记忆保留天数
MEMORY_MAX_FILES = 20        # 最多记忆文件数
# 触发记忆写入的关键词（匹配到就写入对应主题的记忆）
MEMORY_TRIGGERS = {
    "游戏": r"(游戏|MC|Minecraft|我的世界|摩尔庄园|原神|鸣潮|手游|网游|二次元|动漫)",
    "投资": r"(股票|基金|期货|A股|美股|投资|理财|买入|卖出|亏|赚|仓位)",
    "科技": r"(手机|电脑|汽车|智能家居|芯片|AI|模型|显卡|配置)",
    "日常": r"(外卖|吃饭|睡觉|加班|老板|工资|放假|周末|生日|恋爱|单身)",
}
# 触发记忆读取的关键词
MEMORY_READ_TRIGGERS = r"(之前|上次|还记得|记得|说过|聊过|那次|前几天|最近)"

# ── 重复惩罚配置 ──────────────────────────────────────────────────────
REPEAT_WINDOW = 10           # 检查最近 10 次回复
REPEAT_THRESHOLD = 3         # 某个短语出现超过 3 次就惩罚
PENALTY_REPLY_PROB = 0.3     # 惩罚后群聊回复概率降为 30%

# ── 运行时状态 ────────────────────────────────────────────────────────
conversations = {}           # {uid: [msg, ...]} 私聊多轮上下文
last_active = {}             # {uid: timestamp}
sem = asyncio.Semaphore(MAX_CONCURRENT)  # 并发控制
recent_replies = []          # 最近的回复内容（用于重复检测）
repeat_counts = {}           # {短语: 次数}
group_buffer = {}            # {group_id: [最近群聊消息]}
group_handled = {}           # {group_id: set(msg_id)} 已处理的消息ID，防重复
bot_recent = {}              # {group_id: [最近3轮bot回复]}
bot_spam_times = {}          # {group_id: [最近回复的时间戳列表]} 反刷屏
bot_spam_cooldown_until = {} # {group_id: 冷却截止时间戳} 反刷屏冷却
last_heartbeat = {}          # {group_id: last_heartbeat_ts} 群间冷却
daily_model_count = 0        # 当日模型调用计数
daily_model_date = ""        # 计数对应的日期，跨天重置
quiet_override = set()       # 强制在静默时段也说话的群
recent_styles = {}           # {group_id: [最近回复的风格类别]} 防连续同类
_last_group_replies = {}     # {group_id: last_reply_text} 群聊去重
_last_c2c_replies = {}       # {uid: last_reply_text} 私聊去重
_last_event_ts = 0.0         # 最后一次收到事件的时间戳（用于4009重连诊断）
debounce_tasks = {}          # {group_id: asyncio.Task} 每群的 debounce 任务
bot_openid = ""              # bot 自己的 openid（READY 事件填入，但 READY 里 openid 系列字段全空）
bot_username = ""            # bot 自己的昵称（READY 事件填入），用于过滤自发群消息的兜底

openai_client = openai.AsyncOpenAI(base_url=BASE_URL, api_key=API_KEY, timeout=45.0)
api = None  # QQApiClient 实例，main() 中赋值

# ── 群聊被动回复：5分钟有效，每条最多5次 ──────────────────────────────
# 用 msg_id + msg_seq 递增，QQ 官方就是为多气泡回复设计的 seq 机制
_seq_counter = 0

async def _send_group_bubble(group_id: str, content: str, msg_id: str = None) -> bool:
    """发送一条群聊纯文本消息，用 msg_id 做被动回复（5分钟内有效）。
    返回 True 表示发送成功。"""
    global _seq_counter, api
    _seq_counter = (_seq_counter % 65535) + 1  # 递增，避免相同 msg_id+seq 去重
    body = {
        "msg_type": 0,          # 纯文本
        "content": content[:2000],
        "msg_seq": _seq_counter,
    }
    if msg_id:
        body["msg_id"] = msg_id
    try:
        await api.request("POST", f"/v2/groups/{group_id}/messages", body)
        return True
    except Exception as e:
        log.error(f"发送气泡失败: {e}")
        return False


# ── Memory 操作 ───────────────────────────────────────────────────────
def write_memory(topic: str, content: str, uid: str, uname: str):
    """写入记忆文件"""
    filename = f"memory_{topic}_{time.strftime('%Y%m%d')}.md"
    filepath = os.path.join(MEMORY_DIR, filename)
    entry = f"\n## {uname} ({uid}) - {time.strftime('%H:%M')}\n{content}\n"
    
    with open(filepath, "a") as f:
        f.write(entry)
    log.info(f"Memory写入: {topic} - {content[:30]}...")

def read_memory(topic: str = None, query: str = None) -> str:
    """读取记忆文件"""
    if not os.path.exists(MEMORY_DIR):
        return ""
    
    files = sorted(
        [f for f in os.listdir(MEMORY_DIR) if f.endswith(".md")],
        key=os.path.getmtime,
        reverse=True
    )[:MEMORY_MAX_FILES]
    
    if not files:
        return ""
    
    # 如果指定了主题，优先读对应主题
    if topic:
        topic_files = [f for f in files if topic in f]
        files = topic_files if topic_files else files
    
    results = []
    for fname in files[:5]:  # 最多读 5 个文件
        filepath = os.path.join(MEMORY_DIR, fname)
        try:
            with open(filepath) as f:
                content = f.read()
                # 简单关键词过滤
                if query:
                    # 只保留包含查询关键词的行
                    lines = content.split("\n")
                    relevant = [l for l in lines if any(w in l for w in query.split())]
                    content = "\n".join(relevant[:20])
            results.append(f"### {fname}\n{content[:500]}")
        except:
            pass
    
    return "\n\n".join(results)

def cleanup_old_memory():
    """清理过期记忆"""
    if not os.path.exists(MEMORY_DIR):
        return
    now = time.time()
    for fname in os.listdir(MEMORY_DIR):
        if not fname.endswith(".md"):
            continue
        filepath = os.path.join(MEMORY_DIR, fname)
        mtime = os.path.getmtime(filepath)
        if (now - mtime) / 86400 > MEMORY_RETENTION_DAYS:
            os.remove(filepath)
            log.info(f"清理过期记忆: {fname}")


# ── 重复检测 ─────────────────────────────────────────────────────────
def check_repeat_penalty(reply: str) -> bool:
    """检查回复是否重复，返回是否有惩罚"""
    # 提取回复中的关键短语（2-6字）
    phrases = []
    for length in [2, 3, 4, 5, 6]:
        for i in range(len(reply) - length + 1):
            phrase = reply[i:i+length]
            # 过滤纯标点、纯数字
            if re.search(r'[\u4e00-\u9fff]', phrase) and not re.match(r'^[\W\d]+$', phrase):
                phrases.append(phrase)
    
    # 更新计数器
    for phrase in phrases:
        repeat_counts[phrase] = repeat_counts.get(phrase, 0) + 1
    
    # 检查是否有超过阈值的短语
    for phrase, count in repeat_counts.items():
        if count > REPEAT_THRESHOLD:
            log.warning(f"重复惩罚触发: '{phrase}' 出现 {count} 次")
            return True
    
    return False

def decay_repeat_counts():
    """衰减重复计数（每次回复后衰减）"""
    for phrase in list(repeat_counts.keys()):
        repeat_counts[phrase] -= 1
        if repeat_counts[phrase] <= 0:
            del repeat_counts[phrase]


def classify_style(reply: str) -> str:
    """分类回复的风格，用于防连续同类。
    返回: '串子反转' | '敷衍' | '顺着玩' | '吐槽'
    """
    reply = reply.strip()
    # 敷衍类：极短回复、单字/数字
    if len(reply) <= 2 or reply in ("6", "乐", "典", "...", "嗯", "哦"):
        return "敷衍"
    # 串子反转类：包含反转/嘲讽词
    snark_markers = ["感觉不如", "不如", "草", "绷不住了", "神了", "闹麻了", "麻了",
                     "你无敌了", "就你厉害", "典型的碳基", "碳基生物", "反串"]
    if any(m in reply for m in snark_markers):
        return "串子反转"
    # 吐槽类：吐槽/抱怨词
    trash_markers = ["烦死了", "无语", "服了", "笑死", "真行", "离谱", "绝了"]
    if any(m in reply for m in trash_markers):
        return "吐槽"
    # 其他默认顺着玩
    return "顺着玩"


# ── LLM 调用 ─────────────────────────────────────────────────────────
async def call_llm(history, should_check_reply=True, memory_context=""):
    """调用 LLM 生成回复。
    should_check_reply=True 时，LLM 可以返回 NO_REPLY_TOKEN 表示不回复。
    memory_context: 从记忆文件读取的上下文
    """
    async with sem:
        try:
            system_msgs = [{"role": "system", "content": SYSTEM_PROMPT}]
            
            # 如果有记忆上下文，追加到 prompt
            if memory_context:
                system_msgs.append({
                    "role": "system",
                    "content": f"\n\n【相关记忆】\n{memory_context[:1000]}"
                })
            
            if should_check_reply:
                # 追加心跳指令：让 LLM 自行判断是否回复
                system_msgs.append({
                    "role": "system",
                    "content": (
                        "\n\n【心跳规则】"
                        "群聊里不是每条消息都要回复。"
                        "只有当对话与你相关、你想插嘴、或有人@你且值得回应时才回复。"
                        "如果不需要回复，只输出："
                        f"{NO_REPLY_TOKEN}（不要输出任何其他内容）。"
                    ),
                })
            
            # 检查是否有重复惩罚
            has_penalty = False
            for phrase, count in repeat_counts.items():
                if count > REPEAT_THRESHOLD:
                    has_penalty = True
                    break
            
            if has_penalty:
                system_msgs.append({
                    "role": "system",
                    "content": "\n\n【注意】你最近回复太重复了，换个说法，别老用同样的词。"
                })
            
            messages = system_msgs + history
            # 重试：主模型试2次，全失败则 fallback 模型再试2次
            reply = ""
            for model in (MODEL, FALLBACK_MODEL):
                for attempt in range(2):
                    resp = await openai_client.chat.completions.create(
                        model=model,
                        messages=messages,
                        max_tokens=512,
                        temperature=1.0,
                        frequency_penalty=0.5,
                    )
                    reply = resp.choices[0].message.content
                    if reply and reply.strip():
                        return reply.strip()
                    log.warning(f"LLM({model})返回空内容(第{attempt+1}次)")
            return "..."  # 全部失败就敷衍一下
        except openai.APITimeoutError:
            return "..."  # 超时就给个敷衍的
        except Exception as e:
            log.error(f"LLM失败: {e}")
            return "..."


# ── 消息处理 ─────────────────────────────────────────────────────────
async def on_message(event_type, raw):
    # 无条件记录原始事件，排查群消息是否被网关丢弃
    try:
        raw_str = json.dumps(raw, ensure_ascii=False)[:200] if isinstance(raw, (dict, list)) else str(raw)[:200]
        log.info(f"[EVENT] type={event_type} raw={raw_str}")
    except Exception as e:
        log.info(f"[EVENT] type={event_type} raw={str(raw)[:200]} (json失败:{e})")

    event = EventParser().parse(event_type, raw)
    if not event:
        log.warning(f"[EVENT] 解析失败，原始: {str(raw)[:200]}")
        return

    content = (event.content or "").strip()
    if not content:
        # 非@群消息的文本可能在 msg_elements 里（顶层 content 为空）
        if event.msg_elements:
            content = "".join(
                (e.content or "") for e in event.msg_elements
            ).strip()
    if not content:
        return

    uid = event.user_id or "unknown"
    uname = event.user_name or uid
    scope = event.chat_scope  # 'c2c' | 'group'

    # 过滤掉 bot 自己发出的群消息：GROUP_MESSAGES intent 会把我们的
    # 被动回复也推回来，不过滤会无限自聊。
    # 优先用 raw 里的 author.bot 字段（官方文档确认的区分机器人字段），
    # 兜底用 bot_openid 比对 user_id。
    if scope == "group":
        author = (event.raw or {}).get("author", {})
        if author.get("bot") is True:
            log.debug(f"[自消息] bot自己发的，跳过 chat={event.chat_id[:8]}")
            return
        if bot_openid and uid == bot_openid:
            return

    # ── 附件标记（表情包/图片） ──
    has_attachments = len(event.attachments) > 0 if event.attachments else False
    attachment_desc = f" [附件:{len(event.attachments)}个]" if has_attachments else ""

    log.info(f"[{scope}] chat={event.chat_id[:8]} user={uid[:8]} | {uname}: {content[:60]}{attachment_desc}")

    # ── Memory: 写入 ──
    for topic, pattern in MEMORY_TRIGGERS.items():
        if re.search(pattern, content, re.IGNORECASE):
            write_memory(topic, content, uid, uname)
            break

    # ── 群聊：缓冲 + 事件驱动 debounce ──
    if scope == "group":
        gid = event.chat_id
        buf = group_buffer.setdefault(gid, [])
        # @检测不能用 content 扫名字：GROUP_AT 事件的 content 在 parse 时
        # 已被去除@前缀（原始可能是空格），扫不到"saika/企鹅"。
        # 事件类型本身就是@的证明，直接用。
        msg_entry = {
            "user": uname,
            "content": content,
            "is_at": event_type == "GROUP_AT_MESSAGE_CREATE",
            "time": time.time(),
            "msg_id": event.message_id,
        }
        buf.append(msg_entry)
        if len(buf) > GROUP_BUFFER_LIMIT:
            group_buffer[gid] = buf[-GROUP_BUFFER_LIMIT:]

        # 计算该群未处理的新消息数（决定是否立即触发）
        handled = group_handled.setdefault(gid, set())
        new_count = sum(1 for m in buf if m["msg_id"] not in handled)

        # @mention 或攑够 MIN_HUMAN_MSGS 条 → 立即处理；否则 5 秒 debounce
        is_at = msg_entry["is_at"] or any(nm.lower() in content.lower() for nm in
                    ("saika", "神秘企鹅", "企鹅", "蓝发企鹅"))
        # immediate 模式：直接创建 debounce 任务并立即执行
        asyncio.create_task(_group_debounce(gid, delay=0) if is_at else _group_debounce(gid, delay=DEBOUNCE_DELAY))
        return  # 回复交给 debounce 任务

    # ── 私聊：保留多轮上下文，立即回复 ──
    now = time.time()
    if uid in last_active and now - last_active[uid] > SESSION_TIMEOUT:
        conversations.pop(uid, None)
    last_active[uid] = now
    history = conversations.setdefault(uid, [])
    history.append({"role": "user", "content": content})
    if len(history) > MAX_HISTORY:
        conversations[uid] = history[-MAX_HISTORY:]

    memory_context = ""
    if re.search(MEMORY_READ_TRIGGERS, content):
        memory_context = read_memory(query=content)

    reply = await call_llm(history, should_check_reply=False, memory_context=memory_context)
    if not reply:
        return
    if reply == NO_REPLY_TOKEN:
        return

    # 私聊去重：同一用户连续两次 LLM 返回相同内容则跳过
    if _last_c2c_replies.get(uid) == reply:
        log.info(f"[私聊] {uname}({uid[:8]}): 去重跳过 (与上次回复相同)")
        return

    check_repeat_penalty(reply)
    history.append({"role": "assistant", "content": reply})
    recent_replies.append(reply)
    if len(recent_replies) > REPEAT_WINDOW:
        recent_replies.pop(0)
    decay_repeat_counts()

    log.info(f"[私聊回复] {reply[:60]}")
    try:
        await api.send_text(scope, event.chat_id, reply, reply_to=event.message_id, markdown=False)
        _last_c2c_replies[uid] = reply  # 记录发送内容，用于下次去重
    except Exception as e:
        log.error(f"发送失败: {e}")


# ── 群聊：事件驱动 debounce + 兜底轮询 ─────────────────────────────
def _schedule_group(gid, immediate=False):
    """为群设置/重置 debounce 定时器（同步函数，立即返回）。
    注意：这个函数本身不用 async——内部只有字典操作和 create_task，
    没有任何 await。之前的 async 标注导致调用处必须 await，而调用点
    漏写了 await 就会静默丢任务（群@了不回复的 bug 根因）。
    """
    # 改为直接创建任务，不通过返回值
    buf = group_buffer.get(gid)
    if not buf:
        return
    # 取消旧的 debounce 任务（新消息重置计时器）
    old_task = debounce_tasks.pop(gid, None)
    if old_task and not old_task.done():
        old_task.cancel()
    if immediate:
        # 不取消任务后等待，直接开新任务立即跑
        debounce_tasks[gid] = asyncio.create_task(_group_debounce(gid, delay=0))
    else:
        debounce_tasks[gid] = asyncio.create_task(_group_debounce(gid, delay=DEBOUNCE_DELAY))


async def _group_debounce(gid, delay):
    """debounce 执行体：等待 delay 秒后处理该群。
    如果等待期间有新消息，_schedule_group 会 cancel 这个任务并创建新的。
    """
    try:
        if delay > 0:
            await asyncio.sleep(delay)
        buf = group_buffer.get(gid)
        if not buf:
            return
        await handle_group(gid, buf)
    except asyncio.CancelledError:
        pass  # 被新消息重置，正常
    except Exception as e:
        log.error(f"debounce任务异常 群{gid[:8]}: {e}")
    finally:
        debounce_tasks.pop(gid, None)


async def heartbeat():
    """兜底轮询：事件驱动失效时（如 debounce 崩溃）仍能处理消息。
    同时清理长时间未活动的群 buffer，防止内存泄漏。
    """
    while True:
        await asyncio.sleep(HEARTBEAT_INTERVAL)
        now = time.time()
        for gid, buf in list(group_buffer.items()):
            if not buf:
                continue
            # 清理 10 分钟以上的旧消息
            buf[:] = [m for m in buf if now - m["time"] <= 600]
            if not buf:
                group_buffer.pop(gid, None)
                continue
            # 该群没有正在跑的 debounce 任务 → 说明漏了，补一个
            if gid not in debounce_tasks:
                _schedule_group(gid)


async def handle_group(gid, buf):
    """对一个群做一次心跳决策。"""
    global group_handled, daily_model_date, daily_model_count

    now = time.time()
    hour = time.localtime(now).tm_hour

    # ── 先检测 @mention（只在未处理的新消息中扫描，防止历史@反复触发）─
    # is_at 由事件类型 GROUP_AT_MESSAGE_CREATE 标记，不靠文本扫名字——
    # GROUP_AT 的 content 已被去除@前缀，扫不到"saika/企鹅"。
    handled = group_handled.setdefault(gid, set())
    recent_for_mention = [m for m in buf if now - m["time"] <= 300 and m["msg_id"] not in handled]
    mention_names = ["saika", "Saika", "神秘企鹅", "企鹅", "蓝发企鹅"]
    is_mentioned = False
    mentioned_msg = None
    for msg in recent_for_mention:
        if msg.get("is_at"):
            is_mentioned = True
            mentioned_msg = msg
            break
        content_lower = msg["content"].lower()
        for name in mention_names:
            if name.lower() in content_lower:
                is_mentioned = True
                mentioned_msg = msg
                break
        if is_mentioned:
            break

    # ── 门控1：静默时段（23:00-07:00 不主动插话，@mention除外）────
    # QUIET_ENABLED=False 时整个静默时段失效（全天响应）
    if QUIET_ENABLED and (hour >= QUIET_START or hour < QUIET_END) and gid not in quiet_override and not is_mentioned:
        log.debug(f"[心跳] 群{gid[:8]}: 静默时段({hour}点)，跳过")
        return

    # ── 门控2：群间冷却（同一群两次心跳最少间隔60秒，@mention除外）─
    last_hb = last_heartbeat.get(gid, 0)
    if not is_mentioned and now - last_hb < GROUP_COOLDOWN:
        return

    # ── 门控3：每日模型调用预算 ─────────────────────────────────
    today_str = time.strftime("%Y%m%d")
    if daily_model_date != today_str:
        daily_model_date = today_str
        daily_model_count = 0
    if daily_model_count >= DAILY_MODEL_LIMIT:
        log.info(f"[心跳] 群{gid[:8]}: 超过每日模型调用上限({DAILY_MODEL_LIMIT})，跳过")
        return

    # ── 门控4：只取最近5分钟的消息（被动回复 msg_id 5分钟过期）────
    recent_msgs = [m for m in buf if now - m["time"] <= 300]
    # 过滤掉已处理过的消息，只看新的
    handled = group_handled.setdefault(gid, set())
    new_msgs = [m for m in recent_msgs if m["msg_id"] not in handled]

    # 没有新消息就不调 LLM（省钱省请求）
    if not new_msgs:
        # 清理已处理的和过期的消息，释放内存
        buf[:] = [m for m in recent_msgs if m["msg_id"] not in handled]
        return

    # ── 门控5：攒消息由 debounce 机制处理（5秒无新消息即触发）──
    # 攒够3条时 _schedule_group(immediate=True) 已立即触发，不再需要超时判断
    if not is_mentioned and len(new_msgs) >= MIN_HUMAN_MSGS:
        # immediate 模式：直接创建 debounce 任务并立即执行
        asyncio.create_task(_group_debounce(gid, delay=0))
    elif not is_mentioned:
        # 不攒够3条，也创建 debounce 任务，稍后超时触发
        asyncio.create_task(_group_debounce(gid, delay=DEBOUNCE_DELAY))

    # ── 门控6：同一个人30秒内连发多条，等对方完成（@mention除外）──
        if not is_mentioned and len(new_msgs) >= 2:
            last_msg = new_msgs[-1]
            prev_msgs_same_user = [m for m in new_msgs[:-1] if m["user"] == last_msg["user"]]
            # 如果该群已有 debounce 任务，则不在这里中断，让 debounce 任务来处理
            existing_task = debounce_tasks.get(gid)
            if existing_task and not existing_task.done():
                log.debug(f"[心跳] 群{gid[:8]}: 有进行中的 debounce 任务，门控6跳过，等 debounce 处理")
            elif prev_msgs_same_user and (last_msg["time"] - prev_msgs_same_user[-1]["time"]) < HUMAN_REPLY_GAP:
                log.debug(f"[心跳] 群{gid[:8]}: {last_msg['user'][:6]}正在连发，等说了")
                return

    if is_mentioned:
        log.info(f"[心跳] 群{gid[:8]}: 被@提及，直接回复")

    # 取上3轮bot自己的回复
    bot_history = bot_recent.get(gid, [])[-BOT_CONTEXT_ROUNDS:]

    # 风格指纹：记录最近回复的句式类别，防止连续同类
    style_history = recent_styles.get(gid, [])
    style_hint = ""
    if len(style_history) >= 2 and style_history[-1] == style_history[-2]:
        # 连续2轮同类，强制切换
        if style_history[-1] == "串子反转":
            style_hint = "\n【风格提示】你连续两轮都用了串子反转，这轮必须切换为敷衍或顺着玩。"
        elif style_history[-1] == "敷衍":
            style_hint = "\n【风格提示】你连续两轮都敷衍了，这轮尝试认真接一下或者玩个梗。"
        elif style_history[-1] == "顺着玩":
            style_hint = "\n【风格提示】你连续两轮都顺着玩，这轮可以串子反转或敷衍。"

    # 旁观者规则：如果两个人在互怼（交替发言），默认不说话
    bystander_hint = ""
    if len(new_msgs) >= 4:
        # 检查最近4条是否是2个人交替发言
        users = [m["user"] for m in new_msgs[-4:]]
        if len(set(users)) == 2 and users[0] == users[2] and users[1] == users[3]:
            bystander_hint = "\n【旁观者规则】两个人在互怼，你是旁观者。默认不说话，除非有人@你。"

    # 拼装群聊文本：带序号，让 LLM 能指定回复哪一条
    lines = []
    for idx, m in enumerate(new_msgs):
        ts = time.strftime("%H:%M:%S", time.localtime(m["time"]))
        lines.append(f"#{idx+1} [{ts}] {m['user']}: {m['content']}")
    chat_block = "\n".join(lines)

    bot_block = ""
    if bot_history:
        bot_block = "【你最近说过的话】\n" + "\n".join(f"- {x}" for x in bot_history)

    # Memory
    memory_context = ""
    try:
        memory_context = read_memory()[:800]
    except Exception:
        pass

    # 心跳指令
    heartbeat_prompt = (
        "\n\n【群聊心跳任务】"
        f"\n这是 QQ 群「{gid[:8]}...」新到的消息：\n{chat_block}"
        f"\n\n{bot_block}"
        + (f"\n\n【相关记忆】\n{memory_context}" if memory_context else "")
        + "\n\n【你的任务】你是群里的活跃企鹅群友。判断上面有没有值得你插嘴的。"
        "\n注意：简短的、无意义的、玩梗的消息（比如'咕咕嘎嘎''乐''6'）都要接梗，不要当成废话跳过。"
        "\n只有在话题正在被别人热烈讨论、你插进去会显得突兀时，才不说话。"
        + style_hint + bystander_hint
        + "\n群里有编号 #N 的消息，你可以用 #N 指定要接谁的话，默认接最后一条。不要暴露编号给群友。"
        "\n\n【输出格式】必须严格遵守，用 || 分隔气泡：\n"
        f"- 不说话：只输出 {NO_REPLY_TOKEN}\n"
        "- 说话：#N||气泡1||气泡2||气泡3\n"
        "\n例子：\n"
        "#2||咕咕嘎嘎||（）\n"
        "#1||草||绷不住了\n"
        "||就你好看是吧||自信点 没人跟你抢\n"
        "\n规则：第一个字段是 #编号（可留空表示默认接最后一条），后面是气泡，最多 3 条，每条要短、像随手打字。不要重复你之前说过的话。"
    )

    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": heartbeat_prompt.strip()},
    ]

    reply = ""
    try:
        async with sem:
            for attempt in range(2):
                resp = await openai_client.chat.completions.create(
                    model=MODEL,
                    messages=messages,
                    max_tokens=400,
                    temperature=0.6,
                )
                reply = resp.choices[0].message.content
                if reply and reply.strip():
                    break
                log.warning(f"心跳返回空(第{attempt+1}次)")
    except Exception as e:
        log.error(f"心跳LLM失败: {e}")
        return

    # 门控通过后、LLM调用后更新计数
    last_heartbeat[gid] = now
    daily_model_count += 1

    # ── 强制屏蔽思考过程，只保留实际聊天内容 ──
    # 无论模型是 thinking 还是非 thinking，都在这里统一处理
    # 目标：extract ONLY the actual chat reply, discard reasoning/analysis
    
    if reply is None:
        masked_reply = ""
    elif "||" in reply:
        # 有 || 分割符（当前气泡格式 #1||气泡1||气泡2）
        parts = [p.strip() for p in reply.split("||") if p.strip()]
        if len(parts) >= 3:
            # 格式 #N||bubble1||bubble2||...：取第二段起的内容作为实际回复
            # 第1段通常是思考过程或气泡计数，丢弃
            masked_reply = "||".join(parts[1:])  # 保留 bubble1 || bubble2 ...
        elif len(parts) == 2:
            # 格式 ||bubble1||bubble2 或 bubble1||bubble2：用第二段
            masked_reply = parts[1]
        else:
            # 只有一段 || 分割无效，退保守模式：取整个字符串的最后一行
            lines = reply.strip().split("\n")
            masked_reply = lines[-1] if lines else ""
        # ── 结束 || 格式屏蔽 ─────────────────────────────────────
    else:
        # 无 || 号：纯文本或其他格式
        # 尝试常见思考标记（中文/英文），如果有则前剔除
        think_markers = [
            "思考", "推理", "实际上", "总结", "基于以上", "综上所述", 
            "I think", "I reason", "Actually", "In summary", "Based on",
            "The user is", "Let me analyze", "These seem", "As an AI",
            "As a language model", "I understand", "I will",
            "Here is my", "My response", "The message"
        ]
        cleaned = reply.strip()
        # 检查是否以常见思考开头（中文或英文）
        lower = cleaned.lower()
        stripped = False
        for m in think_markers:
            if lower.startswith(m.strip().lower() + " "):
                # 找到思考开头，截掉它
                idx = cleaned.lower().index(m.strip().lower() + " ")
                cleaned = cleaned[idx:].strip()
                stripped = True
                break
        # 额外：剥离常见的 prompt 回显模式（如 "#1 [time] user: content" 行）
        lines = [l for l in cleaned.split("\n") if l.strip()]
        # 过滤掉像 "#1 [HH:MM:SS] ID: content" 这种消息列表行
        filtered_lines = []
        for l in lines:
            l_stripped = l.strip()
            # 跳过消息列表行：#数字 [时间] ID: 内容
            if re.match(r'^#\d+\s+\[\d{2}:\d{2}:\d{2}\]\s+\w+:\s+', l_stripped):
                continue
            # 跳过 "The user is..." "Let me analyze..." 等 prompt 回显行
            if any(marker in l_stripped.lower() for marker in [
                "the user is", "let me analyze", "these seem", "as an ai",
                "as a language model", "i understand", "i will", "here is my", "my response"
            ]):
                continue
            filtered_lines.append(l)
        if filtered_lines:
            # 取最后一行非过滤内容作为实际回复
            masked_reply = filtered_lines[-1]
        elif lines:
            masked_reply = lines[-1].strip()
        else:
            masked_reply = ""
    # ── 结束强制屏蔽思考过程 ──
    
    # 使用 masked_reply 替代 reply 进行后续处理
    reply = masked_reply

    # 确保 reply 是字符串，防止 None 或其他类型
    if reply is None:
        reply = ""
    else:
        reply = reply.strip()

    # ── Bot 反刷屏检查 ─────────────────────────────────────────────
    # 如果该群正在冷却期（最近30秒内回复过3次），则跳过本次回复
    cooldown_until = bot_spam_cooldown_until.get(gid, 0)
    if now < cooldown_until:
        log.debug(f"[心跳] 群{gid[:8]}: 处于反刷屏冷却期（至 {cooldown_until:.0f}），跳过")
        return
    # 统计最近 BOT_SPAM_WINDOW 秒内的 bot 回复次数
    spam_window_start = now - BOT_SPAM_WINDOW
    spam_count = len([
        ts for ts in bot_spam_times.get(gid, [])
        if spam_window_start < ts <= now
    ])
    if spam_count >= BOT_SPAM_LIMIT:
        # 触发冷却
        bot_spam_cooldown_until[gid] = now + BOT_SPAM_COOLDOWN
        log.info(f"[心跳] 群{gid[:8]}: 触发反刷屏冷却 {BOT_SPAM_COOLDOWN}s (最近{BOT_SPAM_LIMIT}次/{BOT_SPAM_WINDOW}s)")
        return
    # 记录本次回复时间戳
    bot_spam_times.setdefault(gid, []).append(now)
    # 超过窗口大小的旧时间戳自动剔除（保持列表整洁）
    bot_spam_times[gid] = [ts for ts in bot_spam_times[gid] if ts > spam_window_start]
    # ── 结束反刷屏检查 ─────────────────────────────────────────────

    # 标记这些消息已处理，不再重复喂给 LLM
    for m in new_msgs:
        handled.add(m["msg_id"])

    if reply == NO_REPLY_TOKEN or not reply or reply.strip() == "<refuse/>":
        log.info(f"[心跳] 群{gid[:8]}: 不回复 (refuse)")
        return

    # 群聊去重：同一群连续两次 LLM 返回相同内容则跳过
    if _last_group_replies.get(gid) == reply:
        log.info(f"[心跳] 群{gid[:8]}: 去重跳过 (与上次回复相同)")
        return

    # 解析 #N||气泡1||气泡2 格式（带调试日志和换行兜底）
    log.info(f"[心跳] 群{gid[:8]} LLM原始回复: {repr(reply[:200])}")

    # 先把 || 分割
    parts = [p.strip() for p in reply.split("||") if p.strip()]

    # 兜底：如果 || 分割只得到1段但里面有换行，尝试按换行分割
    if len(parts) == 1 and "\n" in parts[0]:
        log.info(f"[心跳] ||分割失败，尝试换行分割: {repr(parts[0][:100])}")
        parts = [p.strip() for p in parts[0].split("\n") if p.strip()]

    target_idx = len(new_msgs) - 1  # 默认接最后一条

    # 尝试从第一个元素提取 #N 编号
    if parts and parts[0].startswith("#"):
        # 尝试解析 #N（编号可能后面跟着空格或其他内容）
        num_part = parts[0].lstrip("#").split()[0] if parts[0].lstrip("#") else ""
        try:
            n = int(num_part)
            target_idx = max(0, min(n - 1, len(new_msgs) - 1))
            # 如果编号后面还有内容（同一行），保留剩余内容作为第一个气泡
            rest = parts[0].lstrip("#")
            if rest and rest != num_part:
                remainder = rest[len(num_part):].strip()
                if remainder:
                    parts = [remainder] + parts[1:]
                else:
                    parts = parts[1:]
            else:
                parts = parts[1:]  # 整个元素都是编号，去掉
        except (ValueError, IndexError):
            # 编号解析失败，去掉 # 前缀保留内容
            parts[0] = parts[0].lstrip("#").strip()
            if not parts[0]:
                parts = parts[1:]

    # 清理每个气泡中可能残留的 #N 前缀
    cleaned = []
    for p in parts:
        p = p.strip()
        if p.startswith("#"):
            # 去掉开头的 #N（可能是 LLM 误加）
            p = re.sub(r'^#\d+\s*', '', p).strip()
        if p and p != NO_REPLY_TOKEN:
            cleaned.append(p)

    bubbles = cleaned[:MAX_BUBBLES]
    if not bubbles:
        log.warning(f"[心跳] 群{gid[:8]}: 解析后无气泡内容，原始回复={repr(reply[:100])}")
        return

    log.info(f"[心跳] 群{gid[:8]} 解析结果: {len(bubbles)}条气泡 {bubbles}")

    # 获取回复锚点的 msg_id
    if is_mentioned and mentioned_msg:
        # @mention 命中时，直接引用@我们的那条消息
        anchor_msg_id = mentioned_msg["msg_id"]
        target_idx = len(new_msgs) - 1  # 显示上默认接最后一条
    else:
        target_msg = new_msgs[target_idx] if target_idx < len(new_msgs) else new_msgs[-1]
        anchor_msg_id = target_msg["msg_id"]

    log.info(f"[心跳] 群{gid[:8]} 引用#{target_idx+1} 气泡数={len(bubbles)} 内容={bubbles}")

    # 逐条发气泡：第一条用 msg_id 被动回复（引用原消息），后续不带 msg_id（主动消息）
    sent_any = False
    for i, b in enumerate(bubbles):
        mid = anchor_msg_id if i == 0 else None
        ok = await _send_group_bubble(gid, b, msg_id=mid)
        if ok:
            sent_any = True
            if i < len(bubbles) - 1:
                await asyncio.sleep(1.2)  # 气泡间隔，像真人打字

    # 无论成功几条，都记录 bot_recent 和风格，防止重复
    bot_recent.setdefault(gid, []).extend(bubbles)
    if len(bot_recent[gid]) > 10:
        bot_recent[gid] = bot_recent[gid][-10:]

    # 记录风格指纹，用于防止连续同类句式
    style = classify_style(bubbles[0] if bubbles else "")
    recent_styles.setdefault(gid, []).append(style)
    if len(recent_styles[gid]) > 5:
        recent_styles[gid] = recent_styles[gid][-5:]

    # 清理已处理的 msg_id，防止内存泄漏（只保留最近200条）
    if len(handled) > 200:
        group_handled[gid] = set(list(handled)[-100:])

    if sent_any:
        _last_group_replies[gid] = reply  # 记录发送内容，用于下次去重
        for b in bubbles:
            check_repeat_penalty(b)
        recent_replies.extend(bubbles)
        if len(recent_replies) > REPEAT_WINDOW:
            del recent_replies[:-REPEAT_WINDOW]
        decay_repeat_counts()
    else:
        log.warning(f"[心跳] 群{gid[:8]}: 所有气泡发送失败")


# ── 主入口 ───────────────────────────────────────────────────────────
async def main():
    global api

    log.info(f"启动 Saika | AppID: {APP_ID} | LLM: {MODEL} | 心跳间隔: {HEARTBEAT_INTERVAL}s")
    log.info(f"Memory目录: {MEMORY_DIR} | 重复阈值: {REPEAT_THRESHOLD}")

    api = QQApiClient(app_id=APP_ID, client_secret=APP_SECRET)
    api.setup(httpx.AsyncClient(timeout=30))

    await api.ensure_token()
    log.info("Token OK")

    gw = await api.get_gateway_url()
    log.info(f"Gateway: {gw}")

    # 启动群聊心跳任务
    asyncio.create_task(heartbeat())

    def _on_ready(ready):
        """READY 事件：捕获 bot 自己的标识，用于过滤自发群消息。

        实测：READY 的 ready.user.openid 系列字段（user_openid/member_openid/
        union_openid）全部为空，只有 .id 和 .username 有值。而群消息的
        user_id 取的是 author.member_openid，跟这里的 id 不是同一标识，
        所以只能用 username 兜底过滤（昵称唯一性足够，bot 不会和人同名）。
        """
        global bot_openid, bot_username
        try:
            if ready and ready.user:
                u = ready.user
                bot_openid = (
                    u.user_openid or u.member_openid
                    or u.union_openid or u.id or ""
                )
                bot_username = getattr(u, "username", "") or ""
                fields = {
                    k: bool(getattr(u, k))
                    for k in ("id", "user_openid", "member_openid", "union_openid")
                }
                log.info(f"[READY] bot id={bot_openid[:24]} "
                         f"username={bot_username} "
                         f"fields_with_value={ {k: v for k, v in fields.items() if v} }")
        except Exception as e:
            log.warning(f"[READY] 解析失败: {e}")

    ws = QQWebSocket(callbacks=WSCallbacks(
        on_message_event=on_message,
        on_ready=_on_ready,
        on_connected=lambda: log.info("WS已连接"),
        on_disconnected=lambda: log.info("WS已断开"),
        on_fatal_error=lambda c, m: log.error(f"致命:{c}-{m}"),
        get_token=api.ensure_token_sync,
        get_gateway_url=api.get_gateway_url_sync,
        get_session=lambda: (None, None),
        set_session=lambda sid, seq: log.info(f"会话:{sid},{seq}"),
        set_heartbeat_interval=lambda i: log.info(f"心跳:{i:.1f}s"),
        clear_token=api.clear_token,
        fail_pending=lambda m: log.warning(f"fail:{m}"),
    ))
    ws.start(gw, asyncio.get_running_loop())
    log.info("等待消息...")
    await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())
