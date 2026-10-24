#!/usr/bin/env python3
"""复现心跳对'咕咕嘎嘎'的判定，测5次看稳定性"""
import openai, time, re, sys

src = open('/home/gueste89876/qq_ai_bot/qq_ai_bot.py').read()
m = re.search(r'API_KEY = "([^"]+)"', src)
key = m.group(1)
prompt = open('/home/gueste89876/qq_ai_bot/prompt.md').read()

client = openai.OpenAI(base_url='https://api.hcnsec.cn/v1', api_key=key, timeout=50)

hb = '''【群聊心跳任务】
这是 QQ 群「A90A197A...」最近的消息：
[19:29:00] FA2763B0: 咕咕嘎嘎

【你的任务】你是群里的活跃企鹅群友。判断上面有没有值得你插嘴的。
注意：简短的、无意义的、玩梗的消息（比如'咕咕嘎嘎''乐''6'）都要接梗，不要当成废话跳过。
只有在话题正在被别人热烈讨论、你插进去会显得突兀时，才不说话。

【输出格式】必须严格遵守，用 || 分隔：
- 不说话：只输出 [NO_REPLY]
- 说话：对象||气泡1||气泡2

例子：
通用||咕咕嘎嘎||（）
@小明||草||绷不住了

规则：第一个字段是对象（写「通用」或「@昵称」），后面 1-3 个字段是气泡，每条要短、像随手打字。不要重复你之前说过的话。'''

messages = [
    {'role':'system','content': prompt},
    {'role':'user','content': hb},
]

for i in range(5):
    t0 = time.time()
    r = client.chat.completions.create(
        model='sensenova-6.8-flash-lite',
        messages=messages,
        max_tokens=300,
        temperature=0.6,
    )
    c = (r.choices[0].message.content or '<None>').strip()
    dt = time.time()-t0
    # 模拟解析
    if c == '[NO_REPLY]':
        print(f'--- 第{i+1}次 ({dt:.1f}s) => NO_REPLY')
        continue
    parts = [p.strip() for p in c.split('||') if p.strip()]
    target = parts[0] if parts else '?'
    bubbles = parts[1:4] if len(parts)>1 else [c]
    print(f'--- 第{i+1}次 ({dt:.1f}s) => 对象={target} 气泡={len(bubbles)}')
    for b in bubbles: print(f'    气泡: {b}')
    print(f'    原始: {repr(c)}')
