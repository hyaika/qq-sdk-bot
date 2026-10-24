#!/usr/bin/env python3
"""对比 MiMo 和 Qwen 的串子效果"""
import asyncio, os, sys
import openai

BASE_URL = "https://api.hcnsec.cn/v1"
API_KEY = "sk-EkYKEtQz2VG0HUEc0uWBdM92mgqgKZzSxfvRyApwNL12ye9Q"
HERE = os.path.dirname(os.path.abspath(__file__))
PROMPT = open(os.path.join(HERE, "prompt.md")).read()

MODELS = ["MiMo-V2.6-Flash", "Qwen3.8-Flash-Next"]

TESTS = [
    "原神真好玩",
    "我感觉鸣潮比原神强多了",
    "小米手机是真的好用",
    "今天老板夸我了",
    "你怎么看华为新出的车",
    "我错了",
]

client = openai.AsyncOpenAI(base_url=BASE_URL, api_key=API_KEY, timeout=90)


async def ask(model, text, sem):
    async with sem:
        try:
            r = await client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": PROMPT},
                    {"role": "user", "content": text},
                ],
                max_tokens=120,
                temperature=0.8,
            )
            c = r.choices[0].message.content
            return (c or "<None>").strip()
        except Exception as e:
            return f"<ERR {type(e).__name__}: {str(e)[:60]}>"


async def main():
    sem = asyncio.Semaphore(6)
    tasks = []
    for t in TESTS:
        for m in MODELS:
            tasks.append((t, m, asyncio.create_task(ask(m, t, sem))))
    rows = {}
    for t, m, task in tasks:
        rows.setdefault(t, {})[m] = await task
    for t in TESTS:
        print(f"\n{'='*70}\n输入: {t}")
        for m in MODELS:
            print(f"  [{m}] {rows[t][m]!r}")


asyncio.run(main())
