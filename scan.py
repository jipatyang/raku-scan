#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""楽天ラクマ「公認購入代行」店铺扫描 —— 跑在 GitHub Actions 上。

每次运行都是一台全新机器、全新出口 IP，所以可以同时开多个任务并行，
每个任务有独立的请求额度，不会被彼此的限流拖累。

环境变量（由 workflow 传入）：
  SHARD START END   要扫的区间（第几片 sitemap 的第几家到第几家）
  WORKERS           并发数
  REPORT_SRC        设了就把进度和命中的账号实时播报回本地看板
用法: python3 scan.py
"""
import concurrent.futures as cf
import gzip
import json
import os
import re
import threading
import time
import urllib.request

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/151.0.0.0 Safari/537.36")
NEEDLE = "ラクマ公認購入代行"
META = re.compile(r'ラクマ公認購入代行\s*(.{1,30}?)のショップページです')
TITLE = re.compile(r'<title[^>]*>([^<]*)</title>')
UIDRE = re.compile(r'img\.fril\.jp/user/(\d+)/s/\d+\.(?:png|jpg|jpeg)\?(\d+)')

SHARD = int(os.environ.get("SHARD", "26"))
START = int(os.environ.get("START", "0"))
END = int(os.environ.get("END", "200"))
WORKERS = int(os.environ.get("WORKERS", "16"))
REPORT_SRC = os.environ.get("REPORT_SRC", "")
TOPIC = os.environ.get("REPORT_TOPIC", "raku-scan-9f3c7a52")
HITS = "hits.jsonl"

STATE = {"shard": SHARD, "start": START, "end": END,
         "done": 0, "total": END - START, "hits": 0, "rate": 0.0}


def post(obj):
    """把一条消息播报出去（供本地看板实时读取）"""
    if not REPORT_SRC:
        return
    for _ in range(2):
        try:
            urllib.request.urlopen("https://ntfy.sh/" + TOPIC,
                                   data=json.dumps(obj, ensure_ascii=False).encode(),
                                   timeout=10).read()
            return
        except Exception:
            time.sleep(1)


def reporter():
    while True:
        post(dict({"src": REPORT_SRC, "ts": int(time.time())}, **STATE))
        time.sleep(15)


def shard_urls(n):
    """第 n 片 sitemap 的店铺哈希列表（每片 5 万家）"""
    u = f"https://static.fril.jp/sitemap/shop_{n}.xml.gz"
    req = urllib.request.Request(u, headers={"User-Agent": UA})
    raw = gzip.decompress(urllib.request.urlopen(req, timeout=120).read()).decode("utf-8", "ignore")
    return re.findall(r'<loc>https://fril\.jp/shop/([A-Za-z0-9_]+)</loc>', raw)


def probe(h):
    """抓一家店铺页；是公認購入代行号就返回记录，否则 None"""
    html = None
    for _ in range(3):
        try:
            req = urllib.request.Request("https://fril.jp/shop/" + h,
                                         headers={"User-Agent": UA, "Accept-Language": "ja"})
            with urllib.request.urlopen(req, timeout=25) as r:
                html = r.read().decode("utf-8", "ignore")
            break
        except Exception as e:
            time.sleep(25 if "429" in str(e) else 2)
    if not html or NEEDLE not in html[:8000]:
        return None
    m = META.search(html)
    if not m:
        return None
    t, u = TITLE.search(html), UIDRE.search(html)
    return {"hash": h, "title": (t.group(1) if t else "")[:70],
            "nickname": "ラクマ公認購入代行" + m.group(1).strip(), "kind": "account",
            "uid": int(u.group(1)) if u else None,
            "avatar_ts": int(u.group(2)) if u else None,
            "source": "GitHub Actions"}


def main():
    print(f"第{SHARD}片 {START}-{END}  并发 {WORKERS}", flush=True)
    if REPORT_SRC:
        threading.Thread(target=reporter, daemon=True).start()
        print(f"已启动进度广播（来源={REPORT_SRC}）", flush=True)
    urls = shard_urls(SHARD)[START:END]
    print(f"取到 {len(urls)} 家店铺", flush=True)
    n = hits = 0
    t0 = time.time()
    with open(HITS, "a", encoding="utf-8") as fh:
        with cf.ThreadPoolExecutor(WORKERS) as ex:
            for r in ex.map(probe, urls):
                n += 1
                el = time.time() - t0
                STATE.update(done=n, hits=hits, rate=round(n / el, 1) if el else 0.0)
                if r:
                    hits += 1
                    STATE["hits"] = hits
                    print("★", json.dumps(r, ensure_ascii=False), flush=True)
                    fh.write(json.dumps(r, ensure_ascii=False) + "\n")
                    fh.flush()
                    post({"src": REPORT_SRC, "hit": r, "ts": int(time.time())})
                if n % 500 == 0:
                    print(f"  {n}/{len(urls)} 命中={hits} {el:.0f}s {n/el:.1f} 家/秒", flush=True)
    el = time.time() - t0
    print(f"完成：{n} 家，命中 {hits}，用时 {el:.0f}s（{n/el:.1f} 家/秒）", flush=True)


if __name__ == "__main__":
    main()
