#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""挖「代购号 × 来源店铺」明细 —— 跑在 GitHub Actions。

抓 sitemap 指定区间里每家店铺的【评价页】，抽出评价条目；
只保留对方名字里带「ラクマ公認購入代行」的（=代购号），
于是每条 = 「某代购号 在 某家店 有一笔成交」——正是「货源」分析的原料。

环境变量：SHARD START END WORKERS OUT
用法: python3 mine.py
"""
import concurrent.futures as cf
import gzip
import json
import os
import re
import time
import urllib.request

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")
SHARD = int(os.environ.get("SHARD", "1"))
START = int(os.environ.get("START", "0"))
END = int(os.environ.get("END", "2000"))
WORKERS = int(os.environ.get("WORKERS", "16"))
OUT = os.environ.get("OUT", "entries.jsonl")

RE_BLOCK = re.compile(r'<article[^>]*class="[^"]*review-item[^"]*"[^>]*>(.*?)</article>', re.S)
RE_UID = re.compile(r'/user/(\d+)/')
RE_NAME = re.compile(r'review-item-name[^>]*>\s*([^<]{1,60}?)\s*<', re.S)
RE_DATE = re.compile(r'review-item-date[^>]*>\s*(\d{4}/\d{2}/\d{2})')
RE_TITLE = re.compile(r'review-item-title[^>]*>.*?</i>\s*([^<]{1,40})<', re.S)
RE_T = re.compile(r'<title[^>]*>([^<]*)</title>')
NEEDLE = "公認購入代行"


def shard_urls(n):
    u = f"https://static.fril.jp/sitemap/shop_{n}.xml.gz"
    req = urllib.request.Request(u, headers={"User-Agent": UA})
    raw = gzip.decompress(urllib.request.urlopen(req, timeout=120).read()).decode("utf-8", "ignore")
    return re.findall(r'<loc>https://fril\.jp/shop/([A-Za-z0-9_]+)</loc>', raw)


def fetch_review(h, tries=3):
    url = "https://fril.jp/shop/%s/review" % h
    for k in range(tries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept-Language": "ja"})
            with urllib.request.urlopen(req, timeout=25) as r:
                return r.read().decode("utf-8", "ignore")
        except Exception as e:
            time.sleep(20 if "429" in str(e) else 2)
    return None


def parse(html, shop, shopname):
    out = []
    for b in RE_BLOCK.findall(html):
        mu, mn, md = RE_UID.search(b), RE_NAME.search(b), RE_DATE.search(b)
        if not (mu and mn and md):
            continue
        name = re.sub(r"\s+", " ", mn.group(1)).strip()
        if NEEDLE not in name:
            continue                      # 只要对方是代购号的条目
        mt = RE_TITLE.search(b)
        out.append({"shop": shop, "shop_name": shopname, "uid": int(mu.group(1)), "name": name,
                    "date": md.group(1), "title": re.sub(r"\s+", " ", mt.group(1)).strip() if mt else ""})
    return out


def work(h):
    html = fetch_review(h)
    if not html:
        return []
    t = RE_T.search(html)
    shopname = re.sub(r"\s+", " ", (t.group(1) if t else "")).replace("の評価一覧｜ラクマ", "").strip()
    return parse(html, h, shopname)


def main():
    urls = shard_urls(SHARD)[START:END]
    print(f"第{SHARD}片 {START}-{END}：{len(urls)} 家店，并发 {WORKERS}", flush=True)
    n = tot = 0
    t0 = time.time()
    with open(OUT, "a", encoding="utf-8") as fh:
        with cf.ThreadPoolExecutor(WORKERS) as ex:
            for rows in ex.map(work, urls):
                n += 1
                for r in rows:
                    fh.write(json.dumps(r, ensure_ascii=False) + "\n")
                tot += len(rows)
                if n % 200 == 0:
                    fh.flush()
                    el = time.time() - t0
                    print(f"  {n}/{len(urls)} 家 · 代购条目 {tot} · {el:.0f}s · {n/max(el,1):.1f} 家/秒", flush=True)
    el = time.time() - t0
    print(f"完成：{n} 家 · 代购条目 {tot} · {el:.0f}s（{n/max(el,1):.1f} 家/秒）", flush=True)


if __name__ == "__main__":
    main()
