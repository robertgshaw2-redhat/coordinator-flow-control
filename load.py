#!/usr/bin/env python3
"""Overload the prefill pool and show what flow control did with each request.

Runs closed-loop workers against the gateway, each sending a long, unique
prompt (no prefix-cache hits, so every request pays a full prefill). Prints how
many requests completed vs were evicted, and how long each kind waited.

  kubectl port-forward svc/fc-envoy 8081:8081 &
  ./load.py --workers 64 --seconds 60
"""
import argparse
import json
import random
import string
import threading
import time
import urllib.error
import urllib.request
from collections import defaultdict


def prompt(words):
    return " ".join("".join(random.choices(string.ascii_lowercase, k=6)) for _ in range(words))


def worker(args, deadline, results, lock):
    while time.time() < deadline:
        body = json.dumps({
            "model": args.model,
            "prompt": prompt(args.prompt_words),
            "max_tokens": args.max_tokens,
        }).encode()
        req = urllib.request.Request(f"{args.url}/v1/completions", data=body,
                                     headers={"Content-Type": "application/json"})
        start = time.time()
        try:
            with urllib.request.urlopen(req, timeout=300) as resp:
                resp.read()
                code = resp.status
        except urllib.error.HTTPError as e:
            code = e.code
            e.read()
        except Exception as e:  # noqa: BLE001
            code = type(e).__name__
        with lock:
            results[code].append(time.time() - start)


def pct(xs, p):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(p * len(xs)))]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default="http://localhost:8081")
    ap.add_argument("--model", default="Qwen/Qwen3-8B")
    ap.add_argument("--workers", type=int, default=64)
    ap.add_argument("--seconds", type=int, default=60)
    ap.add_argument("--prompt-words", type=int, default=3000)
    ap.add_argument("--max-tokens", type=int, default=16)
    args = ap.parse_args()

    results, lock = defaultdict(list), threading.Lock()
    deadline = time.time() + args.seconds
    threads = [threading.Thread(target=worker, args=(args, deadline, results, lock))
               for _ in range(args.workers)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    print(f"{'status':>8} {'count':>6} {'p50 s':>7} {'p99 s':>7} {'max s':>7}")
    for code, lat in sorted(results.items(), key=lambda kv: str(kv[0])):
        print(f"{code!s:>8} {len(lat):>6} {pct(lat, .5):>7.2f} {pct(lat, .99):>7.2f} {max(lat):>7.2f}")


if __name__ == "__main__":
    main()
