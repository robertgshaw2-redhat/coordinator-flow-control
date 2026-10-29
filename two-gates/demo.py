#!/usr/bin/env python3
"""Show which gate holds traffic under prefill-heavy and decode-heavy load.

Port-forwards the gateway and both EPPs' metrics, then runs two load phases
with ../load.py. During each phase it samples the flow-control saturation
gauges every few seconds. Afterwards it prints how many requests each EPP
dispatched and evicted in that phase.

  ./demo.py                      # namespace robshaw-dev, current KUBECONFIG
  ./demo.py --namespace other
"""
import argparse
import os
import re
import subprocess
import sys
import threading
import time
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
LOAD = os.path.join(HERE, "..", "load.py")

PHASES = [
    ("prefill-heavy", ["--workers", "64", "--prompt-words", "3000", "--max-tokens", "16", "--seconds", "45"]),
    ("decode-heavy", ["--workers", "48", "--prompt-words", "20", "--max-tokens", "1024", "--seconds", "60"]),
]
FORWARDS = [("svc/fc-envoy", "8081:8081"), ("svc/fc-epp-prefill", "19090:9090"), ("svc/fc-epp-decode", "19091:9090")]
P_EPP, D_EPP = "http://localhost:19090/metrics", "http://localhost:19091/metrics"


def scrape(url):
    with urllib.request.urlopen(url, timeout=5) as r:
        return r.read().decode()


def gauge(text, stage):
    m = re.search(r'^llm_d_epp_flow_control_pool_saturation\{[^}]*stage="%s"[^}]*\} (\S+)' % stage, text, re.M)
    return float(m.group(1)) if m else 0.0


def outcomes(text):
    counts = {}
    for m in re.finditer(r'^llm_d_epp_flow_control_request_queue_duration_seconds_count\{([^}]*)\} (\S+)', text, re.M):
        outcome = re.search(r'outcome="([^"]+)"', m.group(1)).group(1)
        counts[outcome] = counts.get(outcome, 0) + float(m.group(2))
    return counts


def sample(stop, rows, start):
    while not stop.wait(5):
        p, d = scrape(P_EPP), scrape(D_EPP)
        rows.append((time.time() - start, gauge(p, "prefill"), gauge(p, "decode"), gauge(p, "effective"),
                     gauge(d, "effective")))


def diff(after, before):
    return {k: int(after.get(k, 0) - before.get(k, 0)) for k in sorted(set(after) | set(before))
            if after.get(k, 0) != before.get(k, 0)}


def wait_idle():
    for _ in range(60):
        if gauge(scrape(P_EPP), "effective") == 0 and gauge(scrape(D_EPP), "effective") == 0:
            return
        time.sleep(5)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--namespace", default="robshaw-dev")
    args = ap.parse_args()

    procs = [subprocess.Popen(["kubectl", "-n", args.namespace, "port-forward", svc, ports],
                              stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL) for svc, ports in FORWARDS]
    try:
        time.sleep(4)
        for name, load_args in PHASES:
            wait_idle()
            print(f"\n=== {name}: {' '.join(load_args)}")
            p0, d0 = outcomes(scrape(P_EPP)), outcomes(scrape(D_EPP))
            rows, stop, start = [], threading.Event(), time.time()
            sampler = threading.Thread(target=sample, args=(stop, rows, start))
            sampler.start()
            client = subprocess.run([sys.executable, LOAD, *load_args], capture_output=True, text=True)
            stop.set()
            sampler.join()

            print("\nsaturation gauges (>= 1 means that gate is holding requests)")
            print(f"{'t':>5}  {'P-EPP: prefill':>14} {'P-EPP: decode':>14} {'P-EPP: gate':>12}  {'D-EPP: gate':>12}")
            for t, sp, sd, se, de in rows:
                print(f"{t:>4.0f}s  {sp:>14.2f} {sd:>14.2f} {se:>12.2f}  {de:>12.2f}")
            print(f"\nprefill EPP queue outcomes: {diff(outcomes(scrape(P_EPP)), p0)}")
            print(f"decode EPP queue outcomes:  {diff(outcomes(scrape(D_EPP)), d0)}")
            print("\nclient\n" + client.stdout.rstrip())
    finally:
        for p in procs:
            p.terminate()


if __name__ == "__main__":
    main()
