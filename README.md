# Coordinator P/D with flow control: queue for up to N seconds, then evict

A minimal llm-d coordinator deployment with prefill/decode disaggregation and
one flow-control rule: when prefill is saturated, a request waits in the queue
for up to `defaultRequestTTL` (10s here). If it is still waiting after that, it
is evicted and the client gets a `429`.

```
client ──> envoy ──(no x-llm-d-epp-profile)──> coordinator
                                                  │
           envoy <──(x-llm-d-epp-profile: prefill)┤  prefill EPP: flow control, TTL 10s ──> prefill vLLM
           envoy <──(x-llm-d-epp-profile: decode)─┘  decode EPP: no flow control ─────────> decode vLLM
```

The flow-control rule is in [`base/config/epp-prefill.yaml`](base/config/epp-prefill.yaml):

```yaml
featureGates:
  - flowControl
flowControl:
  defaultRequestTTL: 10s              # queue for up to N seconds, then evict (429)
  saturationDetector:
    pluginRef: prefill-concurrency    # "saturated" = maxConcurrency prefills in flight per pod
```

## Why two EPPs

The coordinator calls the gateway once per phase, and each call goes through an
EPP on its own. With a single EPP, flow control would see the decode call too.
That call could then be queued or evicted after its prefill had already run,
which wastes the prefill and leaves its KV stranded on the prefill pod. Using a
role-scoped EPP per phase means admission happens once, at prefill, before any
GPU work. Once a request passes prefill, its decode is dispatched immediately.
This follows the 3-EPP coordinator topology in llm-d-router, minus encode.

## Timeouts that must stay above the TTL

| Where | Setting | Why |
|---|---|---|
| coordinator | `gateway.timeout` (60s) | A queued prefill returns no response headers until it leaves the queue. If this is shorter than the TTL, the coordinator times out first and the client gets a 502 instead of the 429. |
| envoy | ext_proc `message_timeout` (1000s) | A queued request keeps its ext_proc message open while it waits. |
| coordinator | `server.write_timeout` (300s) | Covers the whole client response: the queue wait plus prefill plus decode. |

## Run it

Tested on kermit in `robshaw-dev`. It uses 2 H200s: Qwen3-8B, with one prefill
pod and one decode pod, and NIXL over IB. The images are
`llm-d-router-{endpoint-picker,coordinator}:v1.0.0`.

```sh
export KUBECONFIG=~/.kube/kermit.yaml
kubectl apply -k . -n robshaw-dev
kubectl -n robshaw-dev wait --for=condition=Ready pod -l app=fc-vllm --timeout=10m
kubectl -n robshaw-dev port-forward svc/fc-envoy 8081:8081 &
./load.py --workers 64 --seconds 60
```

Clean up:

```sh
kubectl delete -k . -n robshaw-dev
```

## Result (2026-10-28)

The load was 64 closed-loop workers, each sending a unique prompt of about 9k
tokens. One prefill takes about 0.5s. `maxConcurrency: 2` admits roughly 4
prefills/s, far less than the offered load.

These timings come from the Envoy access log, measured end to end at the
gateway:

| outcome | count | p50 | min | max |
|---|---|---|---|---|
| 200 (admitted, served) | 242 | 10.7s | 0.02s | 11.5s |
| 429 `rejected-ttl-expired` | 135 | 10.8s | 10.0s | 10.9s |

- Every eviction happened between 10.0s and 10.9s: the 10s TTL plus at most one
  expiry sweep.
- The EPP's `llm_d_epp_flow_control_request_queue_duration_seconds` metric
  agrees: 242 `Dispatched` and 135 `EvictedTTL`.
- All 242 decode calls returned 200. None was queued or rejected.
- Measured from the laptop, the client's maximum latency was about 17s. The
  extra time over the gateway figures is `kubectl port-forward` overhead, not
  queueing.

## Variant: gates on both EPPs ([`two-gates/`](two-gates/))

- **Prefill requests get two checks.** They queue when prefill **or** decode is
  saturated, and each check has its own threshold.
- **Decode requests get one check.** They queue when decode is saturated.

| EPP | Check | Signal | Threshold | Queue TTL |
|---|---|---|---|---|
| prefill | prefill saturated | this EPP's in-flight prefills (`concurrency-detector`) | 2 per prefill pod | 10s |
| prefill | decode saturated | decode pods' scraped vLLM queue (`utilization-detector`) | 4 waiting per decode pod | (same queue) |
| decode | decode saturated | this EPP's in-flight decodes (`concurrency-detector`) | 14 per decode pod | 30s |

How it works:

- **The prefill EPP's InferencePool selects both roles.** Flow control splits
  the pool by `llm-d.ai/role` and computes a saturation for each stage. A
  `max-saturation-detector` with `stages:` assigns one detector per stage, and
  flow control gates on `max(S_prefill, S_decode)`.
- **The prefill EPP has to read decode load from the pods' own metrics.**
  Decode calls go through the decode EPP, so the prefill EPP's in-flight count
  on decode pods is always 0.
- **The decode EPP is the backstop.** It counts decode load exactly. Its
  threshold is looser than the prefill EPP's decode check, so new work is held
  at prefill first, before any GPU time is spent. Its TTL is longer because
  evicting there throws away a finished prefill.

The overlay also caps decode at `--max-num-seqs 8`. That is for the demo only,
so that decode saturates under modest load.

```sh
kubectl apply -k two-gates -n robshaw-dev
./two-gates/demo.py
```

### Result (2026-10-28, kermit)

**Prefill-heavy phase.** 64 workers sent prompts of about 9k tokens with
`max_tokens: 16`.

| t | P-EPP prefill | P-EPP decode | P-EPP gate | D-EPP gate |
|---|---|---|---|---|
| 12–55s | 1.00 | 0.00–0.01 | 1.00 | 0.00–0.07 |

- The prefill check held the queue. The prefill EPP dispatched 171 requests and
  evicted 108 on TTL.
- The decode EPP dispatched all 171 calls without queueing.

**Decode-heavy phase.** 48 workers sent 20-word prompts with
`max_tokens: 1024`.

| t | P-EPP prefill | P-EPP decode | P-EPP gate | D-EPP gate |
|---|---|---|---|---|
| 12–66s | 0.00 | 1.25–1.50 | 1.25–1.50 | 0.93–1.00 |

- Prefill was idle. The prefill EPP's decode check held the queue: it
  dispatched 93 requests and evicted 126 on TTL.
- The decode EPP sat at its limit, about 8 running and 6 waiting. It queued 31
  decode calls for 5–10s each, because the scraped decode signal lags. It
  evicted none.

## Knobs

- `defaultRequestTTL`: the N. `0s` disables eviction, so requests wait until the
  client disconnects. You can override it per priority band through
  `priorityBands[].defaultRequestTTL`.
- `noEndpointRequestTTL`: a separate budget for an empty pool, for example while
  scaling up from zero. That case returns a 503. It defaults to
  `defaultRequestTTL`.
- `maxConcurrency`: deliberately low so the demo queues. Calibrate it from the
  prefill knee of the model you deploy.
