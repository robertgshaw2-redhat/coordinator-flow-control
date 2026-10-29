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

The flow-control rule is in [`config/epp-prefill.yaml`](config/epp-prefill.yaml):

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

## Knobs

- `defaultRequestTTL`: the N. `0s` disables eviction, so requests wait until the
  client disconnects. You can override it per priority band through
  `priorityBands[].defaultRequestTTL`.
- `noEndpointRequestTTL`: a separate budget for an empty pool, for example while
  scaling up from zero. That case returns a 503. It defaults to
  `defaultRequestTTL`.
- `maxConcurrency`: deliberately low so the demo queues. Calibrate it from the
  prefill knee of the model you deploy.
