# System Design

How the pipeline is put together, and why.

---

## 1. The shape of it

```
  event_generator.py --> Redpanda ---> PyFlink ----> Redis ------> FastAPI
      (producer)       (event log)   (aggregation)   (view)      (stateless)
                            |                          ^
                            |                          |
                            +--------------------------+
                        replay from offset 0 rebuilds the view exactly
```

Redpanda holds every event, forever (until retention expires). Flink reads that
stream and keeps a running total per station. Redis stores those totals. The API
just reads Redis and returns it.

If Redis ever gets wiped, nothing is lost — restart Flink from the beginning of
the topic and it rebuilds the same numbers.

---

## 2. Aggregate on write, not on read

Every event updates a small running total in Flink, and that total is pushed to
Redis right away. So when a request comes in, the answer is already sitting
there — the API does one Redis read, no math, no scanning through events.

The alternative — storing raw events and adding them up per request — would get
slower and slower as more events come in, since every request would have to
re-scan more data. Updating one number per event, once, is cheaper and stays
fast no matter how much data has flowed through.


---

## 3. Why Flink (and not a plain Python service)

A plain Python service doing `for event in stream: totals[station] += 1` works
right up until something goes wrong — the process dies, a pod restarts, events
arrive out of order. Then you're stuck writing your own checkpointing,
recovery, and ordering logic by hand. Flink exists because it already solved
that class of problem:

- **Fault tolerance.** Flink periodically takes a checkpoint — a consistent
  snapshot of both its internal state (the running totals) and its position in
  the Redpanda topic (the offset), written to durable storage. If a Flink
  worker crashes, it restarts from the last checkpoint and replays only the
  events since then, so no counts are lost or double-counted.
- **Out-of-order events.** Events don't always arrive in the order they
  happened (network delays, retries, partition skew). Flink handles this with
  **event-time processing and watermarks** — it tracks progress by the
  timestamp on the event itself, not by arrival order, and uses watermarks to
  decide how long to wait for late data before finalizing a window.

In short: a Python service can express "add one to a counter" just as easily.
What it can't give you for free is the guarantee that the counter stays
correct across crashes, restarts, and out-of-order data — that's the part
Flink is actually buying.

---

## 4. What would change in production


- **A cold path for raw events.** Right now, once Redis has the running totals,
  the individual events aren't kept anywhere else queryable. Redis can only
  answer "what's the total right now" — not "what happened last month." In
  production I'd add a second consumer that writes every raw event to cheap
  storage (like S3), so the full history is available for reporting and
  analysis, not just the live counters.
- **A schema registry.** Right now the event format (field names, types) is
  just assumed — nothing enforces it. If the producer changed a field name or
  type, Flink would fail or silently miscount, and nobody would know until the
  numbers looked wrong. A schema registry (e.g. with Avro) would catch
  incompatible changes before they reach the pipeline, and let the event format
  evolve safely over time.
- **Monitoring and alerts.** Right now nobody gets notified if Flink falls
  behind or crashes. In production I'd add basic monitoring — is the pipeline
  keeping up, are there errors, is anything stuck.
- **A more durable state store.** Flink currently keeps its running totals in
  memory. For a larger dataset, that state should be saved to disk (RocksDB)
  so it can grow beyond available RAM.
