# Architecture

## Lifecycle flow

From HTTP submission, through the queue and the worker pool, to the result store, with the retry and
dead-letter paths.

```mermaid
flowchart TD
    client([Client])

    subgraph apitier ["API tier: stateless, scales on request load"]
        api["REST API<br/>validate, INSERT, return 202 + job id"]
    end

    subgraph pg ["Postgres: queue, state and result store"]
        jobs[("jobs table<br/>queued / running / failed /<br/>succeeded / dead_lettered / cancelled")]
        dlq[("dead_letters table")]
    end

    subgraph workertier ["Worker tier: stateless, scales on queue depth"]
        disp["Dispatcher<br/>claim due jobs: priority DESC, run_at<br/>FOR UPDATE SKIP LOCKED + lease"]
        handler["Pluggable handler<br/>runs under per-attempt timeout"]
        reaper["Reaper<br/>finds expired leases"]
    end

    client -->|"POST /jobs"| api
    client -->|"GET /jobs/id, /queue/depth<br/>DELETE /jobs/id, POST /queue/drain"| api
    api -->|"1. INSERT state=queued"| jobs
    api -.->|"status, result, depth, cancel"| jobs

    jobs -->|"2. claim: state=running, attempts+1"| disp
    disp --> handler
    handler -->|"3a. success: state=succeeded + result"| jobs
    handler -->|"3b. error or timeout, retries left:<br/>state=failed, run_at = now + backoff"| jobs
    handler -->|"3c. retries exhausted or non-retryable:<br/>state=dead_lettered"| dlq

    reaper -->|"worker died, retries left:<br/>state=failed, redeliver"| jobs
    reaper -->|"worker died, retries exhausted"| dlq
    dlq -.->|"POST /dead-letters/id/requeue"| jobs
```

Step 3c and both reaper paths update the job row and insert the dead-letter row in a single SQL
statement, so a job is never marked dead without a dead-letter record, or the reverse.

## Job state machine

```mermaid
stateDiagram-v2
    [*] --> queued: POST /jobs (202)
    queued --> running: worker claims (attempt 1)
    running --> succeeded: handler returned
    running --> failed: error / timeout / lease expired, retries left
    failed --> running: back-off elapsed, worker claims (attempt n+1)
    running --> dead_lettered: retries exhausted or non-retryable
    dead_lettered --> queued: operator requeue
    queued --> cancelled: cancel / drain
    failed --> cancelled: cancel / drain
    running --> queued: graceful shutdown releases the job
    succeeded --> [*]
    cancelled --> [*]
```

`failed` is not terminal: it means "the last attempt failed and a retry is scheduled". The terminal
states are `succeeded`, `dead_lettered` and `cancelled`.

## Why one store

The queue, the in-flight state and the results live in one Postgres table, so every transition is one
atomic statement. There is no second system to keep consistent, which removes the dual-write problem a
separate broker introduces (job row committed but message never published, or the reverse).

The trade-off is throughput: all claims go through one Postgres primary. See "Handling high load" in the
README for the limits and for the broker-plus-outbox design that comes next.
