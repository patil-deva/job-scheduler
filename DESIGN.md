# Design

This scheduler stores every job in PostgreSQL and uses that database as the only queue. Java 21 with JDBC keeps the SQL visible. HikariCP is only the connection pool. There is no application framework and no Redis, RabbitMQ, SQS, Kafka, or other broker.

PostgreSQL fits because claiming work is a short transaction: pick due rows, lock them, and commit the lease before any handler runs. `FOR UPDATE SKIP LOCKED` lets several consumers take different rows at the same time. A row lock is the mutual exclusion. A separate lease is what makes a crash recoverable, because the lock disappears when the claim transaction ends.

## Schema

`jobs` is the queue. `scheduled_at` is the time the caller asked for and never changes. `earliest_run_at` starts equal to that and moves forward only for retry backoff, so a retry cannot hide an early execution. Status moves `PENDING` to `RUNNING` to `COMPLETED` or `FAILED`.

`job_attempts` is one row per claim. `job_effects` has primary key `job_id` and is inserted in the same transaction that marks the job completed. That is the demo's side effect. Callers that send email or call another service should use `job_id` as an idempotency key. A database cannot make an outside action exactly-once by itself; it can make the record of success exactly-once.

Partial indexes cover pending jobs by schedule and running jobs by lease expiry. At a few thousand rows the claim query is cheap either way. The indexes match the two access paths that matter as the table grows.

## One owner, no lost jobs, no double success

A consumer claims only as many jobs as it has free threads. Ranking and locking are two statements in one transaction, because PostgreSQL cannot put `FOR UPDATE` on a query that uses `ROW_NUMBER()`. The update locks the chosen ids with `SKIP LOCKED`, sets `RUNNING`, increments `attempts`, and stores a new `lease_token` and `lease_expires_at`. Two consumers cannot commit that update for the same row.

The handler runs after the claim commits. A heartbeat extends the lease with `WHERE lease_token = ?`. Completion, retry, and heartbeat all use that token. If a dead worker's lease is taken by someone else, the old worker's update matches zero rows and it does not write a second effect.

If the process is killed after the claim, the lease expires. The next claim treats an expired `RUNNING` row as due again, marks the open attempt `EXPIRED`, and starts a new attempt. The job is not deleted and not left `RUNNING` forever. A row that expires after `max_attempts` is marked `FAILED` instead of being claimed again, so a poison job cannot wedge the queue.

The demo's `long` job runs for 8 seconds with a 6 second lease. It finishes on attempt 1 only because heartbeats extend the lease. `--claim-and-exit` claims one `fast` or `slow` job and halts the process without a heartbeat. Another worker later completes it. The effect table still has one row.

A handler failure returns the job to `PENDING` with a short backoff when attempts remain, and the update is fenced by the lease token. The last attempt marks `FAILED` and writes no effect.

## Fairness and schedule lag

Each organization can have at most `MAX_INFLIGHT_PER_ORG` unexpired running jobs (3 in the demo), and each claim takes at most `MAX_PER_ORG` due jobs from one organization (2). The rank is `ROW_NUMBER() OVER (PARTITION BY organization_id ORDER BY earliest_run_at, id)`, and the batch is ordered by that rank first. The first job of every due organization is taken before anyone's second job.

The inflight count would race if two claimers read it at once, so the claim transaction takes `pg_advisory_xact_lock` for the few milliseconds it needs to rank and lock rows. Execution is still parallel: three processes and their worker threads run handlers at the same time. The lock only serializes the claim. `SKIP LOCKED` remains in the update so a row still cannot be given to two transactions if a claim ever runs without that advisory lock.

Jobs with `scheduled_at` in the future are not selected. Consumers poll every 250ms, so an idle due job waits at most about one poll plus the claim. Under load, the per-organization cap stops a tenant with a large backlog of slow jobs from filling every thread. The demo uses that shape on purpose: 30 two-second `northstar` jobs, a handful of fast `bright` and `cobalt` jobs, and six `cobalt` jobs scheduled four seconds ahead. `bright`'s third job starts before `northstar`'s tenth, and the delayed `cobalt` jobs start within a few seconds of their schedule instead of waiting for the whole backlog.

## Scaling

More consumer processes are the first step. They do not need a leader. Each one claims up to its free thread count. A bigger machine helps next, mostly by giving PostgreSQL more disk and CPU and by letting one process run more handler threads.

The ceiling is the primary. Every claim, heartbeat, and completion is a write. Read replicas cannot claim. A global advisory lock keeps the fairness cap exact and will serialize claims before the disk does. At higher throughput I would replace that with one advisory lock per organization, taken in a fixed order, so different tenants can be claimed in parallel.

For about 100 times the load I would also delete or archive `COMPLETED` rows so the live table stays small, partition what remains by day or by organization, and shorten heartbeats only for jobs whose handlers run longer than the lease. `LISTEN/NOTIFY` on insert would wake idle consumers without polling; a poll would stay as the fallback, because a missed notification and an expired lease still have to be found. Virtual threads could replace the fixed pool if handlers are mostly waiting, with the same inflight cap as the bound on concurrency.

## With more time

I would add a replay test that kills a process at random points during claim, heartbeat, and completion, and a metric for claim latency and queue lag per organization. I would also move retry backoff to a per-type policy. I would not add a broker. The hard part of this system is the lease and the fence, and those stay in the database either way.
