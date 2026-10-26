# Job scheduler

A database-backed scheduler for jobs that belong to many organizations. PostgreSQL is the queue and the lock manager. Several Java processes claim due jobs, run them on worker threads, and finish them exactly once.

There is no Redis, RabbitMQ, SQS, Kafka, or other broker. The design notes are in [DESIGN.md](DESIGN.md).

## What you need

- Java 21
- Maven 3.9 or newer
- Docker, with the Compose plugin

PostgreSQL 16 is started by Docker Compose. It listens on `localhost:5432` with database, user, and password all set to `scheduler`.

## Run the demo

From this directory:

```powershell
.\demo\run-demo.ps1
```

On macOS or Linux:

```bash
./demo/run-demo.sh
```

The script starts Postgres, builds `target/job-scheduler.jar`, and enqueues 55 jobs for three organizations:

- `northstar`: 30 slow jobs (about 2 seconds each), due now
- `bright`: 10 fast jobs, 1 long job (8 seconds), and 1 job that fails once then succeeds
- `cobalt`: 6 fast jobs due now, 6 fast jobs due in 4 seconds, and 1 job that fails until its 2 attempts are used up

It then starts three worker processes plus a fourth process that claims one fast or slow job and exits immediately. That abandoned job is the crash. The script waits until the queue is empty and prints a report. `Result: PASS` means every check below held. Worker logs are written to `demo/logs/`.

The demo shortens the lease to 6 seconds and the heartbeat to 2 seconds. The long job runs for 8 seconds, so it only finishes on the first attempt if heartbeats keep the lease alive. A normal install uses a 15 second lease.

## What the report is checking

- No job is lost. Every job ends `COMPLETED`, except the permanent failure, which ends `FAILED`.
- No job is completed twice. Each success has one attempt outcome of `COMPLETED` and one row in `job_effects`.
- The process that exited mid-lease left an `EXPIRED` attempt. A different worker completed that same job, still with one effect.
- The fail-once job has a failed attempt and then one success. The fail-always job has no effect.
- Nothing starts before `scheduled_at`. The delayed `cobalt` jobs start within 10 seconds of their schedule.
- No organization runs more than 3 jobs at the same time. `bright`'s third job starts before `northstar`'s tenth, so the large tenant does not take the whole queue.
- At least two worker processes completed jobs.

## Run it yourself

```powershell
docker compose up -d --wait
mvn -q package

java -jar target\job-scheduler.jar reset
java -jar target\job-scheduler.jar enqueue --org northstar --type slow --count 30
java -jar target\job-scheduler.jar enqueue --org bright --type fast --count 10
java -jar target\job-scheduler.jar enqueue --org bright --type long --count 1
java -jar target\job-scheduler.jar enqueue --org bright --type fail-once --count 1
java -jar target\job-scheduler.jar enqueue --org cobalt --type fast --count 6
java -jar target\job-scheduler.jar enqueue --org cobalt --type fast --count 6 --delay-seconds 4
java -jar target\job-scheduler.jar enqueue --org cobalt --type fail-always --count 1 --max-attempts 2

$env:LEASE_SECONDS = "6"
$env:HEARTBEAT_MS = "2000"
java -jar target\job-scheduler.jar worker --id worker-1
java -jar target\job-scheduler.jar worker --id worker-2
java -jar target\job-scheduler.jar worker --id worker-3
java -jar target\job-scheduler.jar worker --id crasher --claim-and-exit --types fast,slow

java -jar target\job-scheduler.jar report --wait --timeout-seconds 180
```

Run each `worker` command in its own terminal. The report expects the crashed process to use worker id `crasher`.

## Commands

- `migrate` creates tables if they are missing.
- `reset` creates tables, then deletes every job.
- `enqueue` inserts jobs. `--delay-seconds` sets a future `scheduled_at`. `--max-attempts` overrides the default of 5.
- `worker` polls, claims, and executes. `--threads` sets the pool size. `--claim-and-exit` claims one job and stops the process without finishing it. `--types` limits which job types that process will claim.
- `report` prints counts, lag, and pass or fail. `--wait` polls until nothing is `PENDING` or `RUNNING`.

Job types built into the worker are `fast` (50ms), `slow` (2s), `long` (8s), `fail-once`, and `fail-always`.

## Configuration

Set these in the environment. Defaults are shown.

- `JDBC_URL` `jdbc:postgresql://localhost:5432/scheduler`
- `JDBC_USER` `scheduler`
- `JDBC_PASSWORD` `scheduler`
- `POLL_INTERVAL_MS` `250`
- `LEASE_SECONDS` `15`
- `HEARTBEAT_MS` `5000`
- `WORKER_THREADS` `4`
- `MAX_PER_ORG` `2` (jobs taken from one organization in one claim)
- `MAX_INFLIGHT_PER_ORG` `3` (jobs one organization may run at once)
- `MAX_ATTEMPTS` `5`
- `RETRY_BACKOFF_SECONDS` `1`

`reset` deletes all jobs. Do not point `JDBC_URL` at a database you care about.
