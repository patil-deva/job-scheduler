package com.scheduler;

import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public final class JobRunner {
    private final JobRepository repository;
    private final Map<String, JobHandler> handlers;
    private final int leaseSeconds;
    private final int heartbeatMs;
    private final int backoffSeconds;
    private final String workerId;

    public JobRunner(JobRepository repository, Map<String, JobHandler> handlers, Config config, String workerId) {
        this.repository = repository;
        this.handlers = handlers;
        this.leaseSeconds = config.leaseSeconds();
        this.heartbeatMs = config.heartbeatMs();
        this.backoffSeconds = config.retryBackoffSeconds();
        this.workerId = workerId;
    }

    public void run(Job job) {
        JobHandler handler = handlers.get(job.jobType());
        if (handler == null) {
            failQuietly(job, "no handler for type " + job.jobType());
            return;
        }

        JobContext context = new JobContext();
        ScheduledExecutorService heartbeats = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "heartbeat-" + job.id());
            thread.setDaemon(true);
            return thread;
        });
        long initialDelay = Math.min(1_000, heartbeatMs);
        heartbeats.scheduleAtFixedRate(() -> {
            try {
                if (!repository.heartbeat(job.id(), job.leaseToken(), leaseSeconds)) {
                    context.markLost();
                }
            } catch (Exception e) {
                context.markLost();
            }
        }, initialDelay, heartbeatMs, TimeUnit.MILLISECONDS);

        try {
            handler.execute(job, context);
            context.checkpoint();
            if (repository.complete(job, workerId)) {
                log("completed worker=%s job=%d org=%s type=%s attempt=%d",
                        workerId, job.id(), job.organizationId(), job.jobType(), job.attempts());
            } else {
                log("lost lease worker=%s job=%d", workerId, job.id());
            }
        } catch (JobContext.LeaseLostException e) {
            log("lease lost worker=%s job=%d", workerId, job.id());
        } catch (Exception e) {
            if (context.lost()) {
                log("lease lost worker=%s job=%d", workerId, job.id());
                return;
            }
            failQuietly(job, e.getMessage());
        } finally {
            heartbeats.shutdownNow();
        }
    }

    private void failQuietly(Job job, String message) {
        try {
            if (repository.fail(job, message, backoffSeconds)) {
                log("failed worker=%s job=%d org=%s type=%s attempt=%d error=%s",
                        workerId, job.id(), job.organizationId(), job.jobType(), job.attempts(), message);
            } else {
                log("lost lease worker=%s job=%d", workerId, job.id());
            }
        } catch (Exception e) {
            log("could not record failure job=%d error=%s", job.id(), e.getMessage());
        }
    }

    private static void log(String format, Object... args) {
        System.out.printf(format + "%n", args);
        System.out.flush();
    }
}
