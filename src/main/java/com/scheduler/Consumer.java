package com.scheduler;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public final class Consumer {
    private final JobRepository repository;
    private final JobRunner runner;
    private final Config config;
    private final String workerId;
    private final int threads;
    private final boolean claimAndExit;
    private final List<String> types;
    private final ExecutorService pool;
    private final AtomicInteger active = new AtomicInteger();
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final Object monitor = new Object();

    public Consumer(JobRepository repository, Map<String, JobHandler> handlers, Config config,
                    String workerId, int threads, boolean claimAndExit, List<String> types) {
        this.repository = repository;
        this.runner = new JobRunner(repository, handlers, config, workerId);
        this.config = config;
        this.workerId = workerId;
        this.threads = threads;
        this.claimAndExit = claimAndExit;
        this.types = types;
        this.pool = Executors.newFixedThreadPool(threads, runnable -> {
            Thread thread = new Thread(runnable, "worker-" + workerId);
            thread.setDaemon(true);
            return thread;
        });
    }

    public void loop() {
        log("worker started id=%s threads=%d lease=%ds heartbeat=%dms cap=%d claimAndExit=%s",
                workerId, threads, config.leaseSeconds(), config.heartbeatMs(),
                config.maxInflightPerOrg(), claimAndExit);
        while (running.get()) {
            try {
                if (claimAndExit) {
                    List<Job> jobs = repository.claim(
                            workerId, 1, config.maxPerOrg(), config.maxInflightPerOrg(),
                            config.leaseSeconds(), types);
                    if (!jobs.isEmpty()) {
                        Job job = jobs.get(0);
                        log("ABANDONED worker=%s job=%d org=%s type=%s attempt=%d",
                                workerId, job.id(), job.organizationId(), job.jobType(), job.attempts());
                        Runtime.getRuntime().halt(1);
                    }
                } else {
                    int free = threads - active.get();
                    if (free > 0) {
                        List<Job> jobs = repository.claim(
                                workerId, free, config.maxPerOrg(), config.maxInflightPerOrg(),
                                config.leaseSeconds(), types);
                        for (Job job : jobs) {
                            active.incrementAndGet();
                            pool.submit(() -> {
                                try {
                                    log("claimed worker=%s job=%d org=%s type=%s attempt=%d",
                                            workerId, job.id(), job.organizationId(), job.jobType(), job.attempts());
                                    runner.run(job);
                                } finally {
                                    active.decrementAndGet();
                                }
                            });
                        }
                    }
                }
                pause();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                log("worker %s poll error: %s", workerId, e.getMessage());
                try {
                    pause();
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
        pool.shutdownNow();
        try {
            pool.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        log("worker stopped id=%s", workerId);
    }

    public void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        synchronized (monitor) {
            monitor.notifyAll();
        }
        pool.shutdownNow();
    }

    private void pause() throws InterruptedException {
        synchronized (monitor) {
            if (!running.get()) {
                return;
            }
            monitor.wait(config.pollIntervalMs());
        }
    }

    private static void log(String format, Object... args) {
        System.out.printf(format + "%n", args);
        System.out.flush();
    }
}
