package com.scheduler;

import java.util.Map;

public final class Handlers {
    private Handlers() {
    }

    public static Map<String, JobHandler> demo() {
        return Map.of(
                "fast", (job, context) -> sleep(context, 50),
                "slow", (job, context) -> sleep(context, 2_000),
                "long", (job, context) -> sleep(context, 8_000),
                "fail-once", (job, context) -> {
                    sleep(context, 50);
                    if (job.attempts() == 1) {
                        throw new IllegalStateException("transient failure");
                    }
                },
                "fail-always", (job, context) -> {
                    sleep(context, 50);
                    throw new IllegalStateException("permanent failure");
                });
    }

    private static void sleep(JobContext context, long totalMs) throws InterruptedException {
        long remaining = totalMs;
        while (remaining > 0) {
            context.checkpoint();
            long step = Math.min(200, remaining);
            Thread.sleep(step);
            remaining -= step;
        }
        context.checkpoint();
    }
}
