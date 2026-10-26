package com.scheduler;

public record Config(
        String jdbcUrl,
        String username,
        String password,
        int pollIntervalMs,
        int leaseSeconds,
        int heartbeatMs,
        int workerThreads,
        int maxPerOrg,
        int maxInflightPerOrg,
        int maxAttempts,
        int retryBackoffSeconds) {

    public static Config fromEnv() {
        return new Config(
                env("JDBC_URL", "jdbc:postgresql://localhost:5432/scheduler"),
                env("JDBC_USER", "scheduler"),
                env("JDBC_PASSWORD", "scheduler"),
                envInt("POLL_INTERVAL_MS", 250),
                envInt("LEASE_SECONDS", 15),
                envInt("HEARTBEAT_MS", 5_000),
                envInt("WORKER_THREADS", 4),
                envInt("MAX_PER_ORG", 2),
                envInt("MAX_INFLIGHT_PER_ORG", 3),
                envInt("MAX_ATTEMPTS", 5),
                envInt("RETRY_BACKOFF_SECONDS", 1));
    }

    private static String env(String name, String fallback) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }

    private static int envInt(String name, int fallback) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            return fallback;
        }
        return Integer.parseInt(value);
    }
}
