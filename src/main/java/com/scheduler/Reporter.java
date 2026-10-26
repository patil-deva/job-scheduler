package com.scheduler;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

public final class Reporter {
    private static final String CRASHER_ID = "crasher";
    private static final long COBALT_LAG_LIMIT_MS = 10_000;

    private final Database database;
    private final int maxInflightPerOrg;

    public Reporter(Database database, Config config) {
        this.database = database;
        this.maxInflightPerOrg = config.maxInflightPerOrg();
    }

    public int run(boolean wait, int timeoutSeconds) throws Exception {
        if (wait && !waitUntilDrained(timeoutSeconds)) {
            return 1;
        }
        Snapshot snapshot = load();
        printObservations(snapshot);
        List<String> failures = evaluate(snapshot);
        System.out.println();
        System.out.println("Checks");
        if (failures.isEmpty()) {
            System.out.println("  PASS  all invariants hold");
            System.out.println("Result: PASS");
            return 0;
        }
        for (String failure : failures) {
            System.out.println("  FAIL  " + failure);
        }
        System.out.println("Result: FAIL");
        return 1;
    }

    private boolean waitUntilDrained(int timeoutSeconds) throws Exception {
        System.out.println("waiting up to " + timeoutSeconds + "s for the queue to drain");
        long deadline = System.nanoTime() + Duration.ofSeconds(timeoutSeconds).toNanos();
        while (System.nanoTime() < deadline) {
            int active = countActive();
            if (active == 0) {
                System.out.println("queue drained");
                return true;
            }
            System.out.println("  pending_or_running=" + active);
            Thread.sleep(1_000);
        }
        System.out.println("timed out with pending_or_running=" + countActive());
        return false;
    }

    private int countActive() throws Exception {
        try (Connection connection = database.connection();
             Statement statement = connection.createStatement();
             ResultSet rows = statement.executeQuery(
                     "SELECT COUNT(*) FROM jobs WHERE status IN ('PENDING', 'RUNNING')")) {
            rows.next();
            return rows.getInt(1);
        }
    }

    private Snapshot load() throws Exception {
        Map<Long, JobRow> jobs = new HashMap<>();
        List<AttemptRow> attempts = new ArrayList<>();
        Set<Long> effects = new TreeSet<>();
        try (Connection connection = database.connection();
             Statement statement = connection.createStatement()) {
            try (ResultSet rows = statement.executeQuery("""
                    SELECT id, organization_id, job_type, status, scheduled_at, attempts, max_attempts
                    FROM jobs
                    ORDER BY id
                    """)) {
                while (rows.next()) {
                    JobRow job = new JobRow(
                            rows.getLong("id"),
                            rows.getString("organization_id"),
                            rows.getString("job_type"),
                            rows.getString("status"),
                            instant(rows, "scheduled_at"),
                            rows.getInt("attempts"),
                            rows.getInt("max_attempts"));
                    jobs.put(job.id, job);
                }
            }
            try (ResultSet rows = statement.executeQuery("""
                    SELECT job_id, attempt, worker_id, outcome, started_at, finished_at
                    FROM job_attempts
                    ORDER BY job_id, attempt
                    """)) {
                while (rows.next()) {
                    attempts.add(new AttemptRow(
                            rows.getLong("job_id"),
                            rows.getInt("attempt"),
                            rows.getString("worker_id"),
                            rows.getString("outcome"),
                            instant(rows, "started_at"),
                            instant(rows, "finished_at")));
                }
            }
            try (ResultSet rows = statement.executeQuery("SELECT job_id FROM job_effects")) {
                while (rows.next()) {
                    effects.add(rows.getLong(1));
                }
            }
        }
        return new Snapshot(jobs, attempts, effects);
    }

    private void printObservations(Snapshot snapshot) {
        System.out.println();
        System.out.println("Jobs");
        Map<String, Map<String, Long>> byOrg = new TreeMap<>();
        for (JobRow job : snapshot.jobs.values()) {
            byOrg.computeIfAbsent(job.org, ignored -> new TreeMap<>())
                    .merge(job.status, 1L, Long::sum);
        }
        for (var org : byOrg.entrySet()) {
            String counts = org.getValue().entrySet().stream()
                    .map(entry -> entry.getValue() + " " + entry.getKey())
                    .collect(Collectors.joining(", "));
            System.out.println("  " + org.getKey() + ": " + counts);
        }

        System.out.println("Attempts");
        Map<String, Map<String, Long>> byWorker = new TreeMap<>();
        for (AttemptRow attempt : snapshot.attempts) {
            String outcome = attempt.outcome == null ? "OPEN" : attempt.outcome;
            byWorker.computeIfAbsent(attempt.workerId, ignored -> new TreeMap<>())
                    .merge(outcome, 1L, Long::sum);
        }
        for (var worker : byWorker.entrySet()) {
            String counts = worker.getValue().entrySet().stream()
                    .map(entry -> entry.getValue() + " " + entry.getKey())
                    .collect(Collectors.joining(", "));
            System.out.println("  " + worker.getKey() + ": " + counts);
        }

        System.out.println("Schedule lag (first start minus scheduled_at)");
        for (var org : firstStartsByOrg(snapshot).entrySet()) {
            long maxLag = Long.MIN_VALUE;
            long sum = 0;
            int count = 0;
            for (var jobStart : org.getValue()) {
                JobRow job = snapshot.jobs.get(jobStart.jobId);
                long lag = Duration.between(job.scheduledAt, jobStart.startedAt).toMillis();
                maxLag = Math.max(maxLag, lag);
                sum += lag;
                count++;
            }
            if (count > 0) {
                System.out.println("  " + org.getKey() + ": avg " + (sum / count) + "ms, max " + maxLag + "ms");
            }
        }

        Map<String, Integer> inflight = maxInflight(snapshot);
        System.out.println("Peak overlapping runs per organization (cap " + maxInflightPerOrg + ")");
        inflight.forEach((org, peak) -> System.out.println("  " + org + ": " + peak));

        printFairness(snapshot);
        printSpecialJobs(snapshot);
    }

    private void printFairness(Snapshot snapshot) {
        List<JobStart> bright = startsFor(snapshot, "bright");
        List<JobStart> northstar = startsFor(snapshot, "northstar");
        System.out.println("Fairness");
        if (bright.size() >= 3 && northstar.size() >= 10) {
            Instant thirdBright = bright.get(2).startedAt;
            Instant tenthNorthstar = northstar.get(9).startedAt;
            long delta = Duration.between(thirdBright, tenthNorthstar).toMillis();
            System.out.println("  bright job 3 started at " + thirdBright);
            System.out.println("  northstar job 10 started at " + tenthNorthstar + " (" + delta + "ms later)");
        } else {
            System.out.println("  not enough jobs to compare bright #3 and northstar #10");
        }
    }

    private void printSpecialJobs(Snapshot snapshot) {
        System.out.println("Recovery and failures");
        for (AttemptRow attempt : snapshot.attempts) {
            if (CRASHER_ID.equals(attempt.workerId)) {
                List<AttemptRow> history = attemptsFor(snapshot, attempt.jobId);
                String path = history.stream()
                        .map(row -> row.attempt + ":" + row.workerId + ":" + row.outcome)
                        .collect(Collectors.joining(", "));
                System.out.println("  abandoned job " + attempt.jobId + " -> " + path
                        + ", effects=" + (snapshot.effects.contains(attempt.jobId) ? 1 : 0));
            }
        }
        for (JobRow job : snapshot.jobs.values()) {
            if (job.type.equals("fail-once") || job.type.equals("fail-always") || job.type.equals("long")) {
                String path = attemptsFor(snapshot, job.id).stream()
                        .map(row -> row.attempt + ":" + row.outcome)
                        .collect(Collectors.joining(", "));
                System.out.println("  " + job.type + " job " + job.id + " status=" + job.status + " -> " + path);
            }
        }
    }

    private List<String> evaluate(Snapshot snapshot) {
        List<String> failures = new ArrayList<>();
        Set<String> orgs = snapshot.jobs.values().stream().map(job -> job.org).collect(Collectors.toSet());
        if (snapshot.jobs.size() < 50) {
            failures.add("expected at least 50 jobs, found " + snapshot.jobs.size());
        }
        if (orgs.size() < 3) {
            failures.add("expected at least 3 organizations, found " + orgs.size());
        }

        long completed = snapshot.jobs.values().stream().filter(job -> job.status.equals("COMPLETED")).count();
        if (completed != snapshot.effects.size()) {
            failures.add("completed jobs (" + completed + ") != effects (" + snapshot.effects.size() + ")");
        }

        Map<Long, List<AttemptRow>> byJob = snapshot.attempts.stream()
                .collect(Collectors.groupingBy(attempt -> attempt.jobId));

        for (JobRow job : snapshot.jobs.values()) {
            List<AttemptRow> history = byJob.getOrDefault(job.id, List.of());
            long successes = history.stream().filter(attempt -> "COMPLETED".equals(attempt.outcome)).count();
            boolean hasEffect = snapshot.effects.contains(job.id);
            if (job.status.equals("COMPLETED")) {
                if (successes != 1) {
                    failures.add("job " + job.id + " completed with " + successes + " successful attempts");
                }
                if (!hasEffect) {
                    failures.add("job " + job.id + " completed without an effect");
                }
            } else if (job.status.equals("FAILED")) {
                if (hasEffect) {
                    failures.add("job " + job.id + " failed but wrote an effect");
                }
                if (successes != 0) {
                    failures.add("job " + job.id + " failed but has a successful attempt");
                }
            } else {
                failures.add("job " + job.id + " is still " + job.status);
            }

            for (int i = 0; i < history.size(); i++) {
                AttemptRow left = history.get(i);
                if (left.finishedAt == null) {
                    failures.add("job " + job.id + " attempt " + left.attempt + " is still open");
                }
                if (left.startedAt.isBefore(job.scheduledAt)) {
                    failures.add("job " + job.id + " attempt " + left.attempt + " started before scheduled_at");
                }
                for (int j = i + 1; j < history.size(); j++) {
                    if (overlaps(left, history.get(j))) {
                        failures.add("job " + job.id + " has overlapping attempts "
                                + left.attempt + " and " + history.get(j).attempt);
                    }
                }
            }
        }

        for (var peak : maxInflight(snapshot).entrySet()) {
            if (peak.getValue() > maxInflightPerOrg) {
                failures.add(peak.getKey() + " ran " + peak.getValue()
                        + " jobs at once; cap is " + maxInflightPerOrg);
            }
        }

        List<AttemptRow> abandoned = snapshot.attempts.stream()
                .filter(attempt -> CRASHER_ID.equals(attempt.workerId))
                .toList();
        if (abandoned.size() != 1 || !"EXPIRED".equals(abandoned.get(0).outcome)) {
            failures.add("expected one EXPIRED attempt from worker '" + CRASHER_ID + "', found " + abandoned.size());
        } else {
            AttemptRow crash = abandoned.get(0);
            JobRow job = snapshot.jobs.get(crash.jobId);
            boolean recovered = byJob.getOrDefault(crash.jobId, List.of()).stream()
                    .anyMatch(attempt -> attempt.attempt > crash.attempt
                            && "COMPLETED".equals(attempt.outcome)
                            && !CRASHER_ID.equals(attempt.workerId));
            if (job == null || !job.status.equals("COMPLETED") || !recovered || !snapshot.effects.contains(job.id)) {
                failures.add("abandoned job " + crash.jobId + " was not recovered exactly once");
            }
        }

        checkSingle(snapshot, byJob, failures, "fail-once", "COMPLETED", true);
        checkSingle(snapshot, byJob, failures, "fail-always", "FAILED", false);
        List<JobRow> longJobs = snapshot.jobs.values().stream().filter(job -> job.type.equals("long")).toList();
        if (longJobs.size() != 1) {
            failures.add("expected one long job, found " + longJobs.size());
        } else {
            JobRow job = longJobs.get(0);
            List<AttemptRow> history = byJob.getOrDefault(job.id, List.of());
            if (!job.status.equals("COMPLETED") || job.attempts != 1 || history.size() != 1) {
                failures.add("long job " + job.id + " did not finish on its first lease (heartbeat failed)");
            }
        }

        List<JobStart> bright = startsFor(snapshot, "bright");
        List<JobStart> northstar = startsFor(snapshot, "northstar");
        if (bright.size() < 3 || northstar.size() < 10) {
            failures.add("fairness check needs 3 bright jobs and 10 northstar jobs");
        } else if (!bright.get(2).startedAt.isBefore(northstar.get(9).startedAt)) {
            failures.add("bright's 3rd job did not start before northstar's 10th");
        }

        for (JobStart start : startsFor(snapshot, "cobalt")) {
            JobRow job = snapshot.jobs.get(start.jobId);
            long lag = Duration.between(job.scheduledAt, start.startedAt).toMillis();
            if (lag < 0 || lag > COBALT_LAG_LIMIT_MS) {
                failures.add("cobalt job " + job.id + " lag " + lag + "ms is outside 0.." + COBALT_LAG_LIMIT_MS);
            }
        }

        long workers = snapshot.attempts.stream()
                .filter(attempt -> "COMPLETED".equals(attempt.outcome))
                .map(attempt -> attempt.workerId)
                .distinct()
                .count();
        if (workers < 2) {
            failures.add("expected at least 2 workers to complete jobs, found " + workers);
        }
        return failures;
    }

    private void checkSingle(Snapshot snapshot, Map<Long, List<AttemptRow>> byJob, List<String> failures,
                             String type, String status, boolean expectSuccess) {
        List<JobRow> jobs = snapshot.jobs.values().stream().filter(job -> job.type.equals(type)).toList();
        if (jobs.size() != 1) {
            failures.add("expected one " + type + " job, found " + jobs.size());
            return;
        }
        JobRow job = jobs.get(0);
        List<AttemptRow> history = byJob.getOrDefault(job.id, List.of()).stream()
                .sorted(Comparator.comparingInt(attempt -> attempt.attempt))
                .toList();
        int firstFailure = history.stream()
                .filter(attempt -> "FAILED".equals(attempt.outcome))
                .mapToInt(attempt -> attempt.attempt)
                .min()
                .orElse(-1);
        int firstSuccess = history.stream()
                .filter(attempt -> "COMPLETED".equals(attempt.outcome))
                .mapToInt(attempt -> attempt.attempt)
                .min()
                .orElse(-1);
        if (!job.status.equals(status)) {
            failures.add(type + " job " + job.id + " is " + job.status + ", expected " + status);
        }
        if (expectSuccess && (firstFailure < 1 || firstSuccess <= firstFailure || !snapshot.effects.contains(job.id))) {
            failures.add(type + " job " + job.id + " should fail once and then succeed once");
        }
        if (!expectSuccess && (firstSuccess != -1 || snapshot.effects.contains(job.id) || job.attempts != job.maxAttempts)) {
            failures.add(type + " job " + job.id + " should exhaust attempts with no effect");
        }
    }

    private Map<String, Integer> maxInflight(Snapshot snapshot) {
        Map<Long, String> orgByJob = new HashMap<>();
        snapshot.jobs.values().forEach(job -> orgByJob.put(job.id, job.org));
        Map<String, List<AttemptRow>> byOrg = new HashMap<>();
        for (AttemptRow attempt : snapshot.attempts) {
            String org = orgByJob.get(attempt.jobId);
            if (org != null) {
                byOrg.computeIfAbsent(org, ignored -> new ArrayList<>()).add(attempt);
            }
        }
        Map<String, Integer> peaks = new TreeMap<>();
        for (var entry : byOrg.entrySet()) {
            int peak = 0;
            for (AttemptRow probe : entry.getValue()) {
                int running = 0;
                for (AttemptRow other : entry.getValue()) {
                    if (!other.startedAt.isAfter(probe.startedAt) && stillRunningAt(other, probe.startedAt)) {
                        running++;
                    }
                }
                peak = Math.max(peak, running);
            }
            peaks.put(entry.getKey(), peak);
        }
        return peaks;
    }

    private static boolean stillRunningAt(AttemptRow attempt, Instant at) {
        return attempt.finishedAt == null || attempt.finishedAt.isAfter(at);
    }

    private static boolean overlaps(AttemptRow left, AttemptRow right) {
        Instant leftEnd = left.finishedAt == null ? Instant.MAX : left.finishedAt;
        Instant rightEnd = right.finishedAt == null ? Instant.MAX : right.finishedAt;
        return left.startedAt.isBefore(rightEnd) && right.startedAt.isBefore(leftEnd);
    }

    private Map<String, List<JobStart>> firstStartsByOrg(Snapshot snapshot) {
        Map<String, List<JobStart>> byOrg = new TreeMap<>();
        for (String org : snapshot.jobs.values().stream().map(job -> job.org).collect(Collectors.toSet())) {
            byOrg.put(org, startsFor(snapshot, org));
        }
        return byOrg;
    }

    private List<JobStart> startsFor(Snapshot snapshot, String org) {
        Map<Long, Instant> first = new HashMap<>();
        for (AttemptRow attempt : snapshot.attempts) {
            JobRow job = snapshot.jobs.get(attempt.jobId);
            if (job != null && job.org.equals(org)) {
                first.merge(attempt.jobId, attempt.startedAt, (left, right) -> left.isBefore(right) ? left : right);
            }
        }
        return first.entrySet().stream()
                .map(entry -> new JobStart(entry.getKey(), entry.getValue()))
                .sorted(Comparator.comparing((JobStart start) -> start.startedAt).thenComparingLong(start -> start.jobId))
                .toList();
    }

    private List<AttemptRow> attemptsFor(Snapshot snapshot, long jobId) {
        return snapshot.attempts.stream()
                .filter(attempt -> attempt.jobId == jobId)
                .sorted(Comparator.comparingInt(attempt -> attempt.attempt))
                .toList();
    }

    private static Instant instant(ResultSet rows, String column) throws Exception {
        OffsetDateTime value = rows.getObject(column, OffsetDateTime.class);
        return value == null ? null : value.toInstant();
    }

    private record JobRow(long id, String org, String type, String status, Instant scheduledAt,
                          int attempts, int maxAttempts) {
    }

    private record AttemptRow(long jobId, int attempt, String workerId, String outcome,
                              Instant startedAt, Instant finishedAt) {
    }

    private record JobStart(long jobId, Instant startedAt) {
    }

    private record Snapshot(Map<Long, JobRow> jobs, List<AttemptRow> attempts, Set<Long> effects) {
    }
}
