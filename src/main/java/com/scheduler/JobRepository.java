package com.scheduler;

import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public final class JobRepository {
    private static final int CLAIM_LOCK = 424242;

    private final Database database;

    public JobRepository(Database database) {
        this.database = database;
    }

    public int enqueue(String organizationId, String jobType, String payload, int count,
                       int delaySeconds, int maxAttempts) throws SQLException {
        String sql = """
                INSERT INTO jobs (
                    organization_id, job_type, payload, scheduled_at, earliest_run_at, max_attempts
                )
                SELECT ?, ?, ?, run_at, run_at, ?
                FROM (SELECT clock_timestamp() + make_interval(secs => ?) AS run_at) AS t
                """;
        try (Connection connection = database.connection()) {
            connection.setAutoCommit(false);
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                for (int i = 0; i < count; i++) {
                    statement.setString(1, organizationId);
                    statement.setString(2, jobType);
                    statement.setString(3, payload == null ? "" : payload);
                    statement.setInt(4, maxAttempts);
                    statement.setInt(5, delaySeconds);
                    statement.addBatch();
                }
                statement.executeBatch();
            }
            connection.commit();
        }
        return count;
    }

    public void reset() throws SQLException {
        try (Connection connection = database.connection();
             Statement statement = connection.createStatement()) {
            statement.execute("TRUNCATE TABLE job_effects, job_attempts, jobs RESTART IDENTITY");
        }
    }

    /**
     * Claims up to {@code limit} due jobs.
     * Ranking and locking are separate statements: PostgreSQL cannot combine
     * window functions with FOR UPDATE. A transaction advisory lock keeps the
     * per-organization inflight cap exact while SKIP LOCKED still gives each
     * row to only one claimer.
     */
    public List<Job> claim(String workerId, int limit, int maxPerOrg, int maxInflight,
                           int leaseSeconds, List<String> types) throws SQLException {
        if (limit <= 0) {
            return List.of();
        }
        try (Connection connection = database.connection()) {
            connection.setAutoCommit(false);
            try {
                try (Statement statement = connection.createStatement();
                     ResultSet ignored = statement.executeQuery(
                             "SELECT pg_advisory_xact_lock(" + CLAIM_LOCK + ")")) {
                    ignored.next();
                }
                failExhaustedLeases(connection);
                long[] ids = selectFairIds(connection, limit, maxPerOrg, maxInflight, types);
                if (ids.length == 0) {
                    connection.commit();
                    return List.of();
                }
                List<Job> jobs = markRunning(connection, ids, workerId, leaseSeconds);
                for (Job job : jobs) {
                    expireOpenAttempts(connection, job.id());
                    insertAttempt(connection, job, workerId);
                }
                connection.commit();
                return jobs;
            } catch (SQLException | RuntimeException e) {
                connection.rollback();
                throw e;
            }
        }
    }

    public boolean heartbeat(long jobId, UUID leaseToken, int leaseSeconds) throws SQLException {
        String sql = """
                UPDATE jobs
                SET lease_expires_at = clock_timestamp() + make_interval(secs => ?)
                WHERE id = ? AND lease_token = ? AND status = 'RUNNING'
                """;
        try (Connection connection = database.connection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setInt(1, leaseSeconds);
            statement.setLong(2, jobId);
            statement.setObject(3, leaseToken);
            return statement.executeUpdate() == 1;
        }
    }

    public boolean complete(Job job, String workerId) throws SQLException {
        String finishJob = """
                UPDATE jobs
                SET status = 'COMPLETED',
                    completed_at = clock_timestamp(),
                    last_error = NULL,
                    lease_token = NULL,
                    leased_by = NULL,
                    lease_expires_at = NULL
                WHERE id = ? AND lease_token = ? AND status = 'RUNNING'
                """;
        String recordEffect = """
                INSERT INTO job_effects (job_id, worker_id)
                VALUES (?, ?)
                ON CONFLICT (job_id) DO NOTHING
                """;
        String finishAttempt = """
                UPDATE job_attempts
                SET outcome = 'COMPLETED', finished_at = clock_timestamp()
                WHERE job_id = ? AND lease_token = ? AND finished_at IS NULL
                """;
        try (Connection connection = database.connection()) {
            connection.setAutoCommit(false);
            try {
                int updated = updateOwnedJob(connection, finishJob, job);
                if (updated != 1) {
                    connection.rollback();
                    return false;
                }
                try (PreparedStatement effect = connection.prepareStatement(recordEffect)) {
                    effect.setLong(1, job.id());
                    effect.setString(2, workerId);
                    effect.executeUpdate();
                }
                try (PreparedStatement attempt = connection.prepareStatement(finishAttempt)) {
                    attempt.setLong(1, job.id());
                    attempt.setObject(2, job.leaseToken());
                    attempt.executeUpdate();
                }
                connection.commit();
                return true;
            } catch (SQLException | RuntimeException e) {
                connection.rollback();
                throw e;
            }
        }
    }

    public boolean fail(Job job, String error, int backoffSeconds) throws SQLException {
        String finishJob = """
                UPDATE jobs
                SET status = CASE WHEN attempts >= max_attempts THEN 'FAILED' ELSE 'PENDING' END,
                    earliest_run_at = CASE
                        WHEN attempts >= max_attempts THEN earliest_run_at
                        ELSE clock_timestamp() + make_interval(secs => ?)
                    END,
                    last_error = ?,
                    completed_at = CASE
                        WHEN attempts >= max_attempts THEN clock_timestamp()
                        ELSE NULL
                    END,
                    lease_token = NULL,
                    leased_by = NULL,
                    lease_expires_at = NULL
                WHERE id = ? AND lease_token = ? AND status = 'RUNNING'
                """;
        String finishAttempt = """
                UPDATE job_attempts
                SET outcome = 'FAILED', finished_at = clock_timestamp()
                WHERE job_id = ? AND lease_token = ? AND finished_at IS NULL
                """;
        String message = error == null ? "failed" : error;
        if (message.length() > 500) {
            message = message.substring(0, 500);
        }
        try (Connection connection = database.connection()) {
            connection.setAutoCommit(false);
            try {
                int updated;
                try (PreparedStatement statement = connection.prepareStatement(finishJob)) {
                    statement.setInt(1, backoffSeconds);
                    statement.setString(2, message);
                    statement.setLong(3, job.id());
                    statement.setObject(4, job.leaseToken());
                    updated = statement.executeUpdate();
                }
                if (updated != 1) {
                    connection.rollback();
                    return false;
                }
                try (PreparedStatement attempt = connection.prepareStatement(finishAttempt)) {
                    attempt.setLong(1, job.id());
                    attempt.setObject(2, job.leaseToken());
                    attempt.executeUpdate();
                }
                connection.commit();
                return true;
            } catch (SQLException | RuntimeException e) {
                connection.rollback();
                throw e;
            }
        }
    }

    private void failExhaustedLeases(Connection connection) throws SQLException {
        String sql = """
                UPDATE jobs
                SET status = 'FAILED',
                    last_error = 'lease expired after max attempts',
                    completed_at = clock_timestamp(),
                    lease_token = NULL,
                    leased_by = NULL,
                    lease_expires_at = NULL
                WHERE status = 'RUNNING'
                  AND lease_expires_at <= clock_timestamp()
                  AND attempts >= max_attempts
                RETURNING id
                """;
        List<Long> ids = new ArrayList<>();
        try (Statement statement = connection.createStatement()) {
            if (statement.execute(sql)) {
                try (ResultSet rows = statement.getResultSet()) {
                    while (rows.next()) {
                        ids.add(rows.getLong(1));
                    }
                }
            }
        }
        String closeAttempt = """
                UPDATE job_attempts
                SET outcome = 'FAILED', finished_at = clock_timestamp()
                WHERE job_id = ? AND finished_at IS NULL
                """;
        try (PreparedStatement statement = connection.prepareStatement(closeAttempt)) {
            for (long id : ids) {
                statement.setLong(1, id);
                statement.addBatch();
            }
            if (!ids.isEmpty()) {
                statement.executeBatch();
            }
        }
    }

    private long[] selectFairIds(Connection connection, int limit, int maxPerOrg, int maxInflight,
                                 List<String> types) throws SQLException {
        boolean allowAll = types == null || types.isEmpty();
        String[] typeArray = allowAll ? new String[0] : types.toArray(String[]::new);
        String sql = """
                WITH inflight AS (
                    SELECT organization_id, COUNT(*)::int AS n
                    FROM jobs
                    WHERE status = 'RUNNING'
                      AND lease_expires_at > clock_timestamp()
                    GROUP BY organization_id
                ),
                ranked AS (
                    SELECT j.id,
                           j.scheduled_at,
                           COALESCE(i.n, 0) AS inflight,
                           ROW_NUMBER() OVER (
                               PARTITION BY j.organization_id
                               ORDER BY j.earliest_run_at, j.id
                           ) AS org_rank
                    FROM jobs j
                    LEFT JOIN inflight i ON i.organization_id = j.organization_id
                    WHERE j.attempts < j.max_attempts
                      AND j.earliest_run_at <= clock_timestamp()
                      AND COALESCE(i.n, 0) < ?
                      AND (? OR j.job_type = ANY (?))
                      AND (
                            (j.status = 'PENDING' AND j.scheduled_at <= clock_timestamp())
                         OR (j.status = 'RUNNING' AND j.lease_expires_at <= clock_timestamp())
                      )
                )
                SELECT id
                FROM ranked
                WHERE org_rank <= LEAST(CAST(? AS integer), CAST(? AS integer) - inflight)
                ORDER BY org_rank, scheduled_at, id
                LIMIT ?
                """;
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            Array sqlTypes = connection.createArrayOf("text", typeArray);
            statement.setInt(1, maxInflight);
            statement.setBoolean(2, allowAll);
            statement.setArray(3, sqlTypes);
            statement.setInt(4, maxPerOrg);
            statement.setInt(5, maxInflight);
            statement.setInt(6, limit);
            try (ResultSet rows = statement.executeQuery()) {
                List<Long> ids = new ArrayList<>();
                while (rows.next()) {
                    ids.add(rows.getLong(1));
                }
                return ids.stream().mapToLong(Long::longValue).toArray();
            } finally {
                sqlTypes.free();
            }
        }
    }

    private List<Job> markRunning(Connection connection, long[] ids, String workerId, int leaseSeconds)
            throws SQLException {
        String sql = """
                UPDATE jobs j
                SET status = 'RUNNING',
                    attempts = j.attempts + 1,
                    lease_token = gen_random_uuid(),
                    leased_by = ?,
                    lease_expires_at = clock_timestamp() + make_interval(secs => ?),
                    started_at = COALESCE(j.started_at, clock_timestamp()),
                    last_error = CASE
                        WHEN j.status = 'RUNNING' THEN 'reclaimed after lease expiry'
                        ELSE NULL
                    END
                WHERE j.id IN (
                    SELECT id
                    FROM jobs
                    WHERE id = ANY (?)
                    FOR UPDATE SKIP LOCKED
                )
                  AND j.attempts < j.max_attempts
                  AND (
                        (j.status = 'PENDING'
                            AND j.scheduled_at <= clock_timestamp()
                            AND j.earliest_run_at <= clock_timestamp())
                     OR (j.status = 'RUNNING' AND j.lease_expires_at <= clock_timestamp())
                  )
                RETURNING j.id, j.organization_id, j.job_type, j.payload, j.scheduled_at,
                          j.attempts, j.max_attempts, j.lease_token
                """;
        List<Job> jobs = new ArrayList<>();
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            Array idArray = connection.createArrayOf("bigint", boxed(ids));
            statement.setString(1, workerId);
            statement.setInt(2, leaseSeconds);
            statement.setArray(3, idArray);
            try (ResultSet rows = statement.executeQuery()) {
                while (rows.next()) {
                    jobs.add(readJob(rows));
                }
            } finally {
                idArray.free();
            }
        }
        return jobs;
    }

    private static Long[] boxed(long[] ids) {
        Long[] boxed = new Long[ids.length];
        for (int i = 0; i < ids.length; i++) {
            boxed[i] = ids[i];
        }
        return boxed;
    }

    private static void expireOpenAttempts(Connection connection, long jobId) throws SQLException {
        String sql = """
                UPDATE job_attempts
                SET outcome = 'EXPIRED', finished_at = clock_timestamp()
                WHERE job_id = ? AND finished_at IS NULL
                """;
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setLong(1, jobId);
            statement.executeUpdate();
        }
    }

    private static void insertAttempt(Connection connection, Job job, String workerId) throws SQLException {
        String sql = """
                INSERT INTO job_attempts (job_id, attempt, worker_id, lease_token)
                VALUES (?, ?, ?, ?)
                """;
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setLong(1, job.id());
            statement.setInt(2, job.attempts());
            statement.setString(3, workerId);
            statement.setObject(4, job.leaseToken());
            statement.executeUpdate();
        }
    }

    private static int updateOwnedJob(Connection connection, String sql, Job job) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setLong(1, job.id());
            statement.setObject(2, job.leaseToken());
            return statement.executeUpdate();
        }
    }

    private static Job readJob(ResultSet rows) throws SQLException {
        OffsetDateTime scheduledAt = rows.getObject("scheduled_at", OffsetDateTime.class);
        return new Job(
                rows.getLong("id"),
                rows.getString("organization_id"),
                rows.getString("job_type"),
                rows.getString("payload"),
                scheduledAt.toInstant(),
                rows.getInt("attempts"),
                rows.getInt("max_attempts"),
                rows.getObject("lease_token", UUID.class));
    }
}
