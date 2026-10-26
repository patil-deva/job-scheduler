package com.scheduler;

import java.time.Instant;
import java.util.UUID;

public record Job(
        long id,
        String organizationId,
        String jobType,
        String payload,
        Instant scheduledAt,
        int attempts,
        int maxAttempts,
        UUID leaseToken) {
}
