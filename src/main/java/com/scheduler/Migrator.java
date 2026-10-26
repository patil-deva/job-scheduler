package com.scheduler;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public final class Migrator {
    private Migrator() {
    }

    public static void migrate(Database database) throws SQLException, IOException {
        String sql = new String(
                Migrator.class.getResourceAsStream("/schema.sql").readAllBytes(),
                StandardCharsets.UTF_8);
        try (Connection connection = database.connection();
             Statement statement = connection.createStatement()) {
            for (String chunk : sql.split(";")) {
                if (hasSql(chunk)) {
                    statement.execute(chunk);
                }
            }
        }
        System.out.println("schema ready");
    }

    private static boolean hasSql(String chunk) {
        for (String line : chunk.split("\n")) {
            String trimmed = line.trim();
            if (!trimmed.isEmpty() && !trimmed.startsWith("--")) {
                return true;
            }
        }
        return false;
    }
}
