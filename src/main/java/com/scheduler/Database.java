package com.scheduler;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public final class Database implements AutoCloseable {
    private final HikariDataSource dataSource;

    private Database(Config config, int poolSize) {
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("PostgreSQL JDBC driver is not on the classpath", e);
        }
        HikariConfig hikari = new HikariConfig();
        hikari.setJdbcUrl(config.jdbcUrl());
        hikari.setUsername(config.username());
        hikari.setPassword(config.password());
        hikari.setMaximumPoolSize(Math.max(2, poolSize));
        hikari.setPoolName("scheduler");
        hikari.setConnectionTimeout(2_000);
        this.dataSource = new HikariDataSource(hikari);
    }

    public static Database connect(Config config, int poolSize) throws SQLException, InterruptedException {
        SQLException last = null;
        Database database = new Database(config, poolSize);
        for (int attempt = 1; attempt <= 30; attempt++) {
            try (Connection connection = database.connection();
                 Statement statement = connection.createStatement()) {
                statement.execute("SELECT 1");
                return database;
            } catch (SQLException e) {
                last = e;
                if (attempt == 30) {
                    database.close();
                    break;
                }
                System.out.println("waiting for postgres (" + attempt + "/30): " + e.getMessage());
                Thread.sleep(1_000);
            }
        }
        throw last;
    }

    public Connection connection() throws SQLException {
        return dataSource.getConnection();
    }

    @Override
    public void close() {
        dataSource.close();
    }
}
