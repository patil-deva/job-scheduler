package com.scheduler;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class Main {
    private Main() {
    }

    public static void main(String[] args) {
        try {
            if (args.length > 0 && "worker".equals(args[0])) {
                runWorker(args);
                return;
            }
            System.exit(run(args));
        } catch (IllegalArgumentException e) {
            System.out.println(e.getMessage());
            System.out.println(usage());
            System.exit(1);
        } catch (Exception e) {
            e.printStackTrace(System.out);
            System.exit(1);
        }
    }

    private static int run(String[] args) throws Exception {
        if (args.length == 0) {
            throw new IllegalArgumentException("missing command");
        }
        Config config = Config.fromEnv();
        Map<String, String> flags = flags(args, 1);
        return switch (args[0]) {
            case "migrate" -> {
                try (Database database = Database.connect(config, 2)) {
                    Migrator.migrate(database);
                }
                yield 0;
            }
            case "reset" -> {
                try (Database database = Database.connect(config, 2)) {
                    Migrator.migrate(database);
                    new JobRepository(database).reset();
                }
                System.out.println("jobs cleared");
                yield 0;
            }
            case "enqueue" -> {
                String org = required(flags, "org");
                String type = required(flags, "type");
                int count = Integer.parseInt(required(flags, "count"));
                int delay = Integer.parseInt(flags.getOrDefault("delay-seconds", "0"));
                int maxAttempts = Integer.parseInt(flags.getOrDefault("max-attempts", Integer.toString(config.maxAttempts())));
                String payload = flags.getOrDefault("payload", "");
                if (count < 1) {
                    throw new IllegalArgumentException("--count must be positive");
                }
                try (Database database = Database.connect(config, 2)) {
                    Migrator.migrate(database);
                    int inserted = new JobRepository(database).enqueue(org, type, payload, count, delay, maxAttempts);
                    System.out.println("enqueued " + inserted + " " + type + " jobs for " + org);
                }
                yield 0;
            }
            case "report" -> {
                boolean wait = flags.containsKey("wait");
                int timeout = Integer.parseInt(flags.getOrDefault("timeout-seconds", "180"));
                try (Database database = Database.connect(config, 2)) {
                    yield new Reporter(database, config).run(wait, timeout);
                }
            }
            default -> throw new IllegalArgumentException("unknown command: " + args[0]);
        };
    }

    private static void runWorker(String[] args) throws Exception {
        Config config = Config.fromEnv();
        Map<String, String> flags = flags(args, 1);
        String workerId = flags.getOrDefault("id", defaultWorkerId());
        int threads = Integer.parseInt(flags.getOrDefault("threads", Integer.toString(config.workerThreads())));
        boolean claimAndExit = flags.containsKey("claim-and-exit");
        List<String> types = List.of();
        if (flags.containsKey("types")) {
            types = Arrays.stream(flags.get("types").split(","))
                    .map(String::trim)
                    .filter(type -> !type.isEmpty())
                    .toList();
        }
        if (threads < 1) {
            throw new IllegalArgumentException("--threads must be positive");
        }
        try (Database database = Database.connect(config, threads + 8)) {
            Migrator.migrate(database);
            Consumer consumer = new Consumer(
                    new JobRepository(database),
                    Handlers.demo(),
                    config,
                    workerId,
                    threads,
                    claimAndExit,
                    types);
            Thread loop = new Thread(consumer::loop, "consumer");
            loop.start();
            Runtime.getRuntime().addShutdownHook(new Thread(consumer::stop, "shutdown"));
            loop.join();
        }
    }

    private static String defaultWorkerId() {
        String host = "worker";
        try {
            host = InetAddress.getLocalHost().getHostName();
        } catch (Exception ignored) {
            // Fall back to a generic name. The process id still makes the worker unique.
        }
        return host + "-" + ProcessHandle.current().pid();
    }

    private static Map<String, String> flags(String[] args, int start) {
        Map<String, String> flags = new LinkedHashMap<>();
        for (int i = start; i < args.length; i++) {
            String arg = args[i];
            if (!arg.startsWith("--") || arg.length() < 3) {
                throw new IllegalArgumentException("unexpected argument: " + arg);
            }
            String name = arg.substring(2);
            if (i + 1 < args.length && !args[i + 1].startsWith("--")) {
                flags.put(name, args[++i]);
            } else {
                flags.put(name, "true");
            }
        }
        return flags;
    }

    private static String required(Map<String, String> flags, String name) {
        String value = flags.get(name);
        if (value == null) {
            throw new IllegalArgumentException("missing --" + name);
        }
        return value;
    }

    private static String usage() {
        return """
                Usage:
                  migrate
                  reset
                  enqueue --org ID --type TYPE --count N [--delay-seconds N] [--max-attempts N]
                  worker [--id ID] [--threads N] [--claim-and-exit] [--types fast,slow]
                  report [--wait] [--timeout-seconds N]
                """;
    }
}
