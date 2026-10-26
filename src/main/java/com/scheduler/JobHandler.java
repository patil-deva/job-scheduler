package com.scheduler;

@FunctionalInterface
public interface JobHandler {
    void execute(Job job, JobContext context) throws Exception;
}
