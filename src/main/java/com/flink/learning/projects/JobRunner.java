package com.flink.learning.projects;

import java.util.List;
import java.util.NoSuchElementException;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;


@Component
public class JobRunner implements CommandLineRunner {

    @Value("${job.name}")
    private String jobName;

    private final List<FlinkJob> jobs;

    public JobRunner(List<FlinkJob> jobs) {
        this.jobs = jobs;
    }

    @Override
    public void run(String... args) throws Exception {
        jobs.stream()
            .filter(job -> job.name().equals(jobName))
            .findFirst()
            .orElseThrow(() -> new NoSuchElementException(
                "No FlinkJob bean found for job.name='" + jobName + "'. Available jobs: " +
                jobs.stream().map(FlinkJob::name).toList()))
            .run();
    }
}
