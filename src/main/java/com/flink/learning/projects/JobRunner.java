package com.flink.learning.projects;

import java.util.List;
import java.util.NoSuchElementException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;


@Component
public class JobRunner{
    private static final Logger logger = LoggerFactory.getLogger(JobRunner.class);

    @Value("${job.name}")
    private String jobName;

    private final List<FlinkJob> jobs;

     public JobRunner(List<FlinkJob> jobs) {
        this.jobs = jobs;
    }

    public void run(String... args) throws Exception {

        logger.info("Running job with name: {}", jobName);

        jobs.stream()
            .filter(job -> job.name().equals(jobName))
            .findFirst()
            .orElseThrow(() -> {return new NoSuchElementException(
                    "No FlinkJob bean found for job.name='" + jobName + "'");})
            .run();
    }
}
