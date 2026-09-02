package com.flink.learning.projects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

public class ProjectsApplication {
    private static final Logger logger = LoggerFactory.getLogger(ProjectsApplication.class);
    public static void main(String[] args) throws Exception {

        logger.info("Main started");

        try (AnnotationConfigApplicationContext context =
                     new AnnotationConfigApplicationContext(AppConfig.class)) {
            logger.info("Context initialized");
            context.getBean(JobRunner.class).run(args);
        }
    }
}