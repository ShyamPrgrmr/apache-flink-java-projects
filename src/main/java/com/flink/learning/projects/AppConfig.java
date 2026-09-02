package com.flink.learning.projects;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;


@Configuration
@ComponentScan("com.flink.learning.projects")
@PropertySource("classpath:application.properties")
@PropertySource(
    value = "classpath:application-${env:local}.properties",
    ignoreResourceNotFound = true
)
public class AppConfig {
    @Bean
    public static PropertySourcesPlaceholderConfigurer propertyConfigurer() {
        return new PropertySourcesPlaceholderConfigurer();
    }
}