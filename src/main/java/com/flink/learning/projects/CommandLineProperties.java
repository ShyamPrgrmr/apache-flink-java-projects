package com.flink.learning.projects;


import java.util.Arrays;
import java.util.Properties;

public final class CommandLineProperties {

    private CommandLineProperties() {}

    public static Properties fromArgs(String[] args) {
        Properties properties = new Properties();

        Arrays.stream(args)
                .filter(arg -> arg.startsWith("--") && arg.contains("="))
                .forEach(arg -> {
                    String[] parts = arg.substring(2).split("=", 2);
                    properties.setProperty(parts[0], parts[1]);
                });

        return properties;
    }
}