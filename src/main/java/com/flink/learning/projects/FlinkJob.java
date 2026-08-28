package com.flink.learning.projects;

public interface FlinkJob {

    String name();
    void run() throws Exception;
}
