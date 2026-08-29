package com.flink.learning.projects.data_injector.models;

import java.io.Serializable;

public class User implements Serializable {

    private static final long serialVersionUID = 1L;

    private String userId;
    private String name;
    private Integer count;

    public User() {
        // Required by Flink POJO serializer
    }

    public User(String userId, String name) {
        this.userId = userId;
        this.name = name;
        this.count = 0;
    }

    public Integer getCount() { return count; }
    public void setCount(Integer count) { this.count = count; }

    public String getUserId() { return userId; }
    public void setUserId(String userId) { this.userId = userId; }

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    @Override
    public String toString() {
        return "User{" +
                "userId='" + userId + '\'' +
                ", name='" + name + '\'' +
                '}';
    }
}