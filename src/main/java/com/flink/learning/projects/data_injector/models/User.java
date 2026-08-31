package com.flink.learning.projects.data_injector.models;

import java.io.Serializable;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
@ToString
public class User implements Serializable {
    private static final long serialVersionUID = 1L;
    private String userId;
    private String name;
    private Integer count;

    public User(String userId, String name) {
        this.userId = userId;
        this.name = name;
        this.count = 0;
    }

}