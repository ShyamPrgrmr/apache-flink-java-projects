package com.flink.learning.projects.data_injector.models;
import java.io.Serializable;
import java.util.UUID;

import com.flink.learning.projects.data_injector.constants.Severity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;


@Data 
@Getter 
@Setter 
@AllArgsConstructor 
@NoArgsConstructor 
public class UPIThreshould implements Serializable {
    private UUID thresholdId;   // unique identifier for the threshold
    private String toHandle;    // user who last updated the threshold
    private Double value;       // threshold value
    private String description; // description of the threshold
    private Severity severity;  // severity level of the threshold
}