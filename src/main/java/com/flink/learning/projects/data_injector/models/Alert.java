package com.flink.learning.projects.data_injector.models;

import java.io.Serializable;
import java.util.ArrayList;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Data
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class Alert implements Serializable {
    private static final long serialVersionUID = 1L;
    private String message;
    private String severity;
    private String timestamp;
    private ArrayList<UPIPayment> last5Transactions = new ArrayList<>();
    private Double totalTransactions;
}