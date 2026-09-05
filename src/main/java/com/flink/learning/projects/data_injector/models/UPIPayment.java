package com.flink.learning.projects.data_injector.models;

import java.io.Serializable;

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
public class UPIPayment implements Serializable {
    private String handle;  // user Handle
    private String toHandle;  // complete handle name
    private double amount;  // transaction amount
    private double timestamp;  // transaction timestamp

    @Override 
    public String toString() {
        return "UPIPayment = [" +
                "handle='" + handle + '\n' +
                ", toHandle='" + toHandle + '\n' +
                ", amount=" + amount + '\n'+
                ", timestamp=" + timestamp + '\n' + ']'; 
    }

}
