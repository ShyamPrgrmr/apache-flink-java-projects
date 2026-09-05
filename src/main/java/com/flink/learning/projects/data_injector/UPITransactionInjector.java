package com.flink.learning.projects.data_injector;

import java.util.ArrayList;

import com.flink.learning.projects.data_injector.models.UPIPayment;

public class UPITransactionInjector {

    public static ArrayList<UPIPayment> createTestPayments() {
        ArrayList<UPIPayment> payments = new ArrayList<>();

        long now = System.currentTimeMillis() / 1000;

        // User 1 - should cross LOW and MEDIUM thresholds
        payments.add(new UPIPayment("user1@example.com", "to-user1@example.com", 300, now));
        payments.add(new UPIPayment("user1@example.com", "to-user1@example.com", 400, now + 1));
        payments.add(new UPIPayment("user1@example.com", "to-user1@example.com", 500, now + 2));

        // Running total = 1200 (threshold 1000)

        // User 2 - independent keyed state
        payments.add(new UPIPayment("user2@example.com", "to-user1@example.com", 700, now + 3));
        payments.add(new UPIPayment("user2@example.com", "to-user1@example.com", 600, now + 4));

        // Running total = 1300

        // More transactions for User 1 (tests ListState)
        payments.add(new UPIPayment("user1@example.com", "to-user1@example.com", 1000, now + 5));
        payments.add(new UPIPayment("user1@example.com", "to-user1@example.com", 1200, now + 6));
        payments.add(new UPIPayment("user1@example.com", "to-user1@example.com", 1800, now + 7));

        // User 1 total = 5200 (HIGH threshold)

        return payments;
    }
}
