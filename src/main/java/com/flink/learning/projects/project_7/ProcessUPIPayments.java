package com.flink.learning.projects.project_7;

import java.sql.Timestamp;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import com.flink.learning.projects.data_injector.models.Alert;
import com.flink.learning.projects.data_injector.models.UPIPayment;
import com.flink.learning.projects.data_injector.models.UPIThreshould;

public class ProcessUPIPayments extends KeyedProcessFunction<String, UPIPayment, Alert> {

    private MapState<String, Double> handlesTotals;
    private ListState<UPIPayment> last5Transactions;
    private ReducingState<Double> reducedTransactionAmount;

    private ArrayList<UPIThreshould> thresholds;

    public ProcessUPIPayments(ArrayList<UPIThreshould> thresholds) {
        this.thresholds = thresholds;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        handlesTotals = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("handles-totals", String.class, Double.class));

        last5Transactions = getRuntimeContext().getListState(
                new ListStateDescriptor<>("last-5-transactions", UPIPayment.class));

        reducedTransactionAmount = getRuntimeContext().getReducingState(
                new ReducingStateDescriptor<>(
                        "reduced-transaction-amount",
                        Double::sum,
                        Double.class));

        if (thresholds == null) {
            thresholds = new ArrayList<>();
        }
    }

    private UPIThreshould checkThreshold(String handle) {
        return thresholds.stream()
                .filter(t -> t.getToHandle().equals(handle.trim()))
                .findFirst()
                .orElse(null);
    }

    @Override
    public void processElement(
            UPIPayment value,
            Context ctx,
            Collector<Alert> out) throws Exception {

        UPIThreshould threshold = checkThreshold(value.getToHandle());

        // -------- MapState: Running total per receiver --------
        Double currentAmount = handlesTotals.get(value.getToHandle());

        if (currentAmount == null) {
            currentAmount = value.getAmount();
        } else {
            currentAmount += value.getAmount();
        }

        handlesTotals.put(value.getToHandle(), currentAmount);

        // -------- ListState: Keep last 5 transactions --------
        ArrayList<UPIPayment> recentTransactions = new ArrayList<>();

        for (UPIPayment tx : last5Transactions.get()) {
            recentTransactions.add(tx);
        }

        if (recentTransactions.size() >= 5) {
            recentTransactions.remove(0);
        }

        recentTransactions.add(value);
        last5Transactions.update(recentTransactions);

        // -------- ReducingState: Running total amount --------
        reducedTransactionAmount.add(value.getAmount());

        // -------- Alert generation --------
        if (threshold != null && currentAmount > threshold.getValue()) {

            Alert alert = new Alert();

            alert.setMessage(
                    "Threshold exceeded for handle: "
                            + value.getToHandle()
                            + ", current amount: "
                            + currentAmount
                            + ", threshold: "
                            + threshold.getValue());

            alert.setTimestamp(new Timestamp(Math.round(value.getTimestamp() * 1000)).toLocalDateTime().format(DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss")));

            alert.setSeverity(threshold.getSeverity().toString());

            for (UPIPayment tx : last5Transactions.get()) {
                alert.getLast5Transactions().add(tx);
            }

            alert.setTotalTransactions(reducedTransactionAmount.get());

            out.collect(alert);
        }
    }
}