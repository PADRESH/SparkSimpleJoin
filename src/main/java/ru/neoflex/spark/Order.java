package ru.neoflex.spark;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.LocalDate;

public class Order implements Serializable {
    public String getNumber() {
        return number;
    }

    public void setNumber(String number) {
        this.number = number;
    }

    public BigDecimal getAmount() {
        return amount;
    }

    public void setAmount(BigDecimal amount) {
        this.amount = amount;
    }

    public String getClient() {
        return client;
    }

    public void setClient(String client) {
        this.client = client;
    }

    public LocalDate getDate() {
        return date;
    }

    public void setDate(LocalDate date) {
        this.date = date;
    }

    String number;
    BigDecimal amount;
    String client;
    LocalDate date;

    public Order(String number, BigDecimal amount, String client, LocalDate date) {
        this.number = number;
        this.client = client;
        this.amount = amount;
        this.date = date;
    }
}
