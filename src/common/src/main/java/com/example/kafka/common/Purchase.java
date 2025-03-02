package com.example.kafka.common;

import java.io.Serializable;

/**
 * Represents a product purchase with customer information, product details, and pricing.
 */
public class Purchase implements Serializable {
    private String orderId;
    private String customerId;
    private String sku;
    private int quantity;
    private double unitPrice;
    private double extendedPrice;

    // Default constructor for serialization
    public Purchase() {
    }

    public Purchase(String orderId, String customerId, String sku, int quantity, double unitPrice) {
        this.orderId = orderId;
        this.customerId = customerId;
        this.sku = sku;
        this.quantity = quantity;
        this.unitPrice = unitPrice;
        this.extendedPrice = quantity * unitPrice;
    }

    // Getters and setters
    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getCustomerId() {
        return customerId;
    }

    public void setCustomerId(String customerId) {
        this.customerId = customerId;
    }

    public String getSku() {
        return sku;
    }

    public void setSku(String sku) {
        this.sku = sku;
    }

    public int getQuantity() {
        return quantity;
    }

    public void setQuantity(int quantity) {
        this.quantity = quantity;
        // Update extended price when quantity changes
        this.extendedPrice = this.quantity * this.unitPrice;
    }

    public double getUnitPrice() {
        return unitPrice;
    }

    public void setUnitPrice(double unitPrice) {
        this.unitPrice = unitPrice;
        // Update extended price when unit price changes
        this.extendedPrice = this.quantity * this.unitPrice;
    }

    public double getExtendedPrice() {
        return extendedPrice;
    }

    public void setExtendedPrice(double extendedPrice) {
        this.extendedPrice = extendedPrice;
    }

    @Override
    public String toString() {
        return "Purchase{" +
                "orderId='" + orderId + '\'' +
                ", customerId='" + customerId + '\'' +
                ", sku='" + sku + '\'' +
                ", quantity=" + quantity +
                ", unitPrice=" + unitPrice +
                ", extendedPrice=" + extendedPrice +
                '}';
    }
}
