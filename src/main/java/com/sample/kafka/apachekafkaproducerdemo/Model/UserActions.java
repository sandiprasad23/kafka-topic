package com.sample.kafka.apachekafkaproducerdemo.Model;

import java.math.BigInteger;
import java.util.Map;


public class UserActions {

    private String event_type;
    private BigInteger timestamp;
    private Map<String, Object> payload;

    public String getEvent_type() {
        return event_type;
    }

    public void setEvent_type(String event_type) {
        this.event_type = event_type;
    }

    public BigInteger getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(BigInteger timestamp) {
        this.timestamp = timestamp;
    }

    public Map<String, Object> getPayload() {
        return payload;
    }

    public void setPayload(Map<String, Object> payload) {
        this.payload = payload;
    }
}
