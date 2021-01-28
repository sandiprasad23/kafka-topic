package com.sample.kafka.apachekafkaproducerdemo.Controller;

import com.sample.kafka.apachekafkaproducerdemo.Model.UserActions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * Controller to demonstrate publishing kafka message from producer
 */
@RestController
public class DemoController {

    private static final Logger LOGGER = LoggerFactory.getLogger(DemoController.class);

    @Autowired
    KafkaTemplate<String, UserActions> kafkaTemplate;

    private static final String TOPIC="NewTopic";

    /**
     *
     * @param message
     * @return String
     */
    @PostMapping("/publish")
    public String publishMessage(@RequestBody UserActions message) throws Exception{
        try {
            LOGGER.info("Producer Message and Topic" + message + ", " + TOPIC);
            kafkaTemplate.send(TOPIC, message);
            return "Published Successfully";
        } catch (Exception e) {
            LOGGER.error("Could not complete publishing");
            return  null;
        }
    }
}
