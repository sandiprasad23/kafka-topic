package com.sample.kafka.apachekafkaproducerdemo.Consumer;

import com.google.gson.Gson;
import com.sample.kafka.apachekafkaproducerdemo.Model.UserActions;
import org.apache.http.HttpHost;

import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.common.settings.Settings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;

/**
 * KafkaConsumer class to receive json messages from producer
 */
@Component
public class KafkaConsumer {

   private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConsumer.class);

    /**
     *
     * @return client
     */
   @Bean
   public RestHighLevelClient client() {
       final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
       credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials("username", "password"));
       RestClientBuilder builder = RestClient.builder(new HttpHost("localhost", 9200, "http")).
               setHttpClientConfigCallback(httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider));
       RestHighLevelClient client = new RestHighLevelClient(builder);
       return client;
   }

    /**
     *
     * @param message
     * @throws Exception
     */
   @KafkaListener(topics = "NewTopic", groupId = "group_id_new")
   public void consume(String message) throws Exception{
       LOGGER.info("message = " + message);
       Gson gson = new Gson();
       UserActions userActions = gson.fromJson(message, UserActions.class);
       LOGGER.info("User Actions" + userActions.getEvent_type());
       try {
           Class.forName("com.mysql.jdbc.Driver");
           Connection connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/kafka_topic", "root", "root");
           PreparedStatement preparedStatement = connection.prepareStatement("select * from user_actions where event_type=?");
           preparedStatement.setString(1, userActions.getEvent_type());
           ResultSet resultSet = preparedStatement.executeQuery();
           while (resultSet.next()) {
               LOGGER.info("Result Set" + resultSet);
               LOGGER.info(resultSet.getString(2) + " is already present in DB");
               CreateIndexRequest createIndexRequest = new CreateIndexRequest("user_action");
               Map<String, Object> mapping  = new HashMap<>();
               mapping.put("user_actions", userActions);
               createIndexRequest.settings(Settings.builder()
                       .put("index.number_of_shards", 1)
                       .put("index.number_of_replicas", 2)).source(mapping);
               CreateIndexResponse createIndexResponse = client().indices().create(createIndexRequest, RequestOptions.DEFAULT);
               LOGGER.info("Created Request" + createIndexResponse.toString());
               LOGGER.info(" Created response" + createIndexResponse);
               String index = createIndexResponse.index();
               LOGGER.info("Inside Elastic search and index value" + index);
           }
           connection.close();
       } catch (Exception e) {
           LOGGER.error("Exception" + e);
       }

   }
}
