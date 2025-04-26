package com.aurora.firstkstream;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;


public class TopicProducer {

    public static final short REPLICATION_FACTOR = 1;
    public static final int PARTITIONS = 6;
    
    public static void runProducer() throws IOException {

        final String inputTopic = "aurora.firstkstream.inputTopic";
        final String outputTopic = "aurora.firstkstream.outputTopic";

        // Create topics
        System.out.println("Creating topics");
        Properties config = new Properties();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        System.out.println("Starting admin client");
        try(Admin adminClient = Admin.create(config)){
            System.out.println("Trying to create topics..");
            List<NewTopic> topics = List.of(
                new NewTopic(inputTopic, PARTITIONS, REPLICATION_FACTOR),
                new NewTopic(outputTopic, PARTITIONS, REPLICATION_FACTOR));
            // Call .all().get() to wait for all topics to be created. 
            // This also returns exceptions in case of problems which helps for debugging
            adminClient.createTopics(topics).all().get();
            System.out.println("Topics created (or already exist)");
        } catch (Exception e){
            System.out.println("Error during topics creation..");
            e.printStackTrace();
        }

        // Emit data to input topic
        List<String> rawRecords = List.of("orderNumber-1001",
            "orderNumber-5000",
            "orderNumber-999",
            "orderNumber-3330",
            "bogus-1",
            "bogus-2",
            "orderNumber-8400");
        
        System.out.println("Starting Producer..");
        Producer<String, String> producer = new KafkaProducer<>(config);
        List<ProducerRecord<String, String>> producerRecords = rawRecords
            .stream()
            .map(r -> new ProducerRecord<>(inputTopic,"mykey", r))
            .collect(Collectors.toList());
        Callback producerCallback = (metadata, exception) -> {
            if(exception != null) {
                System.out.printf("Producing records encountered error %s %n", exception);
            } else {
                System.out.printf("Record produced - offset - %d timestamp - %d %n", metadata.offset(), metadata.timestamp());
            }

        };
        producerRecords.forEach((record -> producer.send(record, producerCallback)));
    }
}
