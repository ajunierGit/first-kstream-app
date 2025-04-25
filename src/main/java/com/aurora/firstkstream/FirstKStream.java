package com.aurora.firstkstream;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.List;
import java.util.Properties;

public class FirstKStream {

    public static final short REPLICATION_FACTOR = 1;
    public static final int PARTITIONS = 6;

    public static void main(String[] args) {
        System.out.println("Starting up");

        // Config
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "first-kstream-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, System.getenv().getOrDefault("BOOTSTRAP_SERVERS", "localhost:9092"));
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());


        final String inputTopic = "aurora.firstkstream.inputTopic";
        final String outputTopic = "aurora.firstkstream.outputTopic";

        // Create topics
        System.out.println("Creating topics");
        Properties config = new Properties();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
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

        // Stream topology
        System.out.println("Build KStream");
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> inputStream = builder.stream(inputTopic);
        inputStream.peek((key, value) -> System.out.println("Message read - key " + key + " value " + value));
        inputStream.to(outputTopic);

        // Start the stream
        KafkaStreams stream = new KafkaStreams(builder.build(), props);
        stream.start();

        // Prevent app from exiting
        try {
            System.out.println("Sleep time...");
            Thread.sleep(Long.MAX_VALUE);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // Shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("Shutting down streams app...");
                stream.close();
            })
        );
    }
}
