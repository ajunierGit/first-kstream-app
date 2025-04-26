package com.aurora.firstkstream;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.io.IOException;
import java.util.Properties;

public class FirstKStream {

    public static void main(String[] args) {
        System.out.println("Starting up");

        // KafkaStream Configuration
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "first-kstream-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, System.getenv().getOrDefault("BOOTSTRAP_SERVERS", "localhost:9092"));
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        Properties properties;
        String inputTopic = ""; 
        String outputTopic = ""; 
        try {
            properties = Utils.loadProperties();
            inputTopic = properties.getProperty("input.topic");
            outputTopic = properties.getProperty("output.topic");
        } catch (IOException ioe){
            ioe.printStackTrace();
        }
         
        // Stream topology
        System.out.println("Build KStream");
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> inputStream = builder.stream(inputTopic);
        inputStream.peek((key, value) -> System.out.println("Message read - key " + key + " value " + value));
        inputStream.to(outputTopic);

        // Start the stream
        KafkaStreams stream = new KafkaStreams(builder.build(), props);
        stream.setStateListener((newState, oldState) -> {
            System.out.println("State changed from " + oldState + " to " + newState);
        });
        
        stream.start();

        // Register Shutdown hook for graceful shutdowns
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down streams app...");
            stream.close();
            })
        );

        // Start Producer
        try {
            TopicProducer.runProducer();
        } catch (IOException ioe){
            System.out.println("Encountered error when running the Producer. Exception: "+ioe.toString());
            ioe.printStackTrace();
        }

        // Prevent app from exiting
        try {
            System.out.println("Sleep time...");
            Thread.sleep(Long.MAX_VALUE);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
