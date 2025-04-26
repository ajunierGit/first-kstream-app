package com.aurora.firstkstream;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;

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
         
        // *** 
        // Define KafkaStreams topology
        System.out.println("Build KStream");
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> inputStream = builder.stream(inputTopic);
        inputStream.peek((key, value) -> System.out.println("Message read - key " + key + " value " + value));
        inputStream.to(outputTopic);


        // *** 
        // Start Kafka Producer
        try {
            TopicProducer.runProducer();
        } catch (IOException ioe){
            System.out.println("Encountered error when running the Producer. Exception: "+ioe.toString());
            ioe.printStackTrace();
        }

        // *** 
        // Start the KafkaStream application and handle shutdown and errors
        // Start the streams
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        CountDownLatch latch = new CountDownLatch(1);

        streams.setStateListener((newState, oldState) -> {
            System.out.println("State changed from " + oldState + " to " + newState);
        });

        // Register Shutdown hook for graceful shutdowns
        // The Hook close the stream and releases the latch
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down streams app...");
            streams.close();
            latch.countDown();
        }));

        // Handle uncaught exceptions in stream threads
        streams.setUncaughtExceptionHandler(ex -> {
            System.out.println("Uncaught error in thread " + ex.getMessage());
            ex.printStackTrace();
            streams.close();
            latch.countDown();
            return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_APPLICATION;
        });

        // Start KafkaStream app and keep alive until latch is released
        try {
            streams.start();
            latch.await();  // Block here until shutdown is requested
        } catch (Throwable e) {
            System.out.println("Fatal error: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);  // Non-zero exit on error
        }
        System.exit(0);  // Clean exit

    }
}
