package com.aurora.firstkstream;

import java.io.InputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

public class Utils {
    public static final String PROPERTIES_FILE_NAME = "streams.properties";
    public static final short REPLICATION_FACTOR = 1;
    public static final int PARTITIONS = 6;

    public static Properties loadProperties() throws IOException {
    Properties props = new Properties();
    try (InputStream input = FirstKStream.class.getClassLoader().getResourceAsStream(PROPERTIES_FILE_NAME)) {
        if (input == null) {
            throw new FileNotFoundException("Property file '" + PROPERTIES_FILE_NAME + "' not found in the classpath");
        }
        props.load(input);
    }
    return props;
}
}
