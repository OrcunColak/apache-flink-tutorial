package com.colak.kafkaconsumer;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.json.JSONObject;

import java.time.Duration;
import java.util.Properties;

// This example demonstrates how to consume events from Kafka, enrich the data, and generate alerts based on the processed data, all within a real-time event processing pipeline using Flink.
public class RealTimeEventProcessing {

    public static void main() throws Exception {
        // Set up execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setAutoWatermarkInterval(1000);

        // Configure Kafka consumer
        Properties kafkaProperties = new Properties();
        kafkaProperties.setProperty("bootstrap.servers", "localhost:9092");
        kafkaProperties.setProperty("group.id", "flink-group");

        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                "events",
                new SimpleStringSchema(),
                kafkaProperties
        );

        // Set up the data stream
        DataStream<String> stream = env.addSource(kafkaConsumer)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                        .withTimestampAssigner((event, timestamp) -> {
                            JSONObject json = new JSONObject(event);
                            return json.getLong("timestamp");
                        })
                );

        // Enrich the stream
        DataStream<String> enrichedStream = stream.map(event -> {
            JSONObject json = new JSONObject(event);
            String sensorId = json.getString("sensor_id");
            String location = getSensorLocation(sensorId);

            // Convert to fahrenheit
            double temperatureFahrenheit = json.getDouble("temperature") * 1.8 + 32;

            return new JSONObject()
                    .put("sensor_id", sensorId)
                    .put("location", location) // Enriched field
                    .put("temperature_Fahrenheit", temperatureFahrenheit) // Converted field
                    .put("timestamp", json.getLong("timestamp"))
                    .toString();
        });

        // Process alerts
        DataStream<String> alertStream = enrichedStream
                .keyBy(event -> {
                    JSONObject json = new JSONObject(event);
                    return json.getString("sensor_id");
                })
                .process(new TemperatureAlertFunction());

        // Print the alerts
        alertStream.print();

        // Execute the Flink job
        env.execute("Real-Time Event Processing");
    }

    // TemperatureAlertFunction definition
    public static class TemperatureAlertFunction extends KeyedProcessFunction<String, String, String> {
        @Override
        public void processElement(String value, Context ctx, Collector<String> out) {
            JSONObject json = new JSONObject(value);
            double temperatureFahrenheit = json.getDouble("temperature_Fahrenheit");
            if (temperatureFahrenheit > 80) {
                out.collect("Alert: High temperature " + temperatureFahrenheit + "F at sensor " + json.getString("sensor_id"));
            }
        }
    }

    // Function to get sensor location
    public static String getSensorLocation(String sensorId) {
        // External function to get sensor location
        // For demonstration purposes, return a static location
        return "Room 101";
    }
}

