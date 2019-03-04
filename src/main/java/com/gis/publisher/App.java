package com.gis.publisher;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.InputStream;
import java.net.URI;
import java.nio.file.*;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

public class App {
    //default values
    private static String brokers = "localhost:9092";
    private static String topic = "gis-uk-crime-demo";
    private static String inputFilePath = "2018-12-city-of-london-street.csv";

    public static void main(String... args) throws Exception {
        if (args.length == 3) {
            brokers = args[0];
            topic = args[1];
            inputFilePath = args[2];
        }
        App publisher = new App();
        publisher.runPublisher();

    }

    /*
     * create a topic using kafka command-line utility
     *
     * bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 3 --topic gis-uk-crime-demo
     *
     * */
    private static KafkaProducer<String, String> createProducer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", brokers);
        props.put("client.id", "GisPublisher");
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        return new KafkaProducer<>(props);
    }

    private void runPublisher() throws Exception {
        final KafkaProducer<String, String> producer = createProducer();
        // Latch to make sure all the records are published asynchronously using the callback mechanism
        final CountDownLatch countDownLatch = new CountDownLatch(1);

        try {
            //To avoid FileSystemNotFoundException which isn't even an IOException
            //Initialize newFileSystem before Path.get
            URI uri = this.getClass().getClassLoader().getResource(inputFilePath).toURI();
            FileSystem fs = FileSystems.newFileSystem(uri, Collections.emptyMap());

            // Read the input file as a stream of lines
            Path path = Paths.get(uri);
            Stream<String> dataFileStream = Files.lines(path);

            long start = System.currentTimeMillis();
            // Convert each line of record to a Kafka Producer Record
            // Generate a Random UUID for the key. Value is line of loan record in CSV format
            dataFileStream.forEach(line -> {

                final ProducerRecord<String, String> gisRecord =
                        new ProducerRecord<String, String>(topic, UUID.randomUUID().toString(), line);

                // Adding some delay to allow the records to stream for few seconds
                try {
                    Thread.currentThread().sleep(0, 1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                // Send the loan record to Kafka Broker in Async mode. Callback is called after the record receiving the acknowledgement from broker
                producer.send(gisRecord, ((metadata, exception) -> {

                    if (metadata != null) {
                        System.out.println("GIS Data Event Sent --> " + gisRecord.key() + " | "
                                + gisRecord.value() + " | " + metadata.partition());
                    } else {
                        System.out.println("Error Sending Loan Data Event --> " + gisRecord.value());
                    }
                }));
            });

            try {

                long end = System.currentTimeMillis();
                // Wait for 10 seconds to get any pending records processed before proceeding further with the processing
                countDownLatch.await(10, TimeUnit.SECONDS);
                System.out.println("Published all the gis Records to Kafka Broker!");
                System.out.println("Time Taken to publish gis data " + ((end - start) / 1000) + " seconds");
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                dataFileStream.close();
                fs.close();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}