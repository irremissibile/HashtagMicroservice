package hashtags.demo;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import io.vertx.kafka.client.producer.RecordMetadata;

import java.time.LocalDateTime;
import java.util.*;

public class Client {

    public static final String configPath = "conf/config.json";

    private static KafkaConsumer<String, JsonArray> mJsonConsumer;

    private static LocalDateTime time1;
    private static LocalDateTime time2;

    public static void main(String[] args) {
        setupTestListener();

        Map<String, String> config = new HashMap<>();
        config.put("bootstrap.servers", "localhost:9092");

        config.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        config.put("value.serializer", "io.vertx.kafka.client.serialization.JsonObjectSerializer");

        config.put("acks", "1");



        KafkaProducer<String, JsonObject> mProducer = KafkaProducer.create(Vertx.vertx(), config);


        //String description = "Eto ya na #kontsert #PavloZibrov, tyt tak #zashibenno, hobbiti must die";
        //String description = "Eto #Pasha luchij v #mire #bog, kak zhe ya #love #microservices";
        String description = "Space spice #girls love, ya tak zhe ka i #vlad ochen silno #zadolbalsya";
        JsonObject json = new JsonObject()
                .put("id", "59")
                .put("desc", description);

        KafkaProducerRecord<String, JsonObject> record =
                KafkaProducerRecord.create("createtopic", json);

        mProducer.send(record, done -> {
            if (done.succeeded()) {
                RecordMetadata recordMetadata = done.result();
                System.out.println("Message " + record.value() + " written on topic=" + recordMetadata.getTopic() +
                        ", partition=" + recordMetadata.getPartition() +
                        ", offset=" + recordMetadata.getOffset());
            } else {
                System.out.println(done.cause());
            }
        });



        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            System.out.println(e);
        }




        String hashtag = "zadolbalsya";
        JsonObject json2 = new JsonObject()
                .put("hashtag", hashtag);
        KafkaProducerRecord<String, JsonObject> record2 =
                KafkaProducerRecord.create("fetchtopic", json2);

        time1 = LocalDateTime.now();
        /*mProducer.send(record2, done -> {
            if (done.succeeded()) {
                RecordMetadata recordMetadata = done.result();
                System.out.println("Message " + record2.value() + " written on topic=" + recordMetadata.getTopic() +
                        ", partition=" + recordMetadata.getPartition() +
                        ", offset=" + recordMetadata.getOffset());
            } else {
                System.out.println(done.cause());
            }
        });*/



        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            System.out.println(e);
        }


        JsonObject json3 = new JsonObject()
                .put("id", "67");
        KafkaProducerRecord<String, JsonObject> record3 =
                KafkaProducerRecord.create("deletetopic2", json3);

        time1 = LocalDateTime.now();
        mProducer.send(record3, done -> {
            if (done.succeeded()) {
                RecordMetadata recordMetadata = done.result();
                System.out.println("Message " + record3.value() + " written on topic=" + recordMetadata.getTopic() +
                        ", partition=" + recordMetadata.getPartition() +
                        ", offset=" + recordMetadata.getOffset());
            } else {
                System.out.println(done.cause());
            }
        });
    }




    private static void setupTestListener() {
        Map<String, String> config = new HashMap<>();
        config.put("bootstrap.servers", "localhost:9092");

        config.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        config.put("value.deserializer", "io.vertx.kafka.client.serialization.JsonArrayDeserializer");

        config.put("auto.offset.reset", "earliest");
        config.put("enable.auto.commit", "true");
        config.put("group.id", "test-consumer-group");

        mJsonConsumer = KafkaConsumer.create(Vertx.vertx(), config);

        /*mJsonConsumer.partitionsAssignedHandler(topicPartitions -> {

            System.out.println("Partitions assigned");
            for (TopicPartition topicPartition : topicPartitions) {
                System.out.println(topicPartition.getTopic() + " " + topicPartition.getPartition());
            }
        });

        mJsonConsumer.partitionsRevokedHandler(topicPartitions -> {

            System.out.println("Partitions revoked");
            for (TopicPartition topicPartition : topicPartitions) {
                System.out.println(topicPartition.getTopic() + " " + topicPartition.getPartition());
            }
        });*/


        Set<String> topics = new HashSet<>();
        topics.add("requests2");

        mJsonConsumer.subscribe(topics, ar -> {
            if (ar.succeeded()) {
                System.out.println("Consumer subscribed");
            }
            else
                System.out.println("Could not subscribe");
        });

        mJsonConsumer.<JsonArray>handler(record -> {
            time2 = LocalDateTime.now();
            System.out.println("time1: " + time1 + "\ntime2: " + time2);
            JsonArray value = record.value();
            System.out.println("SALAM ALEJKYM: " + record.key() + " , POSTS: " + value);
        });
    }
}
