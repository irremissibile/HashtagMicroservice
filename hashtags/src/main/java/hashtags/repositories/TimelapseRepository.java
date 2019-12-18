package hashtags.repositories;

import io.vertx.core.json.JsonArray;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import io.vertx.kafka.client.serialization.VertxSerdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.*;

import java.util.Properties;


public class TimelapseRepository {

    ReadOnlyKeyValueStore<String, JsonArray> keyValueStore;
    private KafkaProducer<String, JsonArray> producer;


    public TimelapseRepository(KafkaProducer<String, JsonArray> producer) {
        this.producer = producer;
        setup();
    }


    private ReadOnlyKeyValueStore<String, JsonArray> waitUntilStoreIsQueryable(String storeName,
                                                                               final KafkaStreams streams) throws InterruptedException {
        while (true) {
            try {
                System.out.println("one more time");
                return streams.store(storeName, QueryableStoreTypes.keyValueStore());
            } catch (InvalidStateStoreException e) {
                System.out.println(e);
                Thread.sleep(100);
            }
        }
    }



    private void setup() {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "microserviceTimelapseRepository");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, VertxSerdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, VertxSerdes.JsonArray().getClass());

        StreamsBuilder builder = new StreamsBuilder();

        GlobalKTable<String, JsonArray> globalKTable = builder.globalTable(
                "timelapserepo",
                Materialized.<String, JsonArray, KeyValueStore<Bytes, byte[]>>as("timelapse-store2")
                        .withKeySerde(VertxSerdes.String())
                        .withValueSerde(VertxSerdes.JsonArray())
                        .withCachingDisabled());

        Topology topology = builder.build();
        KafkaStreams streams = new KafkaStreams(topology, properties);

        streams.setUncaughtExceptionHandler((Thread thread, Throwable throwable) -> {
            System.out.println("SOMETHING BAD HAPPENED");
        });

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        streams.start();


        try {
            keyValueStore = waitUntilStoreIsQueryable("timelapse-store2", streams);
            System.out.println("WAITED");
        } catch (InterruptedException e) {
            System.out.println(e);
        }
    }



    public JsonArray findTimelapseHashtags(String id) {
        return keyValueStore.get(id);
    }

    public void sendTimelapseRecord(String id, JsonArray hashtags) {
        KafkaProducerRecord<String, JsonArray> record =
                KafkaProducerRecord.create("timelapserepo", id, hashtags);
        producer.send(record);
    }


    public void close() {
        producer.close();
    }
}
