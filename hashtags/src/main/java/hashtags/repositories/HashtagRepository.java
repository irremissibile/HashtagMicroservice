package hashtags.repositories;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import io.vertx.kafka.client.serialization.VertxSerdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.*;

import java.util.List;
import java.util.Properties;


public class HashtagRepository {

    ReadOnlyKeyValueStore<String, JsonArray> keyValueStore;
    private KafkaProducer<String, JsonArray> producer;


    public HashtagRepository(KafkaProducer<String, JsonArray> producer) {
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
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "microserviceHashtagRepository");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, VertxSerdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, VertxSerdes.JsonArray().getClass());

        StreamsBuilder builder = new StreamsBuilder();

        GlobalKTable<String, JsonArray> globalKTable = builder.globalTable(
                "hashtagrepo",
                Materialized.<String, JsonArray, KeyValueStore<Bytes, byte[]>>as("hashtag-store4")
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
            keyValueStore = waitUntilStoreIsQueryable("hashtag-store4", streams);
            System.out.println("WAITED");
        } catch (InterruptedException e) {
            System.out.println(e);
        }
    }





    public JsonArray findTimelapsesForHashtag(String hashtag) {
        return keyValueStore.get(hashtag);
    }

    public JsonObject findTimelapsesForHashtagList(JsonArray hashtags) {
        JsonObject reply = new JsonObject();
        List<String> list = hashtags.getList();

        list.stream()
                .map(tag -> KeyValue.pair(tag, keyValueStore.get(tag)))
                .forEach(pair -> reply.put(pair.key, pair.value));
        return reply;
    }



    public void sendRecord(String hashtag, JsonArray timelapses) {
        KafkaProducerRecord<String, JsonArray> record =
                KafkaProducerRecord.create("hashtagrepo", hashtag, timelapses);
        KafkaProducerRecord<String, JsonArray> record2 =
                KafkaProducerRecord.create("hashtags", hashtag, timelapses);

        producer.send(record);
        producer.send(record2);
    }

    public void close() {
        producer.close();
    }
}
