package hashtags.verticles.kafka;

import io.vertx.core.*;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.kafka.client.common.TopicPartition;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.consumer.OffsetAndMetadata;
import hashtags.model.Config;
import hashtags.verticles.MicroserviceVerticle;

import javax.annotation.Nonnull;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static hashtags.verticles.ConfigurationVerticle.*;

public class UpdateListenerVerticle extends MicroserviceVerticle {

    private Pattern hashtagPattern = Pattern.compile("#(\\w+)");
    private KafkaConsumer<String, JsonObject> mConsumer;


    @Override
    public void start(Promise<Void> startPromise) {
        createServiceDiscovery();
        registerCodecs();

        List<Promise<Void>> promises = Arrays.asList(
                fetchConfigAndStart(),
                setupConfigListener());

        List<Future> futures = promises.stream()
                .map(Promise::future)
                .collect(Collectors.toList());

        CompositeFuture.all(futures).setHandler(ar -> {
            if (ar.succeeded()) {
                System.out.println("UpdateListenerVerticle started");
                startPromise.complete();
            } else
                startPromise.fail(ar.cause());
        });
    }



    private Promise<Void> fetchConfigAndStart() {
        Promise<Void> promise = Promise.promise();
        Promise<Void> promiseSetup = setupTimelapseListener(new Config(config()));

        promiseSetup.future().setHandler(ar -> {
            if (ar.succeeded())
                promise.complete();
            else
                promise.fail(ar.cause());
        });
        return promise;
    }



    private Promise<Void> setupTimelapseListener(@Nonnull Config config) {
        Promise<Void> promise = Promise.promise();
        if (mConsumer != null) mConsumer.close();

        Map<String, String> configuration = new HashMap<>();
        configuration.put("bootstrap.servers", config.getBootstrapServers());
        configuration.put("group.id", config.getUpdateGroup());
        configuration.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        configuration.put("value.deserializer", "io.vertx.kafka.client.serialization.JsonObjectDeserializer");
        configuration.put("auto.offset.reset", "earliest");
        configuration.put("enable.auto.commit", "true");

        String topic = config.getUpdateTopic();

        mConsumer = KafkaConsumer.create(vertx, configuration);

        mConsumer.handler(record -> {
            String id = record.value().getString("id");
            String desc = record.value().getString("desc");

            if (desc != null) {
                JsonArray array = fetchHashtagsFromString(desc);
                if (!array.isEmpty()) {
                    JsonObject update = new JsonObject()
                            .put("id", id)
                            .put("hashtags", array);

                    vertx.eventBus().request(EBA_PROCESS_UPDATE, update, ar -> {
                        if (ar.succeeded())
                            setCommitted(mConsumer, record);
                    });
                } else
                    setCommitted(mConsumer, record);
            } else
                setCommitted(mConsumer, record);
        });

        mConsumer.subscribe(topic, ar -> {
            if (ar.succeeded())
                promise.complete();
            else
                promise.fail(ar.cause());
        });
        return promise;
    }



    private Promise<Void> setupConfigListener() {
        Promise<Void> promise = Promise.promise();

        Promise<MessageConsumer<Config>> consumerPromise = setupConfigConsumer();
        consumerPromise.future().setHandler(ar -> {
            if (ar.succeeded()) {
                MessageConsumer<Config> configConsumer = ar.result();

                configConsumer.handler(message -> {
                    setupTimelapseListener(message.body()).future().setHandler(listenerAr -> {
                        if (listenerAr.failed())
                            System.out.println("UpdateListener restart failed: " + listenerAr.cause().getMessage());
                        else
                            System.out.println("UpdateListener has successfully restarted");
                    });
                });

                promise.complete();
            } else
                promise.fail("Couldn't set ConfigMessageConsumer handler");
        });
        return promise;
    }



    private JsonArray fetchHashtagsFromString(String desc) {
        Matcher matcher = hashtagPattern.matcher(desc);

        List<String> hashtags = new ArrayList<>();
        while (matcher.find())
            hashtags.add(matcher.group(1));

        JsonArray jsonArray = new JsonArray();
        hashtags.stream()
                .distinct()
                .forEach(jsonArray::add);

        return jsonArray;
    }

    private void setCommitted(KafkaConsumer<String, JsonObject> consumer, KafkaConsumerRecord<String, JsonObject> record) {
        Map<TopicPartition, OffsetAndMetadata> map = new HashMap<>();
        map.put(new TopicPartition(record.topic(), record.partition()),
                new OffsetAndMetadata(record.offset(), "some metadata?"));
        consumer.commit(map);
    }
}
