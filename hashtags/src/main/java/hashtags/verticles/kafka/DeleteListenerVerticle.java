package hashtags.verticles.kafka;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonObject;
import io.vertx.kafka.client.common.TopicPartition;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.consumer.OffsetAndMetadata;
import hashtags.model.Config;
import hashtags.verticles.MicroserviceVerticle;

import javax.annotation.Nonnull;
import java.util.*;
import java.util.stream.Collectors;

import static hashtags.verticles.ConfigurationVerticle.*;

public class DeleteListenerVerticle extends MicroserviceVerticle {

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
                System.out.println("DeleteListenerVerticle started");
                startPromise.complete();
            } else
                startPromise.fail(ar.cause());
        });
    }


    private Promise<Void> fetchConfigAndStart() {
        Promise<Void> promise = Promise.promise();
        Promise<Void> promiseSetup = setupDeleteListener(new Config(config()));

        promiseSetup.future().setHandler(ar -> {
            if (ar.succeeded())
                promise.complete();
            else
                promise.fail(ar.cause());
        });
        return promise;
    }




    private Promise<Void> setupDeleteListener(@Nonnull Config config){
        Promise<Void> promise = Promise.promise();
        if (mConsumer != null) mConsumer.close();

        Map<String, String> configuration = new HashMap<>();
        configuration.put("bootstrap.servers", config.getBootstrapServers());
        configuration.put("group.id", config.getDeleteGroup());
        configuration.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        configuration.put("value.deserializer", "io.vertx.kafka.client.serialization.JsonObjectDeserializer");
        configuration.put("auto.offset.reset", "earliest");
        configuration.put("enable.auto.commit", "true");

        String topic = config.getDeleteTopic();

        mConsumer = KafkaConsumer.create(vertx, configuration);

        mConsumer.handler(record -> {
            vertx.eventBus().request(EBA_PROCESS_DELETE, record.value().getString("id"), ar -> {
                if (ar.succeeded()) setCommitted(mConsumer,  record);
            });
        });

        mConsumer.subscribe(topic, ar -> {
            if (ar.succeeded())
                promise.complete();
            else
                promise.fail(ar.cause());

        });
        return promise;
    }


    private void setCommitted(KafkaConsumer<String, JsonObject> consumer, KafkaConsumerRecord<String, JsonObject> record) {
        Map<TopicPartition, OffsetAndMetadata> map = new HashMap<>();
        map.put(new TopicPartition(record.topic(), record.partition()),
                new OffsetAndMetadata(record.offset(), "some metadata?"));
        consumer.commit(map);
    }



    private Promise<Void> setupConfigListener() {
        Promise<Void> promise = Promise.promise();

        Promise<MessageConsumer<Config>> consumerPromise = setupConfigConsumer();
        consumerPromise.future().setHandler(ar -> {
            if (ar.succeeded()) {
                MessageConsumer<Config> configConsumer = ar.result();

                configConsumer.handler(message -> {
                    setupDeleteListener(message.body()).future().setHandler(listenerAr -> {
                        if (listenerAr.failed())
                            System.out.println("DeleteListener restart failed: " + listenerAr.cause().getMessage());
                        else
                            System.out.println("DeleteListener has successfully restarted");
                    });
                });

                promise.complete();
            } else
                promise.fail("Couldn't set ConfigMessageConsumer handler");
        });
        return promise;
    }
}
