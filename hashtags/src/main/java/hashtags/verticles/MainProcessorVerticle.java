package hashtags.verticles;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.kafka.client.producer.KafkaProducer;
import org.apache.kafka.streams.KeyValue;
import hashtags.model.Config;
import hashtags.repositories.HashtagRepository;
import hashtags.repositories.TimelapseRepository;

import javax.annotation.Nonnull;
import java.util.*;
import java.util.stream.Collectors;

import static hashtags.verticles.ConfigurationVerticle.*;

public class MainProcessorVerticle extends MicroserviceVerticle {

    private TimelapseRepository timelapseRepo;
    private HashtagRepository hashtagRepo;

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
                System.out.println("MainProcessorVerticle started");
                startPromise.complete();
            } else
                startPromise.fail(ar.cause());
        });
    }

    private Promise<Void> fetchConfigAndStart() {
        Promise<Void> promise = Promise.promise();
        Promise<Void> promiseSetup = setupRepos(new Config(config()));

        promiseSetup.future().setHandler(ar -> {
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
                    setupRepos(message.body()).future().setHandler(listenerAr -> {
                        if (listenerAr.failed())
                            System.out.println("MainProcessorVerticle restart failed: " + listenerAr.cause().getMessage());
                        else
                            System.out.println("MainProcessorVerticle has successfully restarted");
                    });
                });

                promise.complete();
            } else
                promise.fail("Couldn't set ConfigMessageConsumer handler");
        });
        return promise;
    }


    private Promise<Void> setupRepos(@Nonnull Config config) {
        Promise<Void> promise = Promise.promise();

        if (timelapseRepo != null)
            timelapseRepo.close();
        if (hashtagRepo != null)
            hashtagRepo.close();

        Map<String, String> configuration = new HashMap<>();
        configuration.put("bootstrap.servers", config.getBootstrapServers());
        configuration.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        configuration.put("value.serializer", "io.vertx.kafka.client.serialization.JsonArraySerializer");
        configuration.put("acks", "1");

        KafkaProducer<String, JsonArray> producer1 = KafkaProducer.create(vertx, configuration);
        KafkaProducer<String, JsonArray> producer2 = KafkaProducer.create(vertx, configuration);
        timelapseRepo = new TimelapseRepository(producer1);
        hashtagRepo = new HashtagRepository(producer2);


        setupUpdateProcessor();
        setupDeleteProcessor();

        promise.complete();
        return promise;
    }


    private void setupUpdateProcessor() {
        vertx.eventBus().<JsonObject>consumer(EBA_PROCESS_UPDATE, handler -> {
            String id = handler.body().getString("id");
            JsonArray hashtags = handler.body().getJsonArray("hashtags");

            JsonObject fetchedList = hashtagRepo.findTimelapsesForHashtagList(hashtags);

            Set<String> repliedHashtags = fetchedList.fieldNames();
            repliedHashtags.stream()
                    .map(tag -> KeyValue.pair(tag, fetchedList.getJsonArray(tag) == null ? new JsonArray().add(id) : fetchedList.getJsonArray(tag).add(id)))
                    .forEach(pair -> hashtagRepo.sendRecord(pair.key, pair.value));

            timelapseRepo.sendTimelapseRecord(id, hashtags);
            handler.reply("ok");
        });
    }


    private KeyValue<String, JsonArray> removeItemFromJsonArray(KeyValue<String, JsonArray> pair, String id) {
        JsonArray newArray = new JsonArray();

        pair.value.stream()
                .filter(item -> !(item.toString()).equals(id))
                .forEach(newArray::add);

        return KeyValue.pair(pair.key, newArray);
    }


    private void setupDeleteProcessor() {
        vertx.eventBus().<String>consumer(EBA_PROCESS_DELETE, handler -> {
            String id = handler.body();

            JsonArray hashtags = timelapseRepo.findTimelapseHashtags(id);
            JsonObject fetchedList = hashtagRepo.findTimelapsesForHashtagList(hashtags);

            Set<String> repliedHashtags = fetchedList.fieldNames();
            repliedHashtags.stream()
                    .map(tag -> KeyValue.pair(tag, fetchedList.getJsonArray(tag)))
                    .map(pair -> removeItemFromJsonArray(pair, id))
                    .forEach(pair -> hashtagRepo.sendRecord(pair.key, pair.value));

            timelapseRepo.sendTimelapseRecord(id, null);

            handler.reply("ok");
        });
    }
}




