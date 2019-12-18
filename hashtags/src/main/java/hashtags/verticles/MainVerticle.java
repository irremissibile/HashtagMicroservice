package hashtags.verticles;

import io.vertx.config.ConfigRetriever;
import hashtags.verticles.kafka.*;
import io.vertx.core.CompositeFuture;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class MainVerticle extends MicroserviceVerticle {

    private JsonObject mJsonConfig;
    private Map<String, List<String>> mVerticles = new HashMap<>();


    @Override
    public void start(Promise<Void> startPromise) {
        createServiceDiscovery();

        Promise<Void> configPromise = loadConfig();
        configPromise.future().setHandler(ar -> {
            startVerticles(startPromise);
        });
    }

    private Promise<Void> loadConfig() {
        Promise<Void> promise = Promise.promise();

        ConfigRetriever retriever = ConfigRetriever.create(vertx);
        retriever.getConfig(configAr -> {
            if (configAr.succeeded()) {
                JsonObject json = configAr.result();

                mJsonConfig = new JsonObject()
                        .put("endpoint", json.getJsonObject("endpoint"))
                        .put("kafka", json.getJsonObject("kafka"));
                promise.complete();
            } else
                promise.fail(configAr.cause());
        });
        return promise;
    }

    private void startVerticles(Promise<Void> startPromise) {
        List<Promise> verticlesPromises = Stream.of(
                ConfigurationVerticle.class,
                DeleteListenerVerticle.class,
                MainProcessorVerticle.class,
                UpdateListenerVerticle.class)
                .map(e -> redeployVerticle(e.getName(), mJsonConfig))
                .collect(Collectors.toList());

        List<Future> futures = verticlesPromises.stream()
                .map((Function<Promise, Future>) Promise::future)
                .collect(Collectors.toList());

        CompositeFuture.all(futures).setHandler(ar -> {
            if (ar.failed()) {
                startPromise.fail(ar.cause());
            } else {
                startPromise.complete();
            }
        });
    }



    private Promise<Void> redeployVerticle(String className, JsonObject config) {
        Promise<Void> completion = Promise.promise();
        removeExistingVerticles(className);

        DeploymentOptions options = new DeploymentOptions()
                .setConfig(config);
        vertx.deployVerticle(className, options, ar -> {
            if (ar.failed()) {
                completion.fail(ar.cause());
            } else {
                registerVerticle(className, ar.result());
                completion.complete();
            }
        });

        return completion;
    }

    private void registerVerticle(String className, String deploymentId) {
        mVerticles.computeIfAbsent(className, k -> new ArrayList<>());
        ArrayList<String> configVerticles = (ArrayList<String>) mVerticles.get(className);
        configVerticles.add(deploymentId);
    }

    private void removeExistingVerticles(String className) {
        mVerticles.getOrDefault(className, new ArrayList<>())
                .forEach(vertx::undeploy);
        mVerticles.remove(className);
    }
}
