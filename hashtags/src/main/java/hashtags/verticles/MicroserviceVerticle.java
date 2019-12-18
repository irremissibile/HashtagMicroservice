package hashtags.verticles;

import io.vertx.core.*;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.impl.ConcurrentHashSet;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.healthchecks.HealthCheckHandler;
import io.vertx.ext.healthchecks.HealthChecks;
import io.vertx.ext.healthchecks.Status;
import io.vertx.ext.web.Router;
import io.vertx.servicediscovery.Record;
import io.vertx.servicediscovery.ServiceDiscovery;
import io.vertx.servicediscovery.ServiceDiscoveryOptions;
import io.vertx.servicediscovery.types.HttpEndpoint;
import io.vertx.servicediscovery.types.MessageSource;
import hashtags.model.Config;
import hashtags.model.ConfigMessageCodec;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static hashtags.verticles.ConfigurationVerticle.*;

public abstract class MicroserviceVerticle extends AbstractVerticle {


    private ServiceDiscovery mDiscovery;
    private Set<Record> mRegisteredRecords = new ConcurrentHashSet<>();


    @Override
    public void stop(Promise<Void> stopFuture) {
        List<Promise> promises = mRegisteredRecords
                .stream()
                .map(this::unpublish)
                .collect(Collectors.toList());

        if (promises.isEmpty()) {
            stopDiscovery(stopFuture);
        } else {
            stopServices(promises, stopFuture);
        }
    }




    protected void createServiceDiscovery() {
        JsonObject config = config();
        ServiceDiscoveryOptions opts = new ServiceDiscoveryOptions()
                .setBackendConfiguration(config);
        mDiscovery = ServiceDiscovery.create(vertx, opts);
    }



    private void publish(Record record, Handler<AsyncResult<Void>> completion) {
        mDiscovery.publish(record, ar -> {
            if (ar.succeeded())
                mRegisteredRecords.add(record);
            completion.handle(ar.map((Void)null));
        });
    }



    protected void publishHttpEndpoint(String endpoint, String host, int port, Handler<AsyncResult<Void>> completion) {
        JsonObject filter = new JsonObject()
                .put("name", endpoint);
        mDiscovery.getRecord(filter, ar -> {
            /** if there are no service with this name running */
            if (!ar.succeeded() || ar.result() == null) {
                /** create record to describe this service */
                Record record = HttpEndpoint.createRecord(endpoint, host, port, "/");
                /** and publish the service */
                publish(record, completion);
            }
        });

    }


    protected void publishMessageSource(String name, String address, Handler<AsyncResult<Void>> completionHandler) {
        JsonObject filter = new JsonObject()
                .put("name", name);
        mDiscovery.getRecord(filter, ar -> {
            if (ar.failed() || ar.result() == null) {
                Record record = MessageSource.createRecord(name, address);
                publish(record, completionHandler);
            }
        });
    }


    protected ServiceDiscovery getDiscovery() {
        return mDiscovery;
    }


    protected void createHealthCheck() {
        HealthChecks hc = HealthChecks.create(vertx);
        hc.register("Microservice", 5000, future -> future.complete(Status.OK()));

        HealthCheckHandler healthCheckHandler = HealthCheckHandler.create(vertx);
        Router router = Router.router(vertx);
        router.get("/health*").handler(healthCheckHandler);
        vertx.createHttpServer()
                .requestHandler(router)
                .listen(4000, ar -> {
                    if (ar.failed()) {
                        System.out.println("Health check server failed to start: " + ar.cause().getMessage());
                    } else {
                        System.out.println("Health check server started on 4000");
                    }
                });
    }

    private Promise<Void> unpublish(Record record) {
        mRegisteredRecords.remove(record);

        Promise<Void> unregisteringFuture = Promise.promise();
        mDiscovery.unpublish(record.getRegistration(), unregisteringFuture);

        return unregisteringFuture;
    }


    private void stopDiscovery(@Nonnull Promise<Void> stopPromise) {
        mDiscovery.close();
        stopPromise.complete();
    }

    private void stopServices(@Nonnull List<Promise> promises, Promise<Void> stopPromise) {
        List<Future> futures = promises
                .stream()
                .map((Function<Promise, Future>) Promise::future)
                .collect(Collectors.toList());


        CompositeFuture.all(futures).setHandler(ar -> {
            mDiscovery.close();

            if (ar.failed())
                stopPromise.fail(ar.cause());
            else
                stopPromise.complete();
        });
    }


    protected Promise<MessageConsumer<Config>> setupConfigConsumer() {
        Promise<MessageConsumer<Config>> promise = Promise.promise();
        JsonObject filter = new JsonObject()
                .put("name", EBA_CONFIG_UPDATE);

        MessageSource.<Config>getConsumer(getDiscovery(), filter, ar -> {
            if (ar.succeeded()) {
                MessageConsumer<Config> configConsumer = ar.result();
                promise.complete(configConsumer);
            } else
                promise.fail("Couldn't get ConfigMessageConsumer");
        });

        return promise;
    }

    protected void registerCodecs() {
        try {
            vertx.eventBus().registerDefaultCodec(Config.class, new ConfigMessageCodec());
        } catch (IllegalStateException ignored) {}
    }
}

