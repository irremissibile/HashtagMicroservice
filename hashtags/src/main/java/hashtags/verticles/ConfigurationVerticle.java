package hashtags.verticles;

import hashtags.model.Config;
import io.vertx.config.ConfigChange;
import io.vertx.config.ConfigRetriever;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;


public class ConfigurationVerticle extends MicroserviceVerticle {

  public static final String EBA_CONFIG_FETCH = "configuration:fetch";
  public static final String EBA_CONFIG_UPDATE = "configuration:update";

  public static final String EBA_PROCESS_UPDATE = "process:update";
  public static final String EBA_PROCESS_DELETE = "process:delete";


  private Config mConfig;


  @Override
  public void start(Promise<Void> startPromise) {
    createServiceDiscovery();
    registerCodecs();
    setupRetriever(startPromise);
    setupListener();
  }


  private void setupRetriever(Promise<Void> startPromise) {
    ConfigRetriever retriever = ConfigRetriever.create(vertx);
    retriever.getConfig(configAr -> {
      if (configAr.failed()) {
        startPromise.fail(configAr.cause());
        return;
      }

      mConfig = new Config(configAr.result());

      publishMessageSource(EBA_CONFIG_UPDATE, EBA_CONFIG_UPDATE, publishAr -> {
        if (publishAr.failed())
          startPromise.fail(publishAr.cause());
        else {
          vertx.eventBus().publish(EBA_CONFIG_UPDATE, mConfig);

          startPromise.complete();
        }
      });
    });
    retriever.listen(this::onConfigChange);
    retriever.configStream().exceptionHandler(e -> System.out.println("Error. Config file not found"));
  }

  private void onConfigChange(ConfigChange change) {
    JsonObject configJson = change.getNewConfiguration();
    mConfig = new Config(configJson);

    vertx.eventBus().publish(EBA_CONFIG_UPDATE, mConfig);
  }

  private void setupListener() {
    vertx.eventBus().<Config>consumer(EBA_CONFIG_FETCH, handler -> handler.reply(mConfig));
  }

}
