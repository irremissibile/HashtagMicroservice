package hashtags;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.spi.cluster.hazelcast.HazelcastClusterManager;


public class Launcher extends io.vertx.core.Launcher {

  public static void main(String[] args) {
    new Launcher().dispatch(args);
  }

  @Override
  public void beforeStartingVertx(VertxOptions options) {
    super.beforeStartingVertx(options);

    DeploymentOptions deploymentOptions = new DeploymentOptions();
    deploymentOptions.setConfig(new JsonObject());

    ClusterManager mgr = new HazelcastClusterManager();
    options.setClusterManager(mgr);


    options.getEventBusOptions()
      .setClustered(true)
      .setTrustAll(true);
  }
}