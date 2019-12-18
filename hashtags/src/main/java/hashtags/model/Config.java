package hashtags.model;

import io.vertx.core.json.JsonObject;
import javax.annotation.Nonnull;

public class Config {

  private static final String ENDPOINT = "endpoint";
  private static final String ENDPOINT_HOST = "host";
  private static final String ENDPOINT_PORT = "port";

  private static final String KAFKA = "kafka";
  private static final String BOOTSTRAP_SERVERS = "bootstrap_servers";
  private static final String UPDATE_TOPIC = "update_topic";
  private static final String UPDATE_GROUP = "update_group";
  private static final String DELETE_TOPIC = "delete_topic";
  private static final String DELETE_GROUP = "delete_group";
  private static final String HASHTAGREPO_TOPIC = "hashtagrepo_topic";
  private static final String HASHTAGREPO_ID_CONFIG = "hashtagrepo_id_config";
  private static final String TIMELAPSEREPO_TOPIC = "timelapserepo_topic";
  private static final String TIMELAPSE_ID_CONFIG = "timelapserepo_id_config";
  private static final String OUTPUT_TOPIC = "output_topic";




  private final JsonObject mConfigObject;
  private final String mEndpointHost;
  private final String mEndpointPort;

  private final String mBootstrapServers;
  private final String mUpdateTopic;
  private final String mUpdateGroup;
  private final String mDeleteTopic;
  private final String mDeleteGroup;
  private final String mHashtagrepoTopic;
  private final String mHashtagrepoIdConfig;
  private final String mTimelapserepoTopic;
  private final String mTimelapseIdConfig;
  private final String mOutputTopic;

  // Constructors

  public Config(@Nonnull JsonObject config) {
    mConfigObject = config;

    JsonObject endpoint = config.getJsonObject(ENDPOINT);
    mEndpointHost = endpoint.getString(ENDPOINT_HOST);
    mEndpointPort = endpoint.getString(ENDPOINT_PORT);

    JsonObject kafka = config.getJsonObject(KAFKA);
    mBootstrapServers = kafka.getString(BOOTSTRAP_SERVERS);
    mUpdateTopic = kafka.getString(UPDATE_TOPIC);
    mUpdateGroup = kafka.getString(UPDATE_GROUP);
    mDeleteTopic  = kafka.getString(DELETE_TOPIC);
    mDeleteGroup = kafka.getString(DELETE_GROUP);
    mHashtagrepoTopic = kafka.getString(HASHTAGREPO_TOPIC);
    mHashtagrepoIdConfig = kafka.getString(HASHTAGREPO_ID_CONFIG);
    mTimelapserepoTopic = kafka.getString(TIMELAPSEREPO_TOPIC);
    mTimelapseIdConfig = kafka.getString(TIMELAPSE_ID_CONFIG);
    mOutputTopic = kafka.getString(OUTPUT_TOPIC);
  }

  // Public

  public JsonObject toJson() {
    JsonObject endpoint = new JsonObject()
      .put(ENDPOINT_HOST, mEndpointHost)
      .put(ENDPOINT_PORT, mEndpointPort);

    JsonObject kafka = new JsonObject()
            .put(BOOTSTRAP_SERVERS, mBootstrapServers)
            .put(UPDATE_TOPIC, mUpdateTopic)
            .put(UPDATE_GROUP, mUpdateGroup)
            .put(DELETE_TOPIC, mDeleteTopic)
            .put(DELETE_GROUP, mDeleteGroup)
            .put(HASHTAGREPO_TOPIC, mHashtagrepoTopic)
            .put(HASHTAGREPO_ID_CONFIG, mHashtagrepoIdConfig)
            .put(TIMELAPSEREPO_TOPIC, mTimelapserepoTopic)
            .put(TIMELAPSE_ID_CONFIG, mTimelapseIdConfig)
            .put(OUTPUT_TOPIC, mOutputTopic);

    return new JsonObject()
      .put(ENDPOINT, endpoint)
      .put(KAFKA, kafka);
  }

  // Accessors

  JsonObject getConfigObject() {
    return mConfigObject;
  }

  public String getEndpointHost() {
    return mEndpointHost;
  }

  public String getEndpointPort() {
    return mEndpointPort;
  }



  public String getBootstrapServers() {
    return mBootstrapServers;
  }

  public String getUpdateTopic() {
    return mUpdateTopic;
  }

  public String getUpdateGroup() {
    return mUpdateGroup;
  }

  public String getDeleteTopic() {
    return mDeleteTopic;
  }

  public String getDeleteGroup() {
    return mDeleteGroup;
  }

  public String getHashtagrepoTopic() {
    return mHashtagrepoTopic;
  }

  public String getHashtagrepoIdConfig() {
    return mHashtagrepoIdConfig;
  }

  public String getTimelapserepoTopic() {
    return mTimelapserepoTopic;
  }

  public String getTimelapseIdConfig() {
    return mTimelapseIdConfig;
  }

  public String getOutputTopic() {
    return mOutputTopic;
  }


  // Utils

  @Override
  public String toString() {
    return "Config{" +
      "mEndpointHost='" + mEndpointHost + '\'' +
      ", mEndpointPort=" + mEndpointPort +
      '}';
  }
}
