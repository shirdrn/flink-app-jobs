package org.shirdrn.flink.connector.batch.elasticsearch;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Preconditions;
import org.apache.http.HttpHost;
import org.apache.log4j.Logger;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.client.RestHighLevelClient;
import org.shirdrn.flink.connector.batch.elasticsearch.ElasticsearchApiCallBridge.FlushBackoffType;

public class ElasticsearchOutputFormat<T> extends AbstractElasticsearchOutputFormat<T, RestHighLevelClient> {

  private static final Logger LOG = Logger.getLogger(ElasticsearchOutputFormat.class);
  private static final long serialVersionUID = 1L;

  private ElasticsearchOutputFormat(
      Map<String, String> bulkRequestsConfig,
      List<HttpHost> httpHosts,
      ElasticsearchSinkFunction<T> elasticsearchSinkFunction,
      ActionRequestFailureHandler failureHandler,
      RestClientFactory restClientFactory) {

    super(new Elasticsearch6ApiCallBridge(httpHosts, restClientFactory),  bulkRequestsConfig, elasticsearchSinkFunction, failureHandler);
  }

  @Override
  public void configure(Configuration configuration) {

  }

  /**
   * A builder for creating an {@link ElasticsearchOutputFormat}.
   *
   * @param <T> Type of the elements handled by the sink this builder creates.
   */
  @PublicEvolving
  public static class Builder<T> {

    private final List<HttpHost> httpHosts;
    private final ElasticsearchSinkFunction<T> elasticsearchSinkFunction;

    private Map<String, String> bulkRequestsConfig = new HashMap<>();
    private ActionRequestFailureHandler failureHandler = new NoOpFailureHandler();
    private RestClientFactory restClientFactory = restClientBuilder -> {};

    /**
     * Creates a new {@code ElasticsearchSink} that connects to the cluster using a {@link RestHighLevelClient}.
     *
     * @param httpHosts The list of {@link HttpHost} to which the {@link RestHighLevelClient} connects to.
     * @param elasticsearchSinkFunction This is used to generate multiple {@link ActionRequest} from the incoming element.
     */
    public Builder(List<HttpHost> httpHosts, ElasticsearchSinkFunction<T> elasticsearchSinkFunction) {
      this.httpHosts = Preconditions.checkNotNull(httpHosts);
      this.elasticsearchSinkFunction = Preconditions.checkNotNull(elasticsearchSinkFunction);
    }

    /**
     * Sets the maximum number of actions to buffer for each bulk request.
     *
     * @param numMaxActions the maxinum number of actions to buffer per bulk request.
     */
    public void setBulkFlushMaxActions(int numMaxActions) {
      Preconditions.checkArgument(
          numMaxActions > 0,
          "Max number of buffered actions must be larger than 0.");

      this.bulkRequestsConfig.put(CONFIG_KEY_BULK_FLUSH_MAX_ACTIONS, String.valueOf(numMaxActions));
    }

    /**
     * Sets the maximum size of buffered actions, in mb, per bulk request.
     *
     * @param maxSizeMb the maximum size of buffered actions, in mb.
     */
    public void setBulkFlushMaxSizeMb(int maxSizeMb) {
      Preconditions.checkArgument(
          maxSizeMb > 0,
          "Max size of buffered actions must be larger than 0.");

      this.bulkRequestsConfig.put(CONFIG_KEY_BULK_FLUSH_MAX_SIZE_MB, String.valueOf(maxSizeMb));
    }

    /**
     * Sets the bulk flush interval, in milliseconds.
     *
     * @param intervalMillis the bulk flush interval, in milliseconds.
     */
    public void setBulkFlushInterval(long intervalMillis) {
      Preconditions.checkArgument(
          intervalMillis >= 0,
          "Interval (in milliseconds) between each flush must be larger than or equal to 0.");

      this.bulkRequestsConfig.put(CONFIG_KEY_BULK_FLUSH_INTERVAL_MS, String.valueOf(intervalMillis));
    }

    /**
     * Sets whether or not to enable bulk flush backoff behaviour.
     *
     * @param enabled whether or not to enable backoffs.
     */
    public void setBulkFlushBackoff(boolean enabled) {
      this.bulkRequestsConfig.put(CONFIG_KEY_BULK_FLUSH_BACKOFF_ENABLE, String.valueOf(enabled));
    }

    /**
     * Sets the type of back of to use when flushing bulk requests.
     *
     * @param flushBackoffType the backoff type to use.
     */
    public void setBulkFlushBackoffType(FlushBackoffType flushBackoffType) {
      this.bulkRequestsConfig.put(
          CONFIG_KEY_BULK_FLUSH_BACKOFF_TYPE,
          Preconditions.checkNotNull(flushBackoffType).toString());
    }

    /**
     * Sets the maximum number of retries for a backoff attempt when flushing bulk requests.
     *
     * @param maxRetries the maximum number of retries for a backoff attempt when flushing bulk requests
     */
    public void setBulkFlushBackoffRetries(int maxRetries) {
      Preconditions.checkArgument(
          maxRetries > 0,
          "Max number of backoff attempts must be larger than 0.");

      this.bulkRequestsConfig.put(CONFIG_KEY_BULK_FLUSH_BACKOFF_RETRIES, String.valueOf(maxRetries));
    }

    /**
     * Sets the amount of delay between each backoff attempt when flushing bulk requests, in milliseconds.
     *
     * @param delayMillis the amount of delay between each backoff attempt when flushing bulk requests, in milliseconds.
     */
    public void setBulkFlushBackoffDelay(long delayMillis) {
      Preconditions.checkArgument(
          delayMillis >= 0,
          "Delay (in milliseconds) between each backoff attempt must be larger than or equal to 0.");
      this.bulkRequestsConfig.put(CONFIG_KEY_BULK_FLUSH_BACKOFF_DELAY, String.valueOf(delayMillis));
    }

    /**
     * Sets a failure handler for action requests.
     *
     * @param failureHandler This is used to handle failed {@link ActionRequest}.
     */
    public void setFailureHandler(ActionRequestFailureHandler failureHandler) {
      this.failureHandler = Preconditions.checkNotNull(failureHandler);
    }

    /**
     * Sets a REST client factory for custom client configuration.
     *
     * @param restClientFactory the factory that configures the rest client.
     */
    public void setRestClientFactory(RestClientFactory restClientFactory) {
      this.restClientFactory = Preconditions.checkNotNull(restClientFactory);
    }

    /**
     * Creates the Elasticsearch sink.
     *
     * @return the created Elasticsearch sink.
     */
    public ElasticsearchOutputFormat<T> build() {
      return new ElasticsearchOutputFormat<>(bulkRequestsConfig, httpHosts, elasticsearchSinkFunction, failureHandler, restClientFactory);
    }
  }
}
