package org.shirdrn.flink.broadcaststate;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.log4j.Logger;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.shaded.guava18.com.google.common.collect.Maps;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.util.Collector;

// ./bin/kafka-topics.sh --zookeeper 172.23.4.138:2181,172.23.4.139:2181,172.23.4.140:2181/kafka --create --topic user_events --replication-factor 1 --partitions 1
// ./bin/kafka-topics.sh --zookeeper 172.23.4.138:2181,172.23.4.139:2181,172.23.4.140:2181/kafka --create --topic app_config --replication-factor 1 --partitions 1
// ./bin/kafka-topics.sh --zookeeper 172.23.4.138:2181,172.23.4.139:2181,172.23.4.140:2181/kafka --create --topic action_result --replication-factor 1 --partitions 1
// ./bin/kafka-console-producer.sh --topic user_events --broker-list 172.23.4.138:9092
// ./bin/kafka-console-producer.sh --topic app_config --broker-list 172.23.4.138:9092
// ./bin/kafka-console-consumer.sh --topic action_result --bootstrap-server 172.23.4.138:9092 --from-beginning
// --input-event-topic user_events --input-config-topic app_config --output-topic action_result --bootstrap.servers 172.23.4.138:9092 --zookeeper.connect zk01.td.com:2181,zk02.td.com:2181,zk03.td.com:2181/kafka --group.id customer-purchase-behavior-tracker
public class UserPurchaseBehaviorTracker {

  private static final Logger LOG = Logger.getLogger(UserPurchaseBehaviorTracker.class);
  private static final MapStateDescriptor<String, Config> configStateDescriptor =
      new MapStateDescriptor<>(
          "configBroadcastState",
          BasicTypeInfo.STRING_TYPE_INFO,
          TypeInformation.of(new TypeHint<Config>() {}));

  public static void main(String[] args) throws Exception {
//    args = new String[] {
//        "--input-event-topic",  "user_events",
//        "--input-config-topic", "app_config",
//        "--output-topic", "action_result",
//        "--bootstrap.servers", "172.23.4.138:9092",
//        "--zookeeper.connect", "zk01.td.com:2181,zk02.td.com:2181,zk03.td.com:2181/kafka",
//        "--group.id", "customer-purchase-behavior-tracker" };
    LOG.info("Input args: " + Arrays.asList(args));
    // parse input arguments
    final ParameterTool parameterTool = ParameterTool.fromArgs(args);
    if (parameterTool.getNumberOfParameters() < 5) {
      System.out.println("Missing parameters!\n" +
          "Usage: Kafka --input-event-topic <topic> --input-config-topic <topic> --output-topic <topic> " +
          "--bootstrap.servers <kafka brokers> " +
          "--zookeeper.connect <zk quorum> --group.id <some id>");
      return;
    }

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setStateBackend(new FsStateBackend(
        "hdfs://namenode01.td.com/flink-checkpoints/customer-purchase-behavior-tracker"));
    CheckpointConfig config = env.getCheckpointConfig();
    config.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
    config.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
    config.setCheckpointInterval(1 * 60 * 60 * 1000);
    env.getConfig().setGlobalJobParameters(parameterTool); // make parameters available in the web interface
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


    // create customer user event stream
    final FlinkKafkaConsumer010 kafkaUserEventSource = new FlinkKafkaConsumer010<>(
        parameterTool.getRequired("input-event-topic"),
        new SimpleStringSchema(), parameterTool.getProperties());

    // (userEvent, userId)
    final KeyedStream<UserEvent, String> customerUserEventStream = env
        .addSource(kafkaUserEventSource)
        .map(new MapFunction<String, UserEvent>() {
          @Override
          public UserEvent map(String s) throws Exception {
            return UserEvent.buildEvent(s);
          }
        })
        .assignTimestampsAndWatermarks(new CustomWatermarkExtractor(Time.hours(24)))
        .keyBy(new KeySelector<UserEvent, String>() {
          @Override
          public String getKey(UserEvent userEvent) throws Exception {
            return userEvent.getUserId();
          }
        });

    // create dynamic configuration event stream
    final FlinkKafkaConsumer010 kafkaConfigEventSource = new FlinkKafkaConsumer010<>(
        parameterTool.getRequired("input-config-topic"),
        new SimpleStringSchema(), parameterTool.getProperties());

    final BroadcastStream<Config> configBroadcastStream = env
        .addSource(kafkaConfigEventSource)
        .map(new MapFunction<String, Config>() {
          @Override
          public Config map(String value) throws Exception {
            return Config.buildConfig(value);
          }
        })
        .broadcast(configStateDescriptor);

    final FlinkKafkaProducer010 kafkaProducer = new FlinkKafkaProducer010<>(
        parameterTool.getRequired("output-topic"),
        new EvaluatedResultSchema(),
        parameterTool.getProperties());

    // connect above 2 streams
    DataStream<EvaluatedResult> connectedStream = customerUserEventStream
        .connect(configBroadcastStream)
        .process(new ConnectedBroadcastProcessFuntion());
    connectedStream.addSink(kafkaProducer);

    env.execute();
  }

  // (userId, userEvent, config, evaluatedResult)
  private static class ConnectedBroadcastProcessFuntion extends
      KeyedBroadcastProcessFunction<String, UserEvent, Config, EvaluatedResult> {

    private final Config defaultConfig = new Config();

    public ConnectedBroadcastProcessFuntion() {
      super();
    }

    // (channel, Map<uid, List<userEvent>>)
    private final MapStateDescriptor<String, Map<String, UserEventContainer>> userMapStateDesc =
        new MapStateDescriptor<>(
            "userEventContainerState",
            BasicTypeInfo.STRING_TYPE_INFO,
            new MapTypeInfo<>(String.class, UserEventContainer.class));

    @Override
    public void processBroadcastElement(Config value, Context ctx, Collector<EvaluatedResult> out)
        throws Exception {
      String channel = value.getChannel();
      BroadcastState<String, Config> state = ctx.getBroadcastState(configStateDescriptor);
      final Config oldConfig = ctx.getBroadcastState(configStateDescriptor).get(channel);
      if(state.contains(channel)) {
        LOG.info("Configured channel exists: channel=" + channel);
        LOG.info("Config detail: oldConfig=" + oldConfig + ", newConfig=" + value);
      } else {
        LOG.info("Config detail: defaultConfig=" + defaultConfig + ", newConfig=" + value);
      }
      // update config value for configKey
      state.put(channel, value);
    }

    @Override
    public void processElement(UserEvent value, ReadOnlyContext ctx,
        Collector<EvaluatedResult> out) throws Exception {
      String userId = value.getUserId();
      String channel = value.getChannel();

      EventType eventType = EventType.valueOf(value.getEventType());
      Config config = ctx.getBroadcastState(configStateDescriptor).get(channel);
      LOG.info("Read config: channel=" + channel + ", config=" + config);
      if (Objects.isNull(config)) {
        config = defaultConfig;
      }

      final MapState<String, Map<String, UserEventContainer>> state =
          getRuntimeContext().getMapState(userMapStateDesc);

      // collect per-user events to the user map state
      Map<String, UserEventContainer> userEventContainerMap = state.get(channel);
      if (Objects.isNull(userEventContainerMap)) {
        userEventContainerMap = Maps.newHashMap();
        state.put(channel, userEventContainerMap);
      }
      if (!userEventContainerMap.containsKey(userId)) {
        UserEventContainer container = new UserEventContainer();
        container.setUserId(userId);
        userEventContainerMap.put(userId, container);
      }
      userEventContainerMap.get(userId).getUserEvents().add(value);

      // check whether a user purchase event arrives
      // if true, then compute the purchase path length, and prepare to trigger predefined actions
      if (eventType == EventType.PURCHASE) {
        LOG.info("Receive a purchase event: " + value);
        Optional<EvaluatedResult> result = compute(config, userEventContainerMap.get(userId));
        result.ifPresent(r -> out.collect(result.get()));
        // clear evaluated user's events
        state.get(channel).remove(userId);
      }
    }

    private Optional<EvaluatedResult> compute(Config config, UserEventContainer container) {
      Optional<EvaluatedResult> result = Optional.empty();
      String channel = config.getChannel();
      int historyPurchaseTimes = config.getHistoryPurchaseTimes();
      int maxPurchasePathLength = config.getMaxPurchasePathLength();

      int purchasePathLen = container.getUserEvents().size();
      if (historyPurchaseTimes < 10 && purchasePathLen > maxPurchasePathLength) {
        // sort by event time
        container.getUserEvents().sort(
            Comparator.comparingLong(UserEvent::getEventTimestamp));

        final Map<String, Integer> stat = Maps.newHashMap();
        container.getUserEvents()
            .stream()
            .collect(Collectors.groupingBy(UserEvent::getEventType))
            .forEach((eventType, events) -> stat.put(eventType, events.size()));

        final EvaluatedResult evaluatedResult = new EvaluatedResult();
        evaluatedResult.setUserId(container.getUserId());
        evaluatedResult.setChannel(channel);
        evaluatedResult.setEventTypeCounts(stat);
        evaluatedResult.setPurchasePathLength(purchasePathLen);
        LOG.info("Evaluated result: " + evaluatedResult.toJSONString());
        result = Optional.of(evaluatedResult);
      }
      return result;
    }


  }

  private static class CustomWatermarkExtractor extends
      BoundedOutOfOrdernessTimestampExtractor<UserEvent> {

    public CustomWatermarkExtractor(Time maxOutOfOrderness) {
      super(maxOutOfOrderness);
    }
    @Override
    public long extractTimestamp(UserEvent element) {
      return element.getEventTimestamp();
    }

  }

  private static class UserEventContainer {

    private String userId;
    private List<UserEvent> userEvents = Lists.newArrayList();

    public void setUserId(String userId) {
      this.userId = userId;
    }

    public String getUserId() {
      return userId;
    }

    public List<UserEvent> getUserEvents() {
      return userEvents;
    }

    @Override
    public String toString() {
      return "userId=" +userId + ", userEvents=" +userEvents;
    }
  }

}
