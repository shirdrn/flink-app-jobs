package org.shirdrn.flink.broadcaststate;

import com.alibaba.fastjson.JSONObject;
import java.io.Serializable;
import org.apache.log4j.Logger;

public class Config implements Serializable {

  private static final Logger LOG = Logger.getLogger(Config.class);

  private String channel;
  private String registerDate;
  private int historyPurchaseTimes;
  private int maxPurchasePathLength;

  public Config() {
    this("APP", "2018-01-01", 0, 0);
  }

  public Config(String channel, String registerDate, int historyPurchaseTimes, int maxPurchasePathLength) {
    this.channel = channel;
    this.registerDate = registerDate;
    this.historyPurchaseTimes = historyPurchaseTimes;
    this.maxPurchasePathLength = maxPurchasePathLength;
  }

  public void setChannel(String channel) {
    this.channel = channel;
  }

  public String getChannel() {
    return channel;
  }

  public void setRegisterDate(String registerDate) {
    this.registerDate = registerDate;
  }

  public String getRegisterDate() {
    return registerDate;
  }

  public void setHistoryPurchaseTimes(int historyPurchaseTimes) {
    this.historyPurchaseTimes = historyPurchaseTimes;
  }

  public int getHistoryPurchaseTimes() {
    return historyPurchaseTimes;
  }

  public void setMaxPurchasePathLength(int maxPurchasePathLength) {
    this.maxPurchasePathLength = maxPurchasePathLength;
  }

  public int getMaxPurchasePathLength() {
    return maxPurchasePathLength;
  }

  // {"channel":"APP","registerDate":"2018-01-01","historyPurchaseTimes":0,"maxPurchasePathLength":1}
  public static Config buildConfig(String configStr) {
    Config config = new Config();
    JSONObject event;
    try {
      event = JSONObject.parseObject(configStr);
      config = new Config(
          event.getString("channel"),
          event.getString("registerDate"),
          event.getInteger("historyPurchaseTimes"),
          event.getInteger("maxPurchasePathLength"));
    } catch (Exception e) {
      LOG.warn("", e);
      LOG.warn("Failed to parse config, use default: " + config);
    }
    return config;
  }

  @Override
  public String toString() {
    JSONObject o = new JSONObject();
    o.put("channel", channel);
    o.put("registerDate", registerDate);
    o.put("historyPurchaseTimes", historyPurchaseTimes);
    o.put("maxPurchasePathLength", maxPurchasePathLength);
    return o.toJSONString();
  }
}
