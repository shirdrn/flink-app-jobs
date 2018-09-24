package org.shirdrn.flink.broadcaststate;

import com.alibaba.fastjson.JSONObject;
import java.io.Serializable;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

public class UserEvent implements Serializable {

  private String userId;
  private String channel;
  private String eventType;
  private String eventTime;
  private Long eventTimestamp;
  private JSONObject data;

  private UserEvent(String userId, String channel, String eventType,
      long eventTimestamp, JSONObject data) {
    super();
    this.userId = userId;
    this.channel = channel;
    this.eventType = eventType;
    this.eventTimestamp = eventTimestamp;
    this.data = data;
  }

  public void setUserId(String userId) {
    this.userId = userId;
  }

  public String getUserId() {
    return userId;
  }

  public void setChannel(String channel) {
    this.channel = channel;
  }

  public String getChannel() {
    return channel;
  }

  public void setEventType(String eventType) {
    this.eventType = eventType;
  }

  public String getEventType() {
    return eventType;
  }

  public void setEventTime(String eventTime) {
    this.eventTime = eventTime;
  }

  public String getEventTime() {
    return eventTime;
  }

  public void setEventTimestamp(Long eventTimestamp) {
    this.eventTimestamp = eventTimestamp;
  }

  public Long getEventTimestamp() {
    return eventTimestamp;
  }

  public void setData(JSONObject data) {
    this.data = data;
  }

  public JSONObject getData() {
    return data;
  }

  // {"userId":"a9b83681ba4df17a30abcf085ce80a9b","channel":"APP","eventType":"VIEW_PRODUCT","eventTime":"2018-06-12_08:45:24","data":{"productId":126}}
  // {"userId":"a9b83681ba4df17a30abcf085ce80a9b","channel":"APP","eventType":"VIEW_PRODUCT","eventTime":"2018-06-12_08:57:32","data":{"productId":273}}
  // {"userId":"a9b83681ba4df17a30abcf085ce80a9b","channel":"APP","eventType":"VIEW_PRODUCT","eventTime":"2018-06-12_09:21:08","data":{"productId":126}}
  // {"userId":"a9b83681ba4df17a30abcf085ce80a9b","channel":"APP","eventType":"VIEW_PRODUCT","eventTime":"2018-06-12_09:21:49","data":{"productId":103}}
  // {"userId":"a9b83681ba4df17a30abcf085ce80a9b","channel":"APP","eventType":"VIEW_PRODUCT","eventTime":"2018-06-12_09:21:59","data":{"productId":157}}
  // {"userId":"a9b83681ba4df17a30abcf085ce80a9b","channel":"APP","eventType":"VIEW_PRODUCT","eventTime":"2018-06-12_09:27:11","data":{"productId":126}}
  // {"userId":"a9b83681ba4df17a30abcf085ce80a9b","channel":"APP","eventType":"ADD_TO_CART","eventTime":"2018-06-12_09:43:18","data":{"productId":126}}
  // {"userId":"a9b83681ba4df17a30abcf085ce80a9b","channel":"APP","eventType":"VIEW_PRODUCT","eventTime":"2018-06-12_09:27:11","data":{"productId":126}}
  // {"userId":"a9b83681ba4df17a30abcf085ce80a9b","channel":"APP","eventType":"PURCHASE","eventTime":"2018-06-12_09:30:28","data":{"productId":126,"price":299.00,"amount":260.00}}

  public static UserEvent buildEvent(String eventStr) {
    JSONObject event = JSONObject.parseObject(eventStr);
    String eventTimeStr = event.getString("eventTime");
    String[] a = eventTimeStr.split("_");
    DateTimeFormatter formatter =
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.of("Asia/Shanghai"));
    ZonedDateTime time = ZonedDateTime.parse(a[0] + " " + a[1], formatter);

    final UserEvent userEvent = new UserEvent(
        event.getString("userId"),
        event.getString("channel"),
        event.getString("eventType"),
        time.toEpochSecond(),
        event.getJSONObject("data"));
    userEvent.setEventTime(eventTimeStr);
    return userEvent;
  }

  @Override
  public String toString() {
    JSONObject event = new JSONObject();
    event.put("userId", userId);
    event.put("channel", channel);
    event.put("eventType", eventType);
    event.put("eventTime", eventTime);
    event.put("eventTimestamp", eventTimestamp);
    event.put("data", data);
    return event.toJSONString();
  }
}
