package org.shirdrn.flink.connector.batch.elasticsearch;

import java.io.Serializable;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.functions.RuntimeContext;

public interface ElasticsearchSinkFunction<T> extends Function, Serializable {

  /**
   * Process element
   *
   * @param element element
   * @param ctx runtime context
   * @param requestIndexer requestIndexer
   */
  void process(T element, RuntimeContext ctx, RequestIndexer requestIndexer);

}
