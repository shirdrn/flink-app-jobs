package org.shirdrn.flink.connector.batch.hbase;

import java.io.IOException;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.hadoop.hbase.client.BufferedMutator;

public interface HBaseSinkFunction<T> extends Function {

  /**
   * Process element
   *
   * @param element element
   * @param ctx runtime context
   * @param bufferedMutator bufferedMutator
   */
  void process(T element, RuntimeContext ctx, BufferedMutator bufferedMutator) throws IOException;

}
