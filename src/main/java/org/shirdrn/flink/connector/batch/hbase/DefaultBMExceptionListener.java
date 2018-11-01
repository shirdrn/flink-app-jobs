package org.shirdrn.flink.connector.batch.hbase;

import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultBMExceptionListener implements BufferedMutator.ExceptionListener {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultBMExceptionListener.class);

  @Override
  public void onException(RetriesExhaustedWithDetailsException e, BufferedMutator mutator) {
    for (int i = 0; i < e.getNumExceptions(); i++) {
      LOG.warn("Failed to sent put " + e.getRow(i) + ".");
    }
  }
}
