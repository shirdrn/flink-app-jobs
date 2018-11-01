package org.shirdrn.flink.jobs.batch;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.http.HttpHost;
import org.apache.log4j.Logger;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.shirdrn.flink.connector.batch.elasticsearch.ElasticsearchApiCallBridge;
import org.shirdrn.flink.connector.batch.elasticsearch.ElasticsearchOutputFormat;
import org.shirdrn.flink.connector.batch.elasticsearch.ElasticsearchOutputFormat.Builder;
import org.shirdrn.flink.connector.batch.elasticsearch.ElasticsearchSinkFunction;
import org.shirdrn.flink.connector.batch.elasticsearch.RequestIndexer;
import org.shirdrn.flink.hbase.HFileInputFormat;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class ExportHBaseDataJob {

  private static final Logger LOG = Logger.getLogger(ExportHBaseDataJob.class);

  // --input-path hdfs://namenode01.td.com/tmp/data --es-http-hosts 172.23.4.141 --es-http-port 9200 --es-index a_multi_val --es-type a_my_type
  public static void main(String[] args) throws Exception {
    LOG.info("Input params: " + Arrays.asList(args));
    final ParameterTool parameterTool = ParameterTool.fromArgs(args);
    if (parameterTool.getNumberOfParameters() < 5) {
      System.out.println("Missing parameters!\n" +
          "Usage: batch " +
          "--input-path <hdfsFile> " +
          "--es-http-hosts <esHttpHosts> " +
          "--es-http-port <esHttpPort> " +
          "--es-index <esIndex> " +
          "--es-type <esType> " +
          "--bulk-flush-interval-millis <bulkFlushIntervalMillis>" +
          "--bulk-flush-max-size-mb <bulkFlushMaxSizeMb>" +
          "--bulk-flush-max-actions <bulkFlushMaxActions>");
      return;
    }

    String file = parameterTool.getRequired("input-path");

    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    final HFileInputFormat inputFormat = new HFileInputFormat();
    env.createInput(inputFormat);

    final String jobName = ExportHBaseDataJob.class.getSimpleName();
    env.execute(jobName);
  }
}
