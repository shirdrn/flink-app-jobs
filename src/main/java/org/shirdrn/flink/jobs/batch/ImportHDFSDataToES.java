package org.shirdrn.flink.jobs.batch;

import com.alibaba.fastjson.JSONObject;
import java.util.ArrayList;
import java.util.Arrays;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.http.HttpHost;
import org.apache.log4j.Logger;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.shirdrn.flink.connector.batch.elasticsearch.ElasticsearchOutputFormat;
import org.shirdrn.flink.connector.batch.elasticsearch.ElasticsearchOutputFormat.Builder;
import org.shirdrn.flink.connector.batch.elasticsearch.ElasticsearchSinkFunction;
import org.shirdrn.flink.connector.batch.elasticsearch.RequestIndexer;

public class ImportHDFSDataToES {

  private static final Logger LOG = Logger.getLogger(ImportHDFSDataToES.class);

  // --input-path hdfs://namenode01.td.com/tmp/data --http-hosts 172.23.4.141 --http-port 9200 --es-index a_multi_val --es-type a_my_type
  public static void main(String[] args) throws Exception {
    LOG.info("Input params: " + Arrays.asList(args));
    final ParameterTool parameterTool = ParameterTool.fromArgs(args);
    if (parameterTool.getNumberOfParameters() < 5) {
      System.out.println("Missing parameters!\n" +
          "Usage: batch " +
          "--input-path <hdfs file> " +
          "--http-hosts <es http hosts> " +
          "--http-port <es http port> " +
          "--es-index <es index> " +
          "--es-type <es type> " +
          "--bulk-flush-max-actions <bulkFlushMaxActions>");
      return;
    }

    String file = parameterTool.getRequired("input-path");

    final ElasticsearchSinkFunction<String> elasticsearchSinkFunction =
        new ElasticsearchSinkFunction<String>() {
      @Override
      public void process(String element, RuntimeContext ctx, RequestIndexer indexer) {
        indexer.add(createIndexRequest(element, parameterTool));
      }
    };
    ArrayList<HttpHost> httpHosts = new ArrayList<>();
    String ipAddress = parameterTool.getRequired("http-hosts");
    LOG.info("Config: httpHosts=" + ipAddress);
    int port = parameterTool.getInt("http-port", 9200);
    LOG.info("Config: httpPort=" + port);
    httpHosts.add(new HttpHost(ipAddress, port, "http"));

    int bulkFlushMaxActions = 1;
    if (parameterTool.has("bulk-flush-max-actions")) {
      bulkFlushMaxActions = parameterTool.getInt("bulk-flush-max-actions");
    }
    LOG.info("Config: bulkFlushMaxActions=" + bulkFlushMaxActions);

    final Builder<String> builder =
        new Builder<>(httpHosts, elasticsearchSinkFunction);
    builder.setBulkFlushMaxActions(bulkFlushMaxActions);
    ElasticsearchOutputFormat outputFormat = builder.build();

    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    env.readTextFile(file)
        .filter(line -> !line.isEmpty())
        .map(line -> line)
        .output(outputFormat);

    final String jobName = ImportHDFSDataToES.class.getSimpleName();
    env.execute(jobName);
  }

  private static IndexRequest createIndexRequest(String element, ParameterTool parameterTool) {
    LOG.info("Create index req: " + element);
    JSONObject o = JSONObject.parseObject(element);
    return Requests.indexRequest()
        .index(parameterTool.getRequired("es-index"))
        .type(parameterTool.getRequired("es-type"))
        .source(o);
  }
}
