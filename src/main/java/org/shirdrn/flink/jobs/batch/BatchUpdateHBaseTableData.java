package org.shirdrn.flink.jobs.batch;

import com.alibaba.fastjson.JSONObject;
import java.io.IOException;
import java.util.Arrays;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.shirdrn.flink.connector.batch.hbase.HBaseOutputFormat;
import org.shirdrn.flink.connector.batch.hbase.HBaseSinkFunction;

public class BatchUpdateHBaseTableData {

  private static final Logger LOG = Logger.getLogger(BatchUpdateHBaseTableData.class);

  // --input-path hdfs://namenode01.td.com/tmp/hbase-data --hbase-table-name test_users --hbase.client.write.buffer 2097152 --hbase.client.keyvalue.maxsize 10485760 --hbase.client.retries.number 3
  public static void main(String[] args) throws Exception {
    LOG.info("Input params: " + Arrays.asList(args));
    final ParameterTool parameterTool = ParameterTool.fromArgs(args);
    if (parameterTool.getNumberOfParameters() < 2) {
      System.out.println("Missing parameters!\n" +
          "Usage: batch " +
          "--input-path <hdfsFile> " +
          "--hbase-table-name <hbaseTableName> " +
          "--hbase.client.write.buffer <writeBufferSize>" +
          "--hbase.client.keyvalue.maxsize <maxKeyValueSize>" +
          "--hbase.client.retries.number <retriesNumber>");
      return;
    }

    String file = parameterTool.getRequired("input-path");

    // create hbase sink function
    final HBaseSinkFunction<Put> hbaseSinkFunction = new HBaseSinkFunction<Put>() {
      @Override
      public void process(Put element, RuntimeContext ctx, BufferedMutator bufferedMutator) throws IOException {
        bufferedMutator.mutate(element);
      }
    };

    String tableName = parameterTool.getRequired("hbase-table-name");

    int writeBufferSize = 2097152; // 2M
    if (parameterTool.has("hbase.client.write.buffer")) {
      writeBufferSize = parameterTool.getInt("hbase.client.write.buffer");
    }

    long maxKeyValueSize = 10485760; // 10M
    if (parameterTool.has("")) {
      maxKeyValueSize = parameterTool.getInt("hbase.client.keyvalue.maxsize");
    }

    int retriesNumber = 35;
    if (parameterTool.has("hbase.client.retries.number")) {
      retriesNumber = parameterTool.getInt("hbase.client.retries.number");
    }

    // create hbase output format
    final HBaseOutputFormat<Put> outputFormat = new HBaseOutputFormat.Builder<>(hbaseSinkFunction)
            .setHBaseTableName(tableName)
            .setHBaseWriteBufferSize(writeBufferSize)
            .setHBaseMaxKeyValueSize(maxKeyValueSize)
            .setHBaseRetriesNumber(retriesNumber)
            .build();

    // configure global variables
    final Configuration conf = new Configuration();
    conf.setString("hbase.rootdir", "hdfs://namenode01.td.com/hbase");
    conf.setString("hbase.cluster.distributed", "true");
    conf.setString("hbase.zookeeper.quorum","zk01.td.com,zk02.td.com,zk03.td.com");
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    env.getConfig().setGlobalJobParameters(conf);

    // batch processing
    env.readTextFile(file)
        .filter(line -> !line.isEmpty())
        .map(line -> {
          JSONObject o = JSONObject.parseObject(line);
          String rowKey = o.getString("rk");
          String cf = o.getString("cf");
          String q = o.getString("q");
          String val = o.getString("val");
          final Put put = new Put(Bytes.toBytes(rowKey));
          put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(q), Bytes.toBytes(val));
          return put;
        })
        .output(outputFormat);

    final String jobName = BatchUpdateHBaseTableData.class.getSimpleName();
    env.execute(jobName);
  }
}

/*
 create 'test_users', {NAME => 'info'}

hdfs dfs -text /tmp/hbase-data
{"rk":"19837169-fe1f-4c88-8c07-106977db9584","cf":"info","q":"name","val":"Dean"}
{"rk":"19837169-fe1f-4c88-8c07-106977db9584","cf":"info","q":"city","val":"New York"}
{"rk":"19837169-fe1f-4c88-8c07-106977db9584","cf":"info","q":"langs","val":"Java PHP Python"}
{"rk":"19837169-fe1f-4c88-8c07-106977db9585","cf":"info","q":"name","val":"Jimmy"}
{"rk":"19837169-fe1f-4c88-8c07-106977db9585","cf":"info","q":"city","val":"Beijing"}
{"rk":"19837169-fe1f-4c88-8c07-106977db9585","cf":"info","q":"langs","val":"C++ Go Csharp"}
{"rk":"19837169-fe1f-4c88-8c07-106977db9586","cf":"info","q":"name","val":"Linda"}
{"rk":"19837169-fe1f-4c88-8c07-106977db9586","cf":"info","q":"city","val":"Tokyo"}
{"rk":"19837169-fe1f-4c88-8c07-106977db9586","cf":"info","q":"langs","val":"Cobol Lisp"}

 hbase(main):003:0> scan 'test_users'
 ROW                                        COLUMN+CELL
 19837169-fe1f-4c88-8c07-106977db9581      column=info:city, timestamp=1540982452158, value=New York
 19837169-fe1f-4c88-8c07-106977db9581      column=info:langs, timestamp=1540982452158, value=Java PHP Python
 19837169-fe1f-4c88-8c07-106977db9581      column=info:name, timestamp=1540982452158, value=Dean
 19837169-fe1f-4c88-8c07-106977db9582      column=info:city, timestamp=1540982452158, value=Beijing
 19837169-fe1f-4c88-8c07-106977db9582      column=info:langs, timestamp=1540982452158, value=C++ Go Csharp
 19837169-fe1f-4c88-8c07-106977db9582      column=info:name, timestamp=1540982452158, value=Jimmy
 19837169-fe1f-4c88-8c07-106977db9583      column=info:city, timestamp=1540982452158, value=Tokyo
 19837169-fe1f-4c88-8c07-106977db9583      column=info:langs, timestamp=1540982452158, value=Cobol Lisp
 19837169-fe1f-4c88-8c07-106977db9583      column=info:name, timestamp=1540982452158, value=Linda
 3 row(s) in 0.0510 seconds

 */
