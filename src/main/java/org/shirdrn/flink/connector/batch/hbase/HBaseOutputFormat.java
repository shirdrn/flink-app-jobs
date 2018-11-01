/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.shirdrn.flink.connector.batch.hbase;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

import com.google.common.base.Throwables;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.ReflectionUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Internal
public class HBaseOutputFormat<T> extends RichOutputFormat<T> {

	private static final Logger LOG = LoggerFactory.getLogger(HBaseOutputFormat.class);

	private static final String CONFIG_KEY_TABLE_NAME = "hbase.table.name";
	// hbase.client.write.buffer, default 2097152 = 2M
	private static final String CONFIG_KEY_WRITE_BUFFER_SIZE = "hbase.client.write.buffer";
	// hbase.client.keyvalue.maxsize, default 10485760 = 10M
	private static final String CONFIG_KEY_MAX_KEY_VALUE_SIZE = "hbase.client.keyvalue.maxsize";
	private static final String CONFIG_KEY_RETRIES_NUMBER = "hbase.client.retries.number";
	private static final String CONFIG_KEY_HBASE_ROOT_DIR = "hbase.rootdir";
	private static final String CONFIG_KEY_HBASE_CLUSTER_DISTRIBUTED = "hbase.cluster.distributed";
	private static final String CONFIG_KEY_HBASE_ZOOKEEPER_QUORUM = "hbase.zookeeper.quorum";
	private static final String CONFIG_KEY_BM_EXCEPTION_LISTENER_CLASS = "bm.exception.listener.class";

	private String tableName;
	private long writeBufferSize;
	private int maxKeyValueSize;
	private int retriesNumber;
	private final Map<String, String> userConfig;
	private final HBaseSinkFunction<T> hbaseSinkFunction;

	private transient Connection connection;
	private transient BufferedMutator bufferedMutator;
	private final AtomicReference<Throwable> failureThrowable = new AtomicReference<>();

	public HBaseOutputFormat(
			Map<String, String> userConfig,
			HBaseSinkFunction<T> hbaseSinkFunction) {
		this.hbaseSinkFunction = checkNotNull(hbaseSinkFunction);
		checkArgument(InstantiationUtil.isSerializable(hbaseSinkFunction),
			"The implementation of the provided HBaseSinkFunction is not serializable. " +
				"The object probably contains or references non-serializable fields.");

		checkNotNull(userConfig);
		// copy config so we can remove entries without side-effects
		userConfig = new HashMap<>(userConfig);
		ParameterTool params = ParameterTool.fromMap(userConfig);

		if (params.has(CONFIG_KEY_TABLE_NAME)) {
			tableName = params.get(CONFIG_KEY_TABLE_NAME);
			userConfig.remove(CONFIG_KEY_TABLE_NAME);
		}

		if (params.has(CONFIG_KEY_WRITE_BUFFER_SIZE)) {
			writeBufferSize = params.getLong(CONFIG_KEY_WRITE_BUFFER_SIZE);
			userConfig.remove(CONFIG_KEY_WRITE_BUFFER_SIZE);
		}

		if (params.has(CONFIG_KEY_MAX_KEY_VALUE_SIZE)) {
			maxKeyValueSize = params.getInt(CONFIG_KEY_MAX_KEY_VALUE_SIZE);
			userConfig.remove(CONFIG_KEY_MAX_KEY_VALUE_SIZE);
		}

		if (params.has(CONFIG_KEY_RETRIES_NUMBER)) {
			retriesNumber = params.getInt(CONFIG_KEY_RETRIES_NUMBER);
			userConfig.remove(CONFIG_KEY_RETRIES_NUMBER);
		}

		checkNotNull(tableName);
		this.userConfig = userConfig;
	}

	@Override
	public void configure(Configuration parameters) {

	}

	@Override
	public void open(int taskNumber, int numberTasks) throws IOException {
		// configure hbase
		ExecutionConfig.GlobalJobParameters globalParams =
				getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
		Configuration globConf = (Configuration) globalParams;
		final org.apache.hadoop.conf.Configuration hbaseConf = HBaseConfiguration.create();
		hbaseConf.set(CONFIG_KEY_HBASE_ROOT_DIR,
				globConf.getString(CONFIG_KEY_HBASE_ROOT_DIR, null));
		hbaseConf.set(CONFIG_KEY_HBASE_CLUSTER_DISTRIBUTED,
				globConf.getString(CONFIG_KEY_HBASE_CLUSTER_DISTRIBUTED, "true"));
		hbaseConf.set(CONFIG_KEY_HBASE_ZOOKEEPER_QUORUM,
				globConf.getString(CONFIG_KEY_HBASE_ZOOKEEPER_QUORUM, null));
		connection = ConnectionFactory.createConnection(hbaseConf);

		String bmExceptionListenerClazz =
				globConf.getString(CONFIG_KEY_BM_EXCEPTION_LISTENER_CLASS, DefaultBMExceptionListener.class.getName());
		LOG.info("Buffered mutator exception listener class: " + bmExceptionListenerClazz);
		try {
			final BufferedMutator.ExceptionListener listener = (BufferedMutator.ExceptionListener)
					ReflectionUtil.newInstance(Class.forName(bmExceptionListenerClazz));
			LOG.info("Buffered mutator exception listener created: " + listener);
			final BufferedMutatorParams params =
					new BufferedMutatorParams(TableName.valueOf(tableName))
							.listener(listener);
			bufferedMutator = connection.getBufferedMutator(params);
		} catch (ClassNotFoundException e) {
			Throwables.propagate(e);
		}
	}

	@Override
	public void writeRecord(T value) throws IOException {
		checkErrorAndRethrow();
		hbaseSinkFunction.process(value, getRuntimeContext(), bufferedMutator);
		// disable this log output by changing log level to debug in real production env
		LOG.info("HBase update: " + value);
	}

	@Override
	public void close() throws IOException {
		bufferedMutator.flush();
		if (connection != null) {
			try {
				connection.close();
			} catch (Exception e) {
				throw new RuntimeException("Fail to close connection: " + connection, e);
			}
		}

		// make sure any errors from callbacks are rethrown
		checkErrorAndRethrow();
	}

	private void checkErrorAndRethrow() {
		Throwable cause = failureThrowable.get();
		if (cause != null) {
			throw new RuntimeException("An error occurred: ", cause);
		}
	}

	@PublicEvolving
	public static class Builder<T> {

		private final HBaseSinkFunction<T> hbaseSinkFunction;
		private final Map<String, String> config = new HashMap<>();

		public Builder(HBaseSinkFunction<T> hbaseSinkFunction) {
			this.hbaseSinkFunction = Preconditions.checkNotNull(hbaseSinkFunction);
		}

		public Builder setHBaseTableName(String tableName) {
			Preconditions.checkNotNull(
					tableName, "HBase table name must be provided.");
			LOG.info("Builder config: tableName=" + tableName);
			this.config.put(CONFIG_KEY_TABLE_NAME, tableName);
			return this;
		}

		public Builder setHBaseWriteBufferSize(int writeBufferSize) {
			Preconditions.checkArgument(
					writeBufferSize > 0, "Write buffer size must be larger than 0, default 2097152(2M).");
			LOG.info("Builder config: writeBufferSize=" + writeBufferSize);
			this.config.put(CONFIG_KEY_WRITE_BUFFER_SIZE, String.valueOf(writeBufferSize));
			return this;
		}

		public Builder setHBaseMaxKeyValueSize(long maxKeyValueSize) {
			Preconditions.checkArgument(
					maxKeyValueSize > 0, "Max key value size must be larger than 0, default 10485760(10M).");
			LOG.info("Builder config: maxKeyValueSize=" + maxKeyValueSize);
			this.config.put(CONFIG_KEY_MAX_KEY_VALUE_SIZE, String.valueOf(maxKeyValueSize));
			return this;
		}

		public Builder setHBaseRetriesNumber(int retriesNumber) {
			Preconditions.checkArgument(
					retriesNumber > 0, "Retries number must be larger than 0, default 35.");
			LOG.info("Builder config: retriesNumber=" + retriesNumber);
			this.config.put(CONFIG_KEY_RETRIES_NUMBER, String.valueOf(retriesNumber));
			return this;
		}

		public HBaseOutputFormat<T> build() {
			return new HBaseOutputFormat<>(config, hbaseSinkFunction);
		}
	}

}
