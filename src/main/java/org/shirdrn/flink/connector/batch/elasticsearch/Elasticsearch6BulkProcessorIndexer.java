/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.shirdrn.flink.connector.batch.elasticsearch;

import static org.apache.flink.util.Preconditions.checkNotNull;

import org.apache.flink.annotation.Internal;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;

/**
 * Implementation of a {@link RequestIndexer}, using a {@link BulkProcessor}.
 * {@link ActionRequest ActionRequests} will be buffered before sending a bulk request to the Elasticsearch cluster.
 *
 * <p>Note: This class is binary compatible to Elasticsearch 6.
 */
@Internal
class Elasticsearch6BulkProcessorIndexer implements RequestIndexer {

	private final BulkProcessor bulkProcessor;

	Elasticsearch6BulkProcessorIndexer(BulkProcessor bulkProcessor) {
		this.bulkProcessor = checkNotNull(bulkProcessor);
	}

	@Override
	public void add(DeleteRequest... deleteRequests) {
		for (DeleteRequest deleteRequest : deleteRequests) {
			this.bulkProcessor.add(deleteRequest);
		}
	}

	@Override
	public void add(IndexRequest... indexRequests) {
		for (IndexRequest indexRequest : indexRequests) {
			this.bulkProcessor.add(indexRequest);
		}
	}

	@Override
	public void add(UpdateRequest... updateRequests) {
		for (UpdateRequest updateRequest : updateRequests) {
			this.bulkProcessor.add(updateRequest);
		}
	}
}
