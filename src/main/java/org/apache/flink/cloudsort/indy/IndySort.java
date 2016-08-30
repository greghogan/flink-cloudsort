/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *	 http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cloudsort.indy;

import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.cloudsort.indy.IndyValidate.Summary;
import org.apache.flink.cloudsort.io.PipedInput;
import org.apache.flink.cloudsort.io.PipedOutput;
import org.apache.flink.cloudsort.io.aws.AwsCliInput;
import org.apache.flink.cloudsort.io.aws.AwsCliOutput;
import org.apache.flink.cloudsort.io.aws.AwsCurlInput;
import org.apache.flink.cloudsort.io.aws.AwsCurlOutput;
import org.apache.flink.cloudsort.io.gcp.GcpGsutilInput;
import org.apache.flink.cloudsort.io.gcp.GcpGsutilOutput;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.types.LongValue;
import org.apache.flink.util.AbstractID;
import org.apache.flink.util.LongValueSequenceIterator;

import java.util.List;

/**
 * Driver for the sortbenchmark.org Indy Cloudsort. Input files are read from
 * and output files written to cloud object storage, either Amazon S3 or Google
 * Cloud Storage.
 *
 * The sort has two phases. In the first phase data is read, partitioned, sorted,
 * and spilled to disk. In the second phase data is read from disk, merge-sorted,
 * and written to output.
 *
 * As cluster size increases the ratio of records sent to a remote partition
 * approaches 1.0. In phase 1 each record is transmitted over the network three
 * times while in phase 2 each record is transmitted over the network once.
 */
public class IndySort {

	private static void printUsage() {
		System.out.println("usage: IndySort <OPTIONS> --input <INPUT> --output <OUTPUT>");
		System.out.println();
		System.out.println("options:");
		System.out.println("  --buffer_size BUFFER_SIZE");
		System.out.println("  --chunk_size CHUNK_SIZE");
		System.out.println("  --concurrent_files CONCURRENT_FILES");
		System.out.println();
		System.out.println("input:");
		System.out.println("  --input fs --input_path PATH");
		System.out.println("  --input awscli --input_bucket BUCKET --input_prefix PREFIX");
		System.out.println("  --input awscurl --input_bucket BUCKET --input_prefix PREFIX");
		System.out.println("  --input gcp --input_bucket BUCKET --input_prefix PREFIX");
		System.out.println("  --input generate --input_count NUMBER_OF_RECORDS");
		System.out.println();
		System.out.println("output:");
		System.out.println("  --output fs --output_path PATH");
		System.out.println("  --output awscli --output_bucket BUCKET --output_prefix PREFIX --storage_class [STANDARD | REDUCED_REDUNDANCY]");
		System.out.println("  --output awscurl --output_bucket BUCKET --output_prefix PREFIX");
		System.out.println("  --output gcp --output_bucket BUCKET --output_prefix PREFIX");
		System.out.println("  --output discard");

		System.exit(-1);
	}

	public static void main(String[] args) throws Exception {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().enableObjectReuse();

		ParameterTool parameters = ParameterTool.fromArgs(args);

		DataSet<Tuple1<IndyRecord>> input;

		int bufferSize = parameters.getInt("buffer_size");

		switch (parameters.get("input", "")) {
			case "awscli": {
				PipedInput pipedInput = new AwsCliInput()
					.setBucket(parameters.get("input_bucket"))
					.setPrefix(parameters.get("input_prefix"));
				input = env
					.createInput(new org.apache.flink.cloudsort.indy.io.IndyInputFormat(pipedInput, bufferSize))
					.name("Amazon Web Services [CLI]");
				} break;

			case "awscurl": {
				PipedInput pipedInput = new AwsCurlInput()
					.setBucket(parameters.get("input_bucket"))
					.setPrefix(parameters.get("input_prefix"));
				input = env
					.createInput(new org.apache.flink.cloudsort.indy.io.IndyInputFormat(pipedInput, bufferSize))
					.name("Amazon Web Services [curl]");
			} break;

			case "fs": {
				String path = parameters.get("input_path");
				input = env
					.readFile(new org.apache.flink.cloudsort.indy.fs.IndyInputFormat(), path)
						.name("Filesystem");
				} break;

			case "gcp": {
				PipedInput pipedInput = new GcpGsutilInput()
					.setBucket(parameters.get("input_bucket"))
					.setPrefix(parameters.get("input_prefix"));
				input = env
					.createInput(new org.apache.flink.cloudsort.indy.io.IndyInputFormat(pipedInput, bufferSize))
						.name("Google Cloud Storage");
			} break;

			case "generate": {
				long count = parameters.getLong("input_count");

				LongValueSequenceIterator iterator = new LongValueSequenceIterator(0, count - 1);

				input = env
					.fromParallelCollection(iterator, LongValue.class)
					.map(new IndyGen())
						.name("Generate");
			} break;

			default:
				printUsage();
				return;
		}

		String validateID = new AbstractID().toString();

		// the partition, sort, and validation operations
		DataSet<Tuple1<IndyRecord>> sorted = input
			.partitionCustom(new IndyPartitioner(), 0)
			.sortPartition(0, Order.ASCENDING)
			.map(new IndyValidate(validateID))
				.name("Validate");

		int concurrent_files = parameters.getInt("concurrent_files");

		// align chunk_size on record boundaries
		long chunkSize = parameters.getLong("chunk_size", Long.MAX_VALUE);
		chunkSize = IndyRecord.LENGTH * (chunkSize / IndyRecord.LENGTH);

		switch (parameters.get("output", "")) {
			case "awscli": {
				PipedOutput pipedOutput = new AwsCliOutput()
					.setBucket(parameters.get("output_bucket"))
					.setPrefix(parameters.get("output_prefix"))
					.setStorageClass(parameters.get("storage_class"));
				sorted
					.output(new org.apache.flink.cloudsort.indy.io.IndyOutputFormat(pipedOutput, concurrent_files, bufferSize, chunkSize))
					.name("Amazon Web Services [CLI]");
				} break;

			case "awscurl": {
				PipedOutput pipedOutput = new AwsCurlOutput()
					.setBucket(parameters.get("output_bucket"))
					.setPrefix(parameters.get("output_prefix"));
				sorted
					.output(new org.apache.flink.cloudsort.indy.io.IndyOutputFormat(pipedOutput, concurrent_files, bufferSize, chunkSize))
					.name("Amazon Web Services [curl]");
			} break;

			case "fs": {
				String path = parameters.get("output_path");
				sorted
					.write(new org.apache.flink.cloudsort.indy.fs.IndyOutputFormat(), path, WriteMode.OVERWRITE)
						.name("Filesystem");
			} break;

			case "gcp": {
				PipedOutput pipedOutput = new GcpGsutilOutput()
					.setBucket(parameters.get("output_bucket"))
					.setPrefix(parameters.get("output_prefix"));
				sorted
					.output(new org.apache.flink.cloudsort.indy.io.IndyOutputFormat(pipedOutput, concurrent_files, bufferSize, chunkSize))
						.name("Google Cloud Storage");
				} break;

			case "discard":
				sorted
					.output(new DiscardingOutputFormat<>())
						.name("Discard");
				break;

			default:
				printUsage();
				return;
		}

		env.execute();

		// sort and merge validation summaries and print results
		List<Summary> summaries = env.getLastJobExecutionResult().getAccumulatorResult(validateID);
		System.out.println(summaries.stream().sorted().reduce(Summary::merge));
	}
}
