/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cloudsort.indy;

import org.apache.flink.api.common.accumulators.ListAccumulator;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.cloudsort.util.PureJavaCrc32;
import org.apache.flink.cloudsort.util.Unsigned16;
import org.apache.flink.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Array;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.zip.Checksum;

/**
 * Validates the sorted output and counts duplicate keys as required by sortbenchmark.org.
 *
 * The validation process:
 * 1) compute checksum as the sum over crc32 on records
 * 2) keys from each partition are in order
 * 3) partitions are non-overlapping
 */
public class IndyValidate extends RichMapFunction<Tuple1<IndyRecord>, Tuple1<IndyRecord>> {

	private static final Logger LOG = LoggerFactory.getLogger(IndyValidate.class);

	// accumulator ID
	private final String id;

	// metrics from validation of partition's sorted data
	private Summary summary = new Summary();

	private Unsigned16 tmp = new Unsigned16();

	private Checksum crc32  = new PureJavaCrc32();

	private Unsigned16 one = new Unsigned16(1);

	public IndyValidate(String id) {
		this.id = id;
	}

	@Override
	public Tuple1<IndyRecord> map(Tuple1<IndyRecord> value) {
		byte[] recordBytes = value.f0.getValue();

		String misordered = null;

		// validate
		if (summary.firstKey == null) {
			summary.firstKey = Arrays.copyOf(recordBytes, IndyRecord.KEY_LENGTH);
			summary.lastKey = Arrays.copyOf(recordBytes, IndyRecord.KEY_LENGTH);
		} else {
			int cmp = value.f0.compareKeyTo(summary.lastKey);

			if (cmp == 0) {
				// increment duplicate count
				summary.duplicateCount.add(one);
			} else if (cmp < 0) {
				// records are out-of-order ... print to logs below once we
				// have a byte array containing only the key
				misordered = StringUtils.byteToHexString(summary.lastKey);
				summary.unorderedCount.add(one);
			}
		}

		// increment record count
		summary.recordCount.add(one);

		// add checksum
		crc32.reset();
		crc32.update(recordBytes, 0, recordBytes.length);
		tmp.set(crc32.getValue());
		summary.checksum.add(tmp);

		// update last record seen
		Array.copy(recordBytes, 0, summary.lastKey, 0, IndyRecord.KEY_LENGTH);

		if (misordered != null) {
			LOG.error(misordered + " and " +  StringUtils.byteToHexString(summary.lastKey) + " are out-of-order");
		}

		return value;
	}

	@Override
	public void close() {
		ListAccumulator<Summary> accumulator = new ListAccumulator<>();
		accumulator.add(summary);
		getRuntimeContext().addAccumulator(id, accumulator);
	}

	public static class Summary implements Comparable<Summary>, Serializable {
		// total number of unordered records
		private Unsigned16 unorderedCount = new Unsigned16();

		// total number of records
		private Unsigned16 recordCount = new Unsigned16();

		// total number of dupliate keys
		private Unsigned16 duplicateCount = new Unsigned16();

		// checksum of all records
		private Unsigned16 checksum = new Unsigned16();

		// first record
		private byte[] firstKey = null;

		// last record
		private byte[] lastKey = new byte[IndyRecord.KEY_LENGTH];

		// combine counts and verify that partitions are non-overlapping;
		// summaries must be sorted before merging
		public Summary merge(Summary other) {
			int cmp = 0;
			for (int i = 0; i < IndyRecord.KEY_LENGTH; i++) {
				// perform unsigned comparison
				cmp = Integer.compare(lastKey[i] & 0xff, other.lastKey[i] & 0xff);
				if (cmp != 0) {
					break;
				}
			}

			if (cmp == 0) {
				LOG.error("Same key in different partitions: " + StringUtils.byteToHexString(lastKey));
				duplicateCount.add(new Unsigned16(1));
			} else if (cmp > 0 ) {
				LOG.error(StringUtils.byteToHexString(lastKey) + " and " +  StringUtils.byteToHexString(other.lastKey) + " are out-of-order");
				unorderedCount.add(new Unsigned16(1));
			}

			unorderedCount.add(other.unorderedCount);
			recordCount.add(other.recordCount);
			duplicateCount.add(other.duplicateCount);
			checksum.add(other.checksum);
			Array.copy(other.firstKey, 0, lastKey, 0, IndyRecord.KEY_LENGTH);

			return this;
		}

		@Override
		public int compareTo(Summary other) {
			for (int i = 0; i < IndyRecord.KEY_LENGTH; i++) {
				// perform unsigned comparison
				int cmp = Integer.compare(lastKey[i] & 0xff, other.lastKey[i] & 0xff);
				if (cmp != 0) {
					break;
				}
			}

			return 0;
		}

		@Override
		public String toString() {
			return new StringBuilder()
				.append("first key: ")
				.append(StringUtils.byteToHexString(firstKey))
				.append(", last key: ")
				.append(StringUtils.byteToHexString(lastKey))
				.append(", checksum: ")
				.append(checksum)
				.append(", record count: ")
				.append(recordCount.getLow8())
				.append(", duplicate count: ")
				.append(duplicateCount.getLow8())
				.append(", unordered count: ")
				.append(unorderedCount.getLow8())
				.toString();
		}
	}
}
