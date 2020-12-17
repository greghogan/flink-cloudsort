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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.cloudsort.util.GenSort;
import org.apache.flink.cloudsort.util.PureJavaCrc32;
import org.apache.flink.cloudsort.util.Random16;
import org.apache.flink.cloudsort.util.Unsigned16;
import org.apache.flink.types.LongValue;

import java.util.zip.Checksum;

/**
 * This is based on Hadoop's TeraGen and is only partially implemented.
 * The data generated looks to be from an old version of gensort and should
 * not be used for submission to sortbenchmark.org.
 */
public class IndyGen implements MapFunction<LongValue, Tuple1<IndyRecord>> {

	private IndyRecord record = new IndyRecord();
	private Tuple1<IndyRecord> result = new Tuple1<>(record);

	private Unsigned16 rand = null;
	private Unsigned16 rowId = null;
	private Unsigned16 checksum = new Unsigned16();
	private Checksum crc32 = new PureJavaCrc32();
	private Unsigned16 total = new Unsigned16();
	private static final Unsigned16 ONE = new Unsigned16(1);
	private byte[] buffer = new byte[IndyRecord.LENGTH];

	@Override
	public Tuple1<IndyRecord> map(LongValue value) {
		if (rand == null) {
			rowId = new Unsigned16(value.getValue());
			rand = Random16.skipAhead(rowId);
		}
		Random16.nextRand(rand);

		GenSort.generateRecord(record.getValue(), rand, rowId);

		crc32.reset();
		crc32.update(buffer, 0, IndyRecord.LENGTH);
		checksum.set(crc32.getValue());

		total.add(checksum);
		rowId.add(ONE);

		return result;
	}
}
