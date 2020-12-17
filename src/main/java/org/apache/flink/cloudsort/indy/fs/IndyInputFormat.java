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

package org.apache.flink.cloudsort.indy.fs;

import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.cloudsort.indy.IndyRecord;
import org.apache.flink.core.fs.FileInputSplit;

import java.io.IOException;

/**
 * This InputFormat overrides FileInputFormat in order to read records of
 * type IndyRecord and to align FileInputSplits to the start of a record.
 */
public class IndyInputFormat extends FileInputFormat<Tuple1<IndyRecord>> {

	// whether end-of-file has been reached
	private transient boolean end;

	// track bytes read to compare with length in InputSplit
	private transient int bytesRead;

	@Override
	public void open(FileInputSplit fileSplit) throws IOException {
		long start = fileSplit.getStart();
		long length = fileSplit.getLength();

		// align start and length to the previous record
		long alignedStart = IndyRecord.LENGTH * (start / IndyRecord.LENGTH);
		long alignedLength = IndyRecord.LENGTH * ((start + length - alignedStart) / IndyRecord.LENGTH);

		if (alignedLength == 0) {
			end = true;
		} else {
			super.open(new FileInputSplit(fileSplit.getSplitNumber(), fileSplit.getPath(), alignedStart, alignedLength, fileSplit.getHostnames()));

			end = false;
			bytesRead = 0;
		}
	}

	@Override
	public Tuple1<IndyRecord> nextRecord(Tuple1<IndyRecord> reuse) throws IOException {
		if (end) {
			return null;
		}

		bytesRead += reuse.f0.read(stream);

		// have reached the end of our input split
		if (bytesRead >= splitLength) {
			// validate that the correct number of bytes have been read
			assert bytesRead == splitLength;
			end = true;
		}

		return reuse;
	}

	@Override
	public boolean reachedEnd() {
		return end;
	}
}
