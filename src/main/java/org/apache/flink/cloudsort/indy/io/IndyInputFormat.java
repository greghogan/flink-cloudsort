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

package org.apache.flink.cloudsort.indy.io;

import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.cloudsort.indy.IndyRecord;
import org.apache.flink.cloudsort.io.InputSplit;
import org.apache.flink.cloudsort.io.PipedInput;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplitAssigner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

/**
 * Reads from a PipedInput.
 */
public class IndyInputFormat extends RichInputFormat<Tuple1<IndyRecord>, InputSplit> {

	private static final Logger LOG = LoggerFactory.getLogger(IndyInputFormat.class);

	private final PipedInput input;

	/** The stream from which the data is read */
	private transient InputStream stream;

	private final int bufferSize;

	private transient boolean end;

	// name of object in case the input stream closes and must be reopened
	private transient String objectName;

	// number of bytes to be read from the current storage object
	private transient long objectSize;

	// track bytes read to compare with length of storage object
	private transient int bytesRead;

	public IndyInputFormat(PipedInput input, int bufferSize) {
		this.input = input;
		this.bufferSize = bufferSize;
	}

	@Override
	public void configure(Configuration parameters) {}

	@Override
	public BaseStatistics getStatistics(BaseStatistics cachedStatistics) throws IOException {
		return null;
	}

	@Override
	public InputSplit[] createInputSplits(int minNumSplits) throws IOException {
		List<InputSplit> inputSplits = input.list();

		long byteCount = inputSplits.stream().map(InputSplit::getObjectSize).reduce(0L, Long::sum);
		LOG.info("Processing {} bytes in {} files", byteCount, inputSplits.size());

		return inputSplits.toArray(new InputSplit[inputSplits.size()]);
	}

	@Override
	public InputSplitAssigner getInputSplitAssigner(InputSplit[] inputSplits) {
		return new DefaultInputSplitAssigner(inputSplits);
	}

	@Override
	public void open(InputSplit inputSplit) throws IOException {
		objectName = inputSplit.getObjectName();
		objectSize = inputSplit.getObjectSize();

		if (objectSize == 0) {
			end = true;
		} else {
			input.open(inputSplit.getObjectName());
			stream = new BufferedInputStream(input.getInputStream(), bufferSize);

			end = false;
			bytesRead = 0;
		}
	}

	@Override
	public Tuple1<IndyRecord> nextRecord(Tuple1<IndyRecord> reuse) throws IOException {
		if (end) {
			return null;
		}

		try {
			bytesRead += reuse.f0.read(stream);

			if (bytesRead >= objectSize) {
				// have reached the end of our input split so validate
				// that the correct number of bytes have been read
				assert bytesRead == objectSize;
				end = true;
			}
		} catch (EOFException ex) {
			close();

			// reopen the stream and skip to current location
			input.open(objectName);
			stream = input.getInputStream();

			byte[] buf = new byte[8192];
			long bytesToSkip = bytesRead;

			while (bytesToSkip > 0) {
				int bytesToRead = (int)Long.min(buf.length, bytesToSkip);
				bytesToSkip -= stream.read(buf, 0, bytesToRead);
			}

			return nextRecord(reuse);
		}

		return reuse;
	}

	@Override
	public void close() throws IOException {
		stream.close();
		stream = null;
		input.close();
	}

	@Override
	public boolean reachedEnd() throws IOException {
		return end;
	}
}
