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

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.types.CopyableValue;
import org.apache.flink.types.NormalizableKey;
import org.apache.flink.types.ResettableValue;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;

/**
 * For IndySort the record type can assume fixed key and value lengths.
 */
public class IndyRecord implements NormalizableKey<IndyRecord>, ResettableValue<IndyRecord>, CopyableValue<IndyRecord> {

	public static final int KEY_LENGTH = 10;

	public static final int VALUE_LENGTH = 90;

	public static final int LENGTH = KEY_LENGTH + VALUE_LENGTH;

	private byte[] value;

	public IndyRecord() {
		this.value = new byte[LENGTH];
	}

	public IndyRecord(IndyRecord other) {
		this.value = Arrays.copyOf(other.value, LENGTH);
	}

	public byte[] getValue() {
		return value;
	}

	// --------------------------------------------------------------------------------------------

	@Override
	public void setValue(IndyRecord source) {
		System.arraycopy(source.value, 0, this.value, 0, LENGTH);
	}

	// --------------------------------------------------------------------------------------------

	@Override
	public void read(DataInputView in) throws IOException {
		in.readFully(this.value);
	}

	public int read(InputStream in) throws IOException {
		int bytesRead = 0;
		while (bytesRead < LENGTH) {
			int count = in.read(this.value, bytesRead, LENGTH - bytesRead);
			if (count < 0) {
				throw new EOFException();
			}

			bytesRead += count;
		}

		return LENGTH;
	}

	@Override
	public void write(DataOutputView out) throws IOException {
		out.write(this.value);
	}

	public void write(OutputStream out) throws IOException {
		out.write(this.value);
	}

	// --------------------------------------------------------------------------------------------

	@Override
	public int compareTo(IndyRecord other) {
		return compareKeyTo(other.getValue());
	}

	public int compareKeyTo(byte[] other) {
		for (int i = 0; i < KEY_LENGTH; i++) {
			// perform unsigned comparison
			int cmp = Integer.compare(this.value[i] & 0xff, other[i] & 0xff);
			if (cmp != 0) {
				return cmp;
			}
		}

		return 0;
	}

	@Override
	public int hashCode() {
		int hashCode = 0;

		for (int i = 0; i < KEY_LENGTH; i++) {
			hashCode = 43 * hashCode + this.value[i];
		}

		return hashCode;
	}

	@Override
	public boolean equals(final Object obj) {
		if (! (obj instanceof IndyRecord)) {
			return false;
		}

		IndyRecord other = (IndyRecord) obj;

		for (int i = 0; i < KEY_LENGTH; i++) {
			if (this.value[i] != other.value[i]) {
				return false;
			}
		}

		return true;
	}

	// --------------------------------------------------------------------------------------------

	@Override
	public int getMaxNormalizedKeyLen() {
		return KEY_LENGTH;
	}

	@Override
	public void copyNormalizedKey(MemorySegment target, int offset, int len) {
		// copy key up to the requested length
		int toCopy = Math.min(len, KEY_LENGTH);
		for (int i = 0; i < toCopy; i++) {
			target.put(offset, this.value[i]);
			offset++;
		}

		// pad with zeroes
		for (int i = len - KEY_LENGTH; i > 0; i--) {
			target.put(offset, (byte) 0);
			offset++;
		}
	}

	// --------------------------------------------------------------------------------------------

	@Override
	public int getBinaryLength() {
		return LENGTH;
	}

	@Override
	public void copyTo(IndyRecord target) {
		System.arraycopy(this.value, 0, target.value, 0, LENGTH);
	}

	@Override
	public IndyRecord copy() {
		return new IndyRecord(this);
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		// CopyableValues perform this copy function using
		//   `target.write(source, LENGTH)`
		// which performs the copy using long- and byte-sized copies which is
		// for an as-yet-undetermined reason not optimally inlined by the compiler

		byte[] tmp = new byte[LENGTH];
		source.read(tmp);
		target.write(tmp);
	}
}
