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

import org.apache.flink.api.common.io.FileOutputFormat;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.cloudsort.indy.IndyRecord;

import java.io.IOException;

/**
 * This OutputFormat overrides FileOutputFormat in order to write records of
 * type IndyRecord.
 */
public class IndyOutputFormat extends FileOutputFormat<Tuple1<IndyRecord>> {

	@Override
	public void writeRecord(Tuple1<IndyRecord> record) throws IOException {
		record.f0.write(stream);
	}
}
