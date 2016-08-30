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

package org.apache.flink.cloudsort.io.gcp;

import org.apache.flink.cloudsort.io.PipedOutputBase;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.lang.ProcessBuilder.Redirect;

/**
 * Uses gsutil to stream files.
 */
public class GcpGsutilOutput extends PipedOutputBase {

	@Override
	public Process open(String taskId) throws IOException {
		Preconditions.checkNotNull(bucket);
		Preconditions.checkNotNull(prefix);
		Preconditions.checkNotNull(taskId);

		String objectName = prefix + taskId;

		return new ProcessBuilder("gsutil", "cp", "-", "gs://" + bucket + "/" + objectName)
			.redirectError(Redirect.INHERIT)
			.redirectOutput(Redirect.INHERIT)
			.start();
	}
}
