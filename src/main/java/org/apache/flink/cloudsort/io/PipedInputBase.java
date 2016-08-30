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

package org.apache.flink.cloudsort.io;

import com.google.api.client.repackaged.com.google.common.base.Preconditions;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.concurrent.TimeUnit;

public abstract class PipedInputBase implements PipedInput, Serializable {

	protected String bucket;

	protected String prefix;

	protected Process downloader;

	@Override
	public PipedInput setBucket(String bucket) {
		this.bucket = bucket;
		return this;
	}

	@Override
	public PipedInput setPrefix(String prefix) {
		this.prefix = prefix;
		return this;
	}

	@Override
	public InputStream getInputStream() {
		return downloader.getInputStream();
	}

	@Override
	public void close() throws IOException {
		Preconditions.checkNotNull(downloader);

		try {
			if (! downloader.waitFor(3, TimeUnit.SECONDS)) {
				downloader.destroyForcibly();
			}
		} catch (InterruptedException ignored) {
		}

		downloader = null;
	}
}
