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

import com.google.api.client.repackaged.com.google.common.base.Preconditions;
import com.google.api.services.storage.model.StorageObject;
import org.apache.flink.cloudsort.io.InputSplit;
import org.apache.flink.cloudsort.io.PipedInputBase;
import org.apache.flink.cloudsort.util.GoogleCloudStorage;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.List;

/**
 * Base class for GCP inputs which can read a list of input object names.
 */
public abstract class GcpInput extends PipedInputBase {

	@Override
	public List<InputSplit> list() throws IOException {
		Preconditions.checkNotNull(bucket);
		Preconditions.checkNotNull(prefix);

		List<InputSplit> objectNames = new ArrayList<>();

		try {
			int index = 0;
			for (StorageObject o : GoogleCloudStorage.listBucket(bucket, prefix)) {
				String objectName = o.getName();
				long objectSize = o.getSize().longValue();
				objectNames.add(new InputSplit(index++, objectName, objectSize));
			}
		} catch (GeneralSecurityException ignored) {
		}

		return objectNames;
	}
}
