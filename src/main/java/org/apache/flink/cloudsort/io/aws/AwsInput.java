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

package org.apache.flink.cloudsort.io.aws;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.api.client.repackaged.com.google.common.base.Preconditions;
import org.apache.flink.cloudsort.io.InputSplit;
import org.apache.flink.cloudsort.io.PipedInputBase;

import java.util.ArrayList;
import java.util.List;

/**
 * Base class for AWS inputs which can read a list of input object names.
 */
public abstract class AwsInput extends PipedInputBase {

	@Override
	public List<InputSplit> list() {
		Preconditions.checkNotNull(bucket);
		Preconditions.checkNotNull(prefix);

		List<InputSplit> objectNames = new ArrayList<>();

		// this will read credentials from user's home directory
		AmazonS3 s3client = new AmazonS3Client(new ProfileCredentialsProvider());

		final ListObjectsV2Request req = new ListObjectsV2Request()
			.withBucketName(bucket)
			.withPrefix(prefix);

		ListObjectsV2Result result;
		int index = 0;
		do {
			result = s3client.listObjectsV2(req);

			for (S3ObjectSummary objectSummary : result.getObjectSummaries()) {
				String objectName = objectSummary.getKey();
				long objectSize = objectSummary.getSize();
				objectNames.add(new InputSplit(index++, objectName, objectSize));
			}
			req.setContinuationToken(result.getNextContinuationToken());
		} while(result.isTruncated());

		return objectNames;
	}
}
