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

import com.amazonaws.HttpMethod;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest;
import org.apache.flink.cloudsort.io.PipedOutputBase;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.lang.ProcessBuilder.Redirect;

/**
 * Uses curl to stream a signed URL.
 */
public class AwsCurlOutput extends PipedOutputBase {

	private static final String CONTENT_TYPE = "application/octet-stream";

	@Override
	public Process open(String filename, String taskId) throws IOException {
		Preconditions.checkNotNull(bucket);
		Preconditions.checkNotNull(prefix);
		Preconditions.checkNotNull(filename);
		Preconditions.checkNotNull(taskId);

		String objectName = prefix + taskId;

		AmazonS3 s3Client = new AmazonS3Client(new ProfileCredentialsProvider());

		java.util.Date expiration = new java.util.Date();
		long msec = expiration.getTime();
		msec += 1000 * 60 * 60; // Add 1 hour.
		expiration.setTime(msec);

		GeneratePresignedUrlRequest generatePresignedUrlRequest = new GeneratePresignedUrlRequest(bucket, objectName);
		generatePresignedUrlRequest.setMethod(HttpMethod.PUT);
		generatePresignedUrlRequest.setContentType(CONTENT_TYPE);
		generatePresignedUrlRequest.setExpiration(expiration);

		String url = s3Client.generatePresignedUrl(generatePresignedUrlRequest).toExternalForm();

		return new ProcessBuilder("curl", "-s", "--max-time", "60", "--retry", "1024", "-X", "PUT", "-H", "Content-Type: " + CONTENT_TYPE, "--data-binary", "@" + filename, url)
			.redirectError(Redirect.INHERIT)
			.redirectOutput(Redirect.INHERIT)
			.start();
	}
}
