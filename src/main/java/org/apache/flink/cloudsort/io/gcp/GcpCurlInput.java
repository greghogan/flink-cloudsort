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

import org.apache.flink.cloudsort.util.GoogleCloudStorageUrlSigner;
import org.apache.flink.util.Preconditions;

import java.io.File;
import java.io.IOException;
import java.net.URLEncoder;

/**
 * Uses curl to stream a signed URL.
 */
public class GcpCurlInput extends GcpInput {

	private static final String GOOGLE_ACCESS_ID = "google-cloud-storage-apis@woven-nimbus-93515.iam.gserviceaccount.com";

	private static final String CONTENT_TYPE = "application/octet-stream";

	@Override
	public void open(String objectName) throws IOException {
		Preconditions.checkNotNull(bucket);

		// 1 hour
		long expiration = 3600 * 1000 + System.currentTimeMillis();

		String signature = null;
		try {
			signature = GoogleCloudStorageUrlSigner.signString("GET\n\n" + CONTENT_TYPE + "\n" + expiration + "\n/" + bucket + "/" + objectName);
		} catch (Exception e) {
			e.printStackTrace();
		}

		String url = "http://storage.googleapis.com/" + bucket + "/" + objectName
			+ "?GoogleAccessId=" + GOOGLE_ACCESS_ID
			+ "&Expires=" + expiration
			+ "&Signature=" + URLEncoder.encode(signature, "UTF-8");

		downloader = new ProcessBuilder("curl", "-s", "-X", "GET", "-H", "Content-Type: " + CONTENT_TYPE, url)
			.redirectError(new File("/dev/null"))
			.start();
	}
}