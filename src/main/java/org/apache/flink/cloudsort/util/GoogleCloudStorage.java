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

package org.apache.flink.cloudsort.util;

import com.google.api.services.storage.Storage;
import com.google.api.services.storage.model.Bucket;
import com.google.api.services.storage.model.Objects;
import com.google.api.services.storage.model.StorageObject;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.List;

// see https://cloud.google.com/storage/docs/access-control/create-signed-urls-program

public class GoogleCloudStorage {

	private GoogleCloudStorage() {}

	/**
	 * Fetches the metadata for the given bucket.
	 *
	 * @param bucketName the name of the bucket to get metadata about.
	 * @return a Bucket containing the bucket's metadata.
	 */
	public static Bucket getBucket(String bucketName) throws IOException, GeneralSecurityException {
		Storage client = GoogleCloudStorageFactory.getService();

		Storage.Buckets.Get bucketRequest = client.buckets().get(bucketName);
		// Fetch the full set of the bucket's properties (e.g. include the ACLs in the response)
		bucketRequest.setProjection("full");
		return bucketRequest.execute();
	}

	/**
	 * Fetch a list of the objects within the given bucket.
	 *
	 * @param bucketName the name of the bucket to list.
	 * @return a list of the contents of the specified bucket.
	 */
	public static List<StorageObject> listBucket(String bucketName, String prefix)
			throws IOException, GeneralSecurityException {
		Storage client = GoogleCloudStorageFactory.getService();
		Storage.Objects.List listRequest = client.objects().list(bucketName).setPrefix(prefix);

		List<StorageObject> results = new ArrayList<>();
		Objects objects;

		// Iterate through each page of results, and add them to our results list.
		do {
			objects = listRequest.execute();
			// Add the items in this page of results to the list we'll return.
			results.addAll(objects.getItems());

			// Get the next page, in the next iteration of this loop.
			listRequest.setPageToken(objects.getNextPageToken());
		} while (null != objects.getNextPageToken());

		return results;
	}
}
