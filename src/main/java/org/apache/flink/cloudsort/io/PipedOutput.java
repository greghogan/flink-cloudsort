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

import java.io.IOException;

/**
 * A PipedOutput starts a separate process to which input is written.
 */
public interface PipedOutput {

	/**
	 * Set the bucket name.
	 *
	 * @param bucket name of the bucket
	 * @return this
	 */
	PipedOutput setBucket(String bucket);

	/**
	 * Set the name prepended to object names.
	 *
	 * @param prefix name of the prefix
	 * @return this
	 */
	PipedOutput setPrefix(String prefix);

	/**
	 * Set the object storage class.
	 *
	 * @param storageClass object storage class
	 * @return this
	 */
	PipedOutput setStorageClass(String storageClass);

	/**
	 * Open the process pipe.
	 *
	 * @param taskId name of the current task, typically "taskNumber-partNumber"
	 * @return the new output process
	 * @throws IOException
	 */
	Process open(String taskId) throws IOException;
}
