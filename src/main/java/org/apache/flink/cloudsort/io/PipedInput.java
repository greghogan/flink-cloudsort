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
import java.io.InputStream;
import java.util.List;

/**
 * A PipedInput starts a separate process from which output is read.
 */
public interface PipedInput {

	/**
	 * Set the bucket name.
	 *
	 * @param bucket name of the bucket
	 * @return this
	 */
	PipedInput setBucket(String bucket);

	/**
	 * Set the name prepended to object names.
	 *
	 * @param prefix name of the prefix
	 * @return this
	 */
	PipedInput setPrefix(String prefix);

	/**
	 * Get the input stream connected to the process's output stream.
	 *
	 * @return the input stream
	 */
	InputStream getInputStream();

	/**
	 * List objects in the preset bucket with the preset prefix.
	 *
	 * @return list of object names
	 * @throws IOException
	 */
	List<InputSplit> list() throws IOException;

	/**
	 * Open the process pipe.
	 *
	 * @param objectName name of the object to stream
	 * @throws IOException
	 */
	void open(String objectName) throws IOException;

	/**
	 * Close the process pipe.
	 *
	 * @throws IOException
	 */
	void close() throws IOException;
}
