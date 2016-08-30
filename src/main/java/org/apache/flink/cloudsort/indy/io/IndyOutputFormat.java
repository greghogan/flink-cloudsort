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

package org.apache.flink.cloudsort.indy.io;

import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.cloudsort.indy.IndyRecord;
import org.apache.flink.cloudsort.io.PipedOutput;
import org.apache.flink.configuration.Configuration;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;

/**
 * Writes to a PipedOutput.
 */
public class IndyOutputFormat extends RichOutputFormat<Tuple1<IndyRecord>> {

	private final PipedOutput output;

	private int concurrentFiles;

	private final int bufferSize;

	private final long chunkSize;

	/** The stream to which the data is written */
	private transient OutputStream stream;

	private static volatile Semaphore lock;

	private List<Thread> activeUploaders = new ArrayList<>();

	private int currentTaskNumber;
	private int currentChunk;
	private long bytesWritten;
	private Process currentProcess;

	public IndyOutputFormat(PipedOutput output, int concurrentFiles, int bufferSize, long chunkSize) {
		this.output = output;
		this.concurrentFiles = concurrentFiles;
		this.bufferSize = bufferSize;
		this.chunkSize = chunkSize;
	}

	@Override
	public void configure(Configuration parameters) {}

	@Override
	public void open(int taskNumber, int numTasks) throws IOException {
		currentTaskNumber = taskNumber;
		currentChunk = 0;

		synchronized (this) {
			if (lock == null) {
				lock = new Semaphore(concurrentFiles, true);
			}
		}
	}

	private void openInternal() throws IOException {
		lock.acquireUninterruptibly();
		String taskId = String.format("%d/%d", currentTaskNumber, currentChunk);

		currentProcess = output.open(taskId);
		stream = new BufferedOutputStream(currentProcess.getOutputStream(), bufferSize);

		bytesWritten = 0;
	}

	@Override
	public void writeRecord(Tuple1<IndyRecord> record) throws IOException {
		if (stream == null) {
			openInternal();
		}

		record.f0.write(this.stream);

		bytesWritten += IndyRecord.LENGTH;
		if (bytesWritten >= chunkSize) {
			closeInternal();
		}
	}

	@Override
	public void close() throws IOException {
		if (stream != null) {
			closeInternal();
		}

		// close finished tasks
		for (Thread uploader : activeUploaders) {
			try {
				uploader.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	private void closeInternal() {
		// close stream and upload
		Thread thread = new Thread(new FileCloser(stream, currentProcess));
		thread.start();
		activeUploaders.add(thread);

		currentProcess = null;
		stream = null;

		currentChunk++;

		// close finished tasks
		List<Thread> stillAlive = new ArrayList<>();

		for (Thread uploader : activeUploaders) {
			if (uploader.isAlive()) {
				stillAlive.add(uploader);
			} else {
				try {
					uploader.join();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}

		this.activeUploaders = stillAlive;
	}

	private static final class FileCloser implements Runnable {
		private OutputStream stream;
		private Process outputProcess;

		public FileCloser(OutputStream stream, Process outputProcess) {
			this.stream = stream;
			this.outputProcess = outputProcess;
		}

		@Override
		public void run() {
			try {
				stream.close();
			} catch (IOException e) {
				e.printStackTrace();
			}

			try {
				outputProcess.waitFor();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

			lock.release();
		}
	}
}
