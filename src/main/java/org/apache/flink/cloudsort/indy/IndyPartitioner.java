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

package org.apache.flink.cloudsort.indy;

import org.apache.flink.api.common.functions.Partitioner;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Arrays;

/**
 * A partitioner based on org.apache.hadoop.examples.terasort.TeraSort
 * with Text replaced with byte arrays.
 */
public class IndyPartitioner implements Partitioner<IndyRecord> {

	private static final int MAX_DEPTH = 3;

	private int lastNumPartitions = 0;

	private TrieNode trie;

	@Override
	public int partition(IndyRecord key, int numPartitions) {
		// only create the trie when the number of partitions has changed
		if (numPartitions != lastNumPartitions) {
			lastNumPartitions = numPartitions;

			byte[][] splitPoints = new byte[numPartitions-1][MAX_DEPTH];

			// this implementation creates partitions of equal size
			for (int i = 1 ; i < numPartitions ; i++) {
				long l = (long)((1.0d * i / numPartitions) * (1L << (8*MAX_DEPTH)));

				byte[] splitPoint = splitPoints[i-1];
				for (int j = MAX_DEPTH-1; j >= 0; j--) {
					splitPoint[j] = (byte)(l & 0xFF);
					l >>= 8;
				}
			}

			trie = buildTrie(splitPoints, 0, splitPoints.length, new byte[0], MAX_DEPTH);
		}

		return trie.findPartition(key.getValue());
	}

	/**
	 * A generic trie node
	 */
	static abstract class TrieNode {
		private int level;
		TrieNode(int level) {
			this.level = level;
		}
		abstract int findPartition(byte[] key);
		abstract void print(PrintStream strm) throws IOException;
		int getLevel() {
			return level;
		}
	}

	/**
	 * An inner trie node that contains 256 children based on the next
	 * character.
	 */
	static class InnerTrieNode extends TrieNode {
		private TrieNode[] child = new TrieNode[256];

		InnerTrieNode(int level) {
			super(level);
		}
		int findPartition(byte[] key) {
			int level = getLevel();
			if (key.length <= level) {
				return child[0].findPartition(key);
			}
			return child[key[level] & 0xff].findPartition(key);
		}
		void setChild(int idx, TrieNode child) {
			this.child[idx] = child;
		}
		void print(PrintStream strm) throws IOException {
			for(int ch=0; ch < 256; ++ch) {
				for(int i = 0; i < 2*getLevel(); ++i) {
					strm.print(' ');
				}
				strm.print(ch);
				strm.println(" ->");
				if (child[ch] != null) {
					child[ch].print(strm);
				}
			}
		}
	}

	private static int compare(byte[] left, byte[] right) {
		int length = Math.min(left.length, right.length);

		for (int i = 0; i < length; i++) {
			int cmp = Integer.compare(left[i] & 0xff, right[i] & 0xff);
			if (cmp != 0) {
				return cmp;
			}
		}

		return Integer.compare(left.length, right.length);
	}

	/**
	 * A leaf trie node that does string compares to figure out where the given
	 * key belongs between lower..upper.
	 */
	static class LeafTrieNode extends TrieNode {
		int lower;
		int upper;
		byte[][] splitPoints;
		LeafTrieNode(int level, byte[][] splitPoints, int lower, int upper) {
			super(level);
			this.splitPoints = splitPoints;
			this.lower = lower;
			this.upper = upper;
		}
		int findPartition(byte[] key) {
			for(int i=lower; i<upper; ++i) {
				if (compare(splitPoints[i], key) >= 0) {
					return i;
				}
			}
			return upper;
		}
		void print(PrintStream strm) {
			for(int i = 0; i < 2*getLevel(); ++i) {
				strm.print(' ');
			}
			strm.print(lower);
			strm.print(", ");
			strm.println(upper);
		}
	}

	/**
	 * Given a sorted set of cut points, build a trie that will find the correct
	 * partition quickly.
	 * @param splits the list of cut points
	 * @param lower the lower bound of partitions 0..numPartitions-1
	 * @param upper the upper bound of partitions 0..numPartitions-1
	 * @param prefix the prefix that we have already checked against
	 * @param maxDepth the maximum depth we will build a trie for
	 * @return the trie node that will divide the splits correctly
	 */
	private static TrieNode buildTrie(byte[][] splits, int lower, int upper, byte[] prefix, int maxDepth) {
		int depth = prefix.length;
		if (depth >= maxDepth || lower == upper) {
			return new LeafTrieNode(depth, splits, lower, upper);
		}
		InnerTrieNode result = new InnerTrieNode(depth);
		byte[] trial = Arrays.copyOf(prefix, depth + 1);
		int currentBound = lower;
		for(int ch = 0; ch < 255; ++ch) {
			trial[depth] = (byte) (ch + 1);
			lower = currentBound;
			while (currentBound < upper) {
				if (compare(splits[currentBound], trial) >= 0) {
					break;
				}
				currentBound += 1;
			}
			trial[depth] = (byte) ch;
			result.child[ch] = buildTrie(splits, lower, currentBound, trial, maxDepth);
		}
		// pick up the rest
		trial[depth] = (byte) 255;
		result.child[255] = buildTrie(splits, currentBound, upper, trial, maxDepth);
		return result;
	}
}
