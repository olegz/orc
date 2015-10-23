/**
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
package org.apache.orc.serde;

import org.apache.hadoop.io.WritableUtils;

/**
 * LazyBinaryUtils.
 *
 */
public final class LazyBinaryUtils {

	/**
	 * A zero-compressed encoded integer.
	 */
	public static class VInt {
		public VInt() {
			value = 0;
			length = 0;
		}

		public int value;
		public byte length;
	};

	public static final ThreadLocal<VInt> threadLocalVInt = new ThreadLocal<VInt>() {
		@Override
		protected VInt initialValue() {
			return new VInt();
		}
	};

	/**
	 * Reads a zero-compressed encoded int from a byte array and returns it.
	 *
	 * @param bytes
	 *          the byte array
	 * @param offset
	 *          offset of the array to read from
	 * @param vInt
	 *          storing the deserialized int and its size in byte
	 */
	public static void readVInt(byte[] bytes, int offset, VInt vInt) {
		byte firstByte = bytes[offset];
		vInt.length = (byte) WritableUtils.decodeVIntSize(firstByte);
		if (vInt.length == 1) {
			vInt.value = firstByte;
			return;
		}
		int i = 0;
		for (int idx = 0; idx < vInt.length - 1; idx++) {
			byte b = bytes[offset + 1 + idx];
			i = i << 8;
			i = i | (b & 0xFF);
		}
		vInt.value = (WritableUtils.isNegativeVInt(firstByte) ? (i ^ -1) : i);
	}

	/**
	 * Read a zero-compressed encoded long from a byte array.
	 *
	 * @param bytes the byte array
	 * @param offset the offset in the byte array where the VLong is stored
	 * @return the long
	 */
	public static long readVLongFromByteArray(final byte[] bytes, int offset) {
		byte firstByte = bytes[offset++];
		int len = WritableUtils.decodeVIntSize(firstByte);
		if (len == 1) {
			return firstByte;
		}
		long i = 0;
		for (int idx = 0; idx < len-1; idx++) {
			byte b = bytes[offset++];
			i = i << 8;
			i = i | (b & 0xFF);
		}
		return (WritableUtils.isNegativeVInt(firstByte) ? ~i : i);
	}

	/**
	 * Write a zero-compressed encoded long to a byte array.
	 *
	 * @param bytes
	 *          the byte array/stream
	 * @param l
	 *          the long
	 */
	public static int writeVLongToByteArray(byte[] bytes, long l) {
		return LazyBinaryUtils.writeVLongToByteArray(bytes, 0, l);
	}

	public static int writeVLongToByteArray(byte[] bytes, int offset, long l) {
		if (l >= -112 && l <= 127) {
			bytes[offset] = (byte) l;
			return 1;
		}

		int len = -112;
		if (l < 0) {
			l ^= -1L; // take one's complement'
			len = -120;
		}

		long tmp = l;
		while (tmp != 0) {
			tmp = tmp >> 8;
		len--;
		}

		bytes[offset] = (byte) len;

		len = (len < -120) ? -(len + 120) : -(len + 112);

		for (int idx = len; idx != 0; idx--) {
			int shiftbits = (idx - 1) * 8;
			long mask = 0xFFL << shiftbits;
			bytes[offset+1-(idx - len)] = (byte) ((l & mask) >> shiftbits);
		}
		return 1 + len;
	}
}
