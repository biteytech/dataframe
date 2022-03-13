/*
 * Copyright 2022 biteytech@protonmail.com
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tech.bitey.bufferstuff.codegen;

import java.io.BufferedWriter;

public class GenBufferSearch implements GenBufferCode {

	@Override
	public void run() throws Exception {
		try (BufferedWriter out = open("BufferSearch.java")) {

			section(out, PREFIX);

			sections(out, false);
			sections(out, true);

			out.write("}\n");
		}
	}

	private void sections(BufferedWriter out, boolean small) throws Exception {

		String s = small ? "Small" : "";

		section(out, intBinarySearch(s + "IntBuffer", "int"));
		section(out, intBinarySearch(s + "LongBuffer", "long"));
		section(out, intBinarySearch(s + "ShortBuffer", "short"));
		section(out, intBinarySearch(s + "ByteBuffer", "byte"));
		section(out, floatBinarySearch(s + "FloatBuffer", "float", "Float", "int", "Int"));
		section(out, floatBinarySearch(s + "DoubleBuffer", "double", "Double", "long", "Long"));

		section(out, intBinaryFindFirst(s + "IntBuffer", "int"));
		section(out, intBinaryFindLast(s + "IntBuffer", "int"));
		section(out, intBinaryFindFirst(s + "LongBuffer", "long"));
		section(out, intBinaryFindLast(s + "LongBuffer", "long"));
		section(out, intBinaryFindFirst(s + "ShortBuffer", "short"));
		section(out, intBinaryFindLast(s + "ShortBuffer", "short"));
		section(out, intBinaryFindFirst(s + "ByteBuffer", "byte"));
		section(out, intBinaryFindLast(s + "ByteBuffer", "byte"));
		section(out, floatBinaryFindFirst(s + "FloatBuffer", "float", "Float"));
		section(out, floatBinaryFindLast(s + "FloatBuffer", "float", "Float"));
		section(out, floatBinaryFindFirst(s + "DoubleBuffer", "double", "Double"));
		section(out, floatBinaryFindLast(s + "DoubleBuffer", "double", "Double"));
	}

	private static String intBinarySearch(String bufferType, String keyType) {
		return BINARY_SEARCH.replace(BINARY_SEARCH_COMPARE, BINARY_SEARCH_INT_COMPARE).replace(BUFFER_TYPE, bufferType)
				.replace(KEY_TYPE, keyType).replace(NAN_COMMENT, "");
	}

	private static String floatBinarySearch(String bufferType, String keyType, String boxType, String bitsType,
			String boxBitsType) {
		return BINARY_SEARCH.replace(BINARY_SEARCH_COMPARE, BINARY_SEARCH_FLOAT_COMPARE)
				.replace(BUFFER_TYPE, bufferType).replace(KEY_TYPE, keyType).replace(BOX_TYPE, boxType)
				.replace(BOX_BITS_TYPE, boxBitsType).replace(BITS_TYPE, bitsType)
				.replace(NAN_COMMENT, FLOAT_NAN_COMMENT);
	}

	private static final String INT_RANGE_INDEX = "b.get(rangeIndex) == key";
	private static final String FLOAT_RANGE_INDEX = "BOX_TYPE.compare(b.get(rangeIndex), key) == 0";

	private static String intBinaryFindFirst(String bufferType, String keyType) {
		return BINARY_FIND_FIRST.replace(BUFFER_TYPE, bufferType).replace(KEY_TYPE, keyType).replace(NAN_COMMENT, "")
				.replace(COMPARE_FROM_INDEX, "b.get(fromIndex - 1) == key")
				.replace(COMPARE_RANGE_INDEX, INT_RANGE_INDEX);
	}

	private static String floatBinaryFindFirst(String bufferType, String keyType, String boxType) {
		return BINARY_FIND_FIRST.replace(COMPARE_FROM_INDEX, "BOX_TYPE.compare(b.get(fromIndex - 1), key) == 0")
				.replace(COMPARE_RANGE_INDEX, FLOAT_RANGE_INDEX).replace(BUFFER_TYPE, bufferType)
				.replace(KEY_TYPE, keyType).replace(NAN_COMMENT, FLOAT_NAN_COMMENT).replace(BOX_TYPE, boxType);
	}

	private static String intBinaryFindLast(String bufferType, String keyType) {
		return BINARY_FIND_LAST.replace(BUFFER_TYPE, bufferType).replace(KEY_TYPE, keyType).replace(NAN_COMMENT, "")
				.replace(COMPARE_FROM_INDEX, "b.get(fromIndex + 1) == key")
				.replace(COMPARE_RANGE_INDEX, INT_RANGE_INDEX);
	}

	private static String floatBinaryFindLast(String bufferType, String keyType, String boxType) {
		return BINARY_FIND_LAST.replace(COMPARE_FROM_INDEX, "BOX_TYPE.compare(b.get(fromIndex + 1), key) == 0")
				.replace(COMPARE_RANGE_INDEX, FLOAT_RANGE_INDEX).replace(BUFFER_TYPE, bufferType)
				.replace(KEY_TYPE, keyType).replace(NAN_COMMENT, FLOAT_NAN_COMMENT).replace(BOX_TYPE, boxType);
	}

	private static final String BINARY_SEARCH_COMPARE = "BINARY_SEARCH_COMPARE";
	private static final String BUFFER_TYPE = "BUFFER_TYPE";
	private static final String KEY_TYPE = "KEY_TYPE";
	private static final String BOX_TYPE = "BOX_TYPE";
	private static final String BITS_TYPE = "BITS_TYPE";
	private static final String BOX_BITS_TYPE = "BOX_BITS_TYPE";
	private static final String NAN_COMMENT = "NAN_COMMENT";
	private static final String COMPARE_FROM_INDEX = "COMPARE_FROM_INDEX";
	private static final String COMPARE_RANGE_INDEX = "COMPARE_RANGE_INDEX";

	private static final String PREFIX = """
			package tech.bitey.bufferstuff;

			import static tech.bitey.bufferstuff.BufferUtils.rangeCheck;
			import static tech.bitey.bufferstuff.BufferUtils.rangeCheckInclusive;

			import java.nio.ByteBuffer;
			import java.nio.DoubleBuffer;
			import java.nio.FloatBuffer;
			import java.nio.IntBuffer;
			import java.nio.LongBuffer;
			import java.nio.ShortBuffer;

			/**
			 * Provides the primitive array binary search implementations from
			 * {@code java.util.Arrays}, modified with minimal changes to support nio
			 * buffers.
			 * <p>
			 * Also provides methods for finding the first or last index of a sequence of
			 * duplicate values.
			 *
			 * @author biteytech@protonmail.com, adapted from java.util.Arrays
			 */
			public enum BufferSearch {
				; // static methods only, enum prevents instantiation
			""";

	private static final String BINARY_SEARCH = """
				/**
				 * Searches a range of the specified {@link BUFFER_TYPE} for the specified value
				 * using the binary search algorithm. The range must be sorted in ascending
				 * order prior to making this call. If it is not sorted, the results are
				 * undefined. If the range contains multiple elements with the specified value,
				 * there is no guarantee which one will be found.NAN_COMMENT
				 *
				 * @param b         the buffer to be searched
				 * @param fromIndex the index of the first element (inclusive) to be searched
				 * @param toIndex   the index of the last element (exclusive) to be searched
				 * @param key       the value to be searched for
				 *
				 * @return index of the search key, if it is contained in the buffer within the
				 *         specified range; otherwise, {@code (-(<i>insertion point</i>) - 1)}.
				 *         The <i>insertion point</i> is defined as the point at which the key
				 *         would be inserted into the buffer: the index of the first element in
				 *         the range greater than the key, or {@code toIndex} if all elements in
				 *         the range are less than the specified key. Note that this guarantees
				 *         that the return value will be &gt;= 0 if and only if the key is
				 *         found.
				 *
				 * @throws IllegalArgumentException  if {@code fromIndex > toIndex}
				 * @throws IndexOutOfBoundsException if
				 *                                   {@code fromIndex < 0 or toIndex > b.capacity()}
				 */
				public static int binarySearch(BUFFER_TYPE b, int fromIndex, int toIndex, KEY_TYPE key) {
					rangeCheck(b.capacity(), fromIndex, toIndex);
					return binarySearch0(b, fromIndex, toIndex, key);
				}

				private static int binarySearch0(BUFFER_TYPE b, int fromIndex, int toIndex, KEY_TYPE key) {
					int low = fromIndex;
					int high = toIndex - 1;

					while (low <= high) {
						int mid = (low + high) >>> 1;
						KEY_TYPE midVal = b.get(mid);

						BINARY_SEARCH_COMPARE
					}
					return -(low + 1); // key not found.
				}
			""";

	private static final String FLOAT_NAN_COMMENT = " This method considers all NaN values to be equivalent and equal.";

	private static final String BINARY_SEARCH_INT_COMPARE = """
			if (midVal < key)
				low = mid + 1;
			else if (midVal > key)
				high = mid - 1;
			else
				return mid; // key found
			""";

	private static final String BINARY_SEARCH_FLOAT_COMPARE = """
			if (midVal < key)
				low = mid + 1; // Neither val is NaN, thisVal is smaller
			else if (midVal > key)
				high = mid - 1; // Neither val is NaN, thisVal is larger
			else {
				BITS_TYPE midBits = BOX_TYPE.KEY_TYPEToBOX_BITS_TYPEBits(midVal);
				BITS_TYPE keyBits = BOX_TYPE.KEY_TYPEToBOX_BITS_TYPEBits(key);
				if (midBits == keyBits) // Values are equal
					return mid; // Key found
				else if (midBits < keyBits) // (-0.0, 0.0) or (!NaN, NaN)
					low = mid + 1;
				else // (0.0, -0.0) or (NaN, !NaN)
					high = mid - 1;
			}
			""";

	private static final String BINARY_FIND_FIRST = """
				/**
				 * Searches a range of the specified {@link BUFFER_TYPE} for the first occurrence
				 * of the value at the given {@code keyIndex}. The range must be sorted in
				 * ascending order prior to making this call. If it is not sorted, the results
				 * are undefined.NAN_COMMENT
				 * <p>
				 * This method is useful as a post-processing step after a binary search on a
				 * buffer which contains duplicate elements.
				 *
				 * @param b        the buffer to be searched
				 * @param minIndex the lowest index to be searched
				 * @param keyIndex an index of the value for which to find the first occurrence
				 *                 (inclusive)
				 *
				 * @return index of the first occurrence of the value at {@code keyIndex}
				 *
				 * @throws IllegalArgumentException  if {@code minIndex > keyIndex}
				 * @throws IndexOutOfBoundsException if
				 *                                   {@code minIndex < 0 or keyIndex >= b.capacity()}
				 */
				public static int binaryFindFirst(BUFFER_TYPE b, int minIndex, int keyIndex) {
					rangeCheckInclusive(b.capacity(), minIndex, keyIndex);
					return binaryFindFirst(b, minIndex, keyIndex, b.get(keyIndex));
				}

				private static int binaryFindFirst(BUFFER_TYPE b, int minIndex, int fromIndex, KEY_TYPE key) {

					while (minIndex != fromIndex && COMPARE_FROM_INDEX) {

						int range = 1, rangeIndex;
						do {
							range <<= 1;
							rangeIndex = fromIndex - range;
						} while (minIndex <= rangeIndex && COMPARE_RANGE_INDEX);

						fromIndex -= range >> 1;
					}

					return fromIndex;
				}
			""";

	private static final String BINARY_FIND_LAST = """
				/**
				 * Searches a range of the specified {@link BUFFER_TYPE} for the last occurrence
				 * of the value at the given {@code keyIndex}. The range must be sorted in
				 * ascending order prior to making this call. If it is not sorted, the results
				 * are undefined.NAN_COMMENT
				 * <p>
				 * This method is useful as a post-processing step after a binary search on a
				 * buffer which contains duplicate elements.
				 *
				 * @param b        the buffer to be searched
				 * @param maxIndex the highest index to be searched (exclusive)
				 * @param keyIndex an index of the value for which to find the first occurrence
				 *                 (inclusive)
				 *
				 * @return index of the last occurrence of the value at {@code keyIndex}
				 *
				 * @throws IllegalArgumentException  if {@code maxIndex < keyIndex}
				 * @throws IndexOutOfBoundsException if
				 *                                   {@code keyIndex < 0 or maxIndex > b.capacity()}
				 */
				public static int binaryFindLast(BUFFER_TYPE b, int maxIndex, int keyIndex) {
					rangeCheck(b.capacity(), keyIndex, maxIndex);
					if (keyIndex == maxIndex)
						return keyIndex;
					else
						return binaryFindLast(b, keyIndex, maxIndex - 1, b.get(keyIndex));
				}

				private static int binaryFindLast(BUFFER_TYPE b, int fromIndex, int maxIndex, KEY_TYPE key) {

					while (fromIndex != maxIndex && COMPARE_FROM_INDEX) {

						int range = 1, rangeIndex;
						do {
							range <<= 1;
							rangeIndex = fromIndex + range;
						} while (rangeIndex <= maxIndex && COMPARE_RANGE_INDEX);

						fromIndex += range >> 1;
					}

					return fromIndex;
				}
			""";
}
