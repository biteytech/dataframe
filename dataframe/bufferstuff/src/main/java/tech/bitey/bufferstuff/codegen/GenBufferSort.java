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

public class GenBufferSort implements GenBufferCode {

	@Override
	public void run() throws Exception {
		try (BufferedWriter out = open("BufferSort.java")) {

			section(out, PREFIX);

			sections(out, false);
			sections(out, true);

			out.write("}\n");
		}
	}

	private void sections(BufferedWriter out, boolean small) throws Exception {

		String s = small ? "Small" : "";

		section(out, heapSort("int", s + "IntBuffer", "b.get(l) > b.get(largest)", "b.get(r) > b.get(largest)"));
		if (small)
			section(out, HEAP_SORT_COMP.replace("IntBuffer", "SmallIntBuffer"));
		else
			section(out, HEAP_SORT_COMP);
		section(out, heapSort("long", s + "LongBuffer", "b.get(l) > b.get(largest)", "b.get(r) > b.get(largest)"));
		section(out, heapSort("short", s + "ShortBuffer", "b.get(l) > b.get(largest)", "b.get(r) > b.get(largest)"));
		section(out, heapSort("byte", s + "ByteBuffer", "b.get(l) > b.get(largest)", "b.get(r) > b.get(largest)"));
		section(out, heapSort("float", s + "FloatBuffer", "Float.compare(b.get(l), b.get(largest)) > 0",
				"Float.compare(b.get(r), b.get(largest)) > 0"));
		section(out, heapSort("double", s + "DoubleBuffer", "Double.compare(b.get(l), b.get(largest)) > 0",
				"Double.compare(b.get(r), b.get(largest)) > 0"));

		section(out, radixSort("int", s + "IntBuffer", "INT_HIGH_BIT"));
		section(out, radixSort("long", s + "LongBuffer", "LONG_HIGH_BIT"));

		section(out, countingSort("short", s + "ShortBuffer", "Short", 16, "0xFFFF"));
		section(out, countingSort("byte", s + "ByteBuffer", "Byte", 8, "0xFF"));

		section(out, insertionSort("int", s + "IntBuffer", "(xj = b.get(j)) > x"));
		section(out, insertionSort("long", s + "LongBuffer", "(xj = b.get(j)) > x"));
		section(out, insertionSort("short", s + "ShortBuffer", "(xj = b.get(j)) > x"));
		section(out, insertionSort("byte", s + "ByteBuffer", "(xj = b.get(j)) > x"));
		section(out, insertionSort("float", s + "FloatBuffer", "Float.compare(xj = b.get(j), x) > 0"));
		section(out, insertionSort("double", s + "DoubleBuffer", "Double.compare(xj = b.get(j), x) > 0"));

		section(out, insertionHeapRadix(s + "IntBuffer"));
		section(out, insertionHeapRadix(s + "LongBuffer"));
		section(out, insertionHeapCounting(s + "ShortBuffer", "10^7", "LARGE_RANGE"));
		section(out, insertionHeapCounting(s + "ByteBuffer", "10^5", "100000"));
		section(out, insertionHeap(s + "FloatBuffer"));
		section(out, insertionHeap(s + "DoubleBuffer"));
	}

	private static String heapSort(String valType, String bufferType, String compareL, String compareR) {
		return HEAP_SORT.replace(VAL_TYPE, valType).replace(BUFFER_TYPE, bufferType).replace(COMPARE_L, compareL)
				.replace(COMPARE_R, compareR);
	}

	private static String radixSort(String valType, String bufferType, String highBitName) {
		return RADIX_SORT.replace(VAL_TYPE, valType).replace(BUFFER_TYPE, bufferType).replace(HIGH_BIT_NAME,
				highBitName);
	}

	private static String countingSort(String valType, String bufferType, String boxType, int bits, String mask) {
		return COUNTING_SORT.replace(VAL_TYPE, valType).replace(BUFFER_TYPE, bufferType).replace(BOX_TYPE, boxType)
				.replace(COUNTING_BITS, "" + bits).replace(COUNTING_MASK, mask);
	}

	private static String insertionSort(String valType, String bufferType, String compare) {
		return INSERTION_SORT.replace(VAL_TYPE, valType).replace(BUFFER_TYPE, bufferType).replace(COMPARE, compare);
	}

	private static String insertionHeapRadix(String bufferType) {
		return INSERTION_HEAP_RADIX.replace(BUFFER_TYPE, bufferType);
	}

	private static String insertionHeapCounting(String bufferType, String comment, String range) {
		return INSERTION_HEAP_COUNTING.replace(BUFFER_TYPE, bufferType).replace(HEAP_RANGE_COMMENT, comment)
				.replace(HEAP_RANGE, range);
	}

	private static String insertionHeap(String bufferType) {
		return INSERTION_HEAP.replace(BUFFER_TYPE, bufferType);
	}

	private static final String BUFFER_TYPE = "BUFFER_TYPE";
	private static final String VAL_TYPE = "VAL_TYPE";
	private static final String COMPARE = "COMPARE";
	private static final String COMPARE_L = "COMPARE_L";
	private static final String COMPARE_R = "COMPARE_R";
	private static final String HIGH_BIT_NAME = "HIGH_BIT_NAME";
	private static final String BOX_TYPE = "BOX_TYPE";
	private static final String COUNTING_BITS = "COUNTING_BITS";
	private static final String COUNTING_MASK = "COUNTING_MASK";
	private static final String HEAP_RANGE_COMMENT = "HEAP_RANGE_COMMENT";
	private static final String HEAP_RANGE = "HEAP_RANGE";

	private static final String INSERTION_HEAP = """
				/**
				 * Sorts a range of the specified {@link BUFFER_TYPE} in ascending order
				 * (lowest first). The actual sorting algorithm used depends on the length of
				 * the range:
				 * <table border=1 summary="Sorting algorithm by array length">
				 * <tr>
				 * <th>Length</th>
				 * <th>Algorithm</th>
				 * </tr>
				 * <tr>
				 * <td>{@code [0 - 100)}</td>
				 * <td>{@link BufferSort#insertionSort(BUFFER_TYPE, int, int)
				 * insertionSort}</td>
				 * </tr>
				 * <tr>
				 * <td>{@code 100+}</td>
				 * <td>{@link BufferSort#heapSort(BUFFER_TYPE, int, int) heapSort}</td>
				 * </tr>
				 * </table>
				 *
				 * @param b         the buffer to be sorted
				 * @param fromIndex the index of the first element (inclusive) to be sorted
				 * @param toIndex   the index of the last element (exclusive) to be sorted
				 *
				 * @throws IllegalArgumentException  if {@code fromIndex > toIndex}
				 * @throws IndexOutOfBoundsException if
				 *                                   {@code fromIndex < 0 or toIndex > b.capacity()}
				 */
				public static void sort(BUFFER_TYPE b, int fromIndex, int toIndex) {

					final int length = toIndex - fromIndex;

					if (length < SMALL_RANGE)
						insertionSort(b, fromIndex, toIndex);
					else
						heapSort(b, fromIndex, toIndex);
				}
			""";

	private static final String INSERTION_HEAP_COUNTING = """
				/**
				 * Sorts a range of the specified {@link BUFFER_TYPE} in ascending order (lowest
				 * first). The actual sorting algorithm used depends on the length of the range:
				 * <table border=1 summary="Sorting algorithm by array length">
				 * <tr>
				 * <th>Length</th>
				 * <th>Algorithm</th>
				 * </tr>
				 * <tr>
				 * <td>{@code [0 - 100)}</td>
				 * <td>{@link BufferSort#insertionSort(BUFFER_TYPE, int, int)
				 * insertionSort}</td>
				 * </tr>
				 * <tr>
				 * <td>{@code [100 - HEAP_RANGE_COMMENT)}</td>
				 * <td>{@link BufferSort#heapSort(BUFFER_TYPE, int, int) heapSort}</td>
				 * </tr>
				 * <tr>
				 * <td>{@code HEAP_RANGE_COMMENT+}</td>
				 * <td>{@link BufferSort#countingSort(BUFFER_TYPE, int, int) countingSort}</td>
				 * </tr>
				 * </table>
				 *
				 * @param b         the buffer to be sorted
				 * @param fromIndex the index of the first element (inclusive) to be sorted
				 * @param toIndex   the index of the last element (exclusive) to be sorted
				 *
				 * @throws IllegalArgumentException  if {@code fromIndex > toIndex}
				 * @throws IndexOutOfBoundsException if
				 *                                   {@code fromIndex < 0 or toIndex > b.capacity()}
				 */
				public static void sort(BUFFER_TYPE b, int fromIndex, int toIndex) {

					final int length = toIndex - fromIndex;

					if (length < SMALL_RANGE)
						insertionSort(b, fromIndex, toIndex);
					else if (length < HEAP_RANGE)
						heapSort(b, fromIndex, toIndex);
					else
						countingSort(b, fromIndex, toIndex);
				}
			""";

	private static final String INSERTION_HEAP_RADIX = """
				/**
				 * Sorts a range of the specified {@link BUFFER_TYPE} in ascending order (lowest
				 * first). The actual sorting algorithm used depends on the length of the range:
				 * <table border=1 summary="Sorting algorithm by array length">
				 * <tr>
				 * <th>Length</th>
				 * <th>Algorithm</th>
				 * </tr>
				 * <tr>
				 * <td>{@code [0 - 100)}</td>
				 * <td>{@link BufferSort#insertionSort(BUFFER_TYPE, int, int) insertionSort}</td>
				 * </tr>
				 * <tr>
				 * <td>{@code [100 - 10^7)}</td>
				 * <td>{@link BufferSort#heapSort(BUFFER_TYPE, int, int) heapSort}</td>
				 * </tr>
				 * <tr>
				 * <td>{@code 10^7+}</td>
				 * <td>{@link BufferSort#radixSort(BUFFER_TYPE, int, int) radixSort}</td>
				 * </tr>
				 * </table>
				 *
				 * @param b         the buffer to be sorted
				 * @param fromIndex the index of the first element (inclusive) to be sorted
				 * @param toIndex   the index of the last element (exclusive) to be sorted
				 *
				 * @throws IllegalArgumentException  if {@code fromIndex > toIndex}
				 * @throws IndexOutOfBoundsException if
				 *                                   {@code fromIndex < 0 or toIndex > b.capacity()}
				 */
				public static void sort(BUFFER_TYPE b, int fromIndex, int toIndex) {

					final int length = toIndex - fromIndex;

					if (length < SMALL_RANGE)
						insertionSort(b, fromIndex, toIndex);
					else if (length < LARGE_RANGE)
						heapSort(b, fromIndex, toIndex);
					else
						radixSort(b, fromIndex, toIndex);
				}
			""";

	private static final String INSERTION_SORT = """
				/**
				 * Sorts a range of the specified {@link BUFFER_TYPE} in ascending order (lowest
				 * first). The sort is:
				 * <ul>
				 * <li>in-place
				 * <li>{@code O(n^2)} in the worst case. However, insertion sort has less
				 * overhead than heat sort, and is faster for small ranges.
				 * </ul>
				 *
				 * @param b         the buffer to be sorted
				 * @param fromIndex the index of the first element (inclusive) to be sorted
				 * @param toIndex   the index of the last element (exclusive) to be sorted
				 *
				 * @throws IllegalArgumentException  if {@code fromIndex > toIndex}
				 * @throws IndexOutOfBoundsException if
				 *                                   {@code fromIndex < 0 or toIndex > b.capacity()}
				 */
				public static void insertionSort(BUFFER_TYPE b, int fromIndex, int toIndex) {
					rangeCheck(b.capacity(), fromIndex, toIndex);

					for (int i = fromIndex + 1; i < toIndex; i++) {
						VAL_TYPE x = b.get(i);
						int j = i - 1;
						for (VAL_TYPE xj; j >= fromIndex && COMPARE; j--)
							b.put(j + 1, xj);
						b.put(j + 1, x);
					}
				}
			""";

	private static final String COUNTING_SORT = """
				/**
				 * Sorts a range of the specified {@link BUFFER_TYPE} in ascending order (lowest
				 * first). This sort is {@code O(n)} in the worst case, but it creates and
				 * iterates over an {@code int} array of length 2^COUNTING_BITS.
				 *
				 * @param b         the buffer to be sorted
				 * @param fromIndex the index of the first element (inclusive) to be sorted
				 * @param toIndex   the index of the last element (exclusive) to be sorted
				 *
				 * @throws IllegalArgumentException  if {@code fromIndex > toIndex}
				 * @throws IndexOutOfBoundsException if
				 *                                   {@code fromIndex < 0 or toIndex > b.capacity()}
				 */
				public static void countingSort(BUFFER_TYPE b, int fromIndex, int toIndex) {
					rangeCheck(b.capacity(), fromIndex, toIndex);

					int[] counts = new int[1 << COUNTING_BITS];

					for (int i = fromIndex; i < toIndex; i++)
						counts[b.get(i) & COUNTING_MASK]++;

					int k = fromIndex;

					// negative values
					for (int i = BOX_TYPE.MAX_VALUE + 1; i < counts.length; i++) {
						VAL_TYPE s = (VAL_TYPE) i;
						for (int j = 0; j < counts[i]; j++)
							b.put(k++, s);
					}

					// positive values
					for (int i = 0; i <= BOX_TYPE.MAX_VALUE; i++) {
						VAL_TYPE s = (VAL_TYPE) i;
						for (int j = 0; j < counts[i]; j++)
							b.put(k++, s);
					}
				}
			""";

	private static final String RADIX_SORT = """
				/**
				 * Sorts a range of the specified {@link BUFFER_TYPE} in ascending order (lowest
				 * first). The sort is:
				 * <ul>
				 * <li>in-place
				 * <li>{@code O(n)} in the worst case. However, radix sort has more overhead
				 * than heat sort, and is only faster for large ranges.
				 * </ul>
				 *
				 * @param b         the buffer to be sorted
				 * @param fromIndex the index of the first element (inclusive) to be sorted
				 * @param toIndex   the index of the last element (exclusive) to be sorted
				 *
				 * @throws IllegalArgumentException  if {@code fromIndex > toIndex}
				 * @throws IndexOutOfBoundsException if
				 *                                   {@code fromIndex < 0 or toIndex > b.capacity()}
				 */
				public static void radixSort(BUFFER_TYPE b, int fromIndex, int toIndex) {
					rangeCheck(b.capacity(), fromIndex, toIndex);
					radixSort0(b, fromIndex, toIndex, HIGH_BIT_NAME);
				}

				private static void radixSort0(BUFFER_TYPE b, int fromIndex, int toIndex, VAL_TYPE bit) {

					int zero = fromIndex;
					int one = toIndex;

					final VAL_TYPE direction = bit == HIGH_BIT_NAME ? bit : 0;

					while (zero < one) {
						if ((b.get(zero) & bit) == direction)
							zero++;
						else
							swap(b, zero, --one);
					}

					if (bit != 1) {
						if (fromIndex < zero)
							radixSort0(b, fromIndex, zero, bit >>> 1);
						if (one < toIndex)
							radixSort0(b, one, toIndex, bit >>> 1);
					}
				}
			""";

	private static final String HEAP_SORT_COMP = """
				/**
				 * Sorts a range of the specified {@link IntBuffer} in ascending order (lowest
				 * first). The sort is:
				 * <ul>
				 * <li>in-place
				 * <li>{@code O(n*log(n))} in the worst case
				 * <li>a good general-purpose sorting algorithm
				 * </ul>
				 *
				 * @param b          the buffer to be sorted
				 * @param comparator used to compare values from {@code b}. useful when the
				 *                   integers are identifiers or indices referencing some
				 *                   external data structure.
				 * @param fromIndex  the index of the first element (inclusive) to be sorted
				 * @param toIndex    the index of the last element (exclusive) to be sorted
				 *
				 * @throws IllegalArgumentException  if {@code fromIndex > toIndex}
				 * @throws IndexOutOfBoundsException if
				 *                                   {@code fromIndex < 0 or toIndex > b.capacity()}
				 */
				public static void heapSort(IntBuffer b, IntBinaryOperator comparator, int fromIndex, int toIndex) {
					rangeCheck(b.capacity(), fromIndex, toIndex);

					int n = toIndex - fromIndex;
					if (n <= 1)
						return;

					// Build max heap
					for (int i = fromIndex + n / 2 - 1; i >= fromIndex; i--)
						heapify(b, comparator, toIndex, i, fromIndex);

					// Heap sort
					for (int i = toIndex - 1; i >= fromIndex; i--) {
						swap(b, fromIndex, i);

						// Heapify root element
						heapify(b, comparator, i, fromIndex, fromIndex);
					}
				}

				// based on https://www.programiz.com/dsa/heap-sort
				private static void heapify(IntBuffer b, IntBinaryOperator comparator, int n, int i, int offset) {
					// Find largest among root, left child and right child
					int largest = i;
					int l = 2 * i + 1 - offset;
					int r = l + 1;

					if (l < n && comparator.applyAsInt(b.get(l), b.get(largest)) > 0)
						largest = l;

					if (r < n && comparator.applyAsInt(b.get(r), b.get(largest)) > 0)
						largest = r;

					// Swap and continue heapifying if root is not largest
					if (largest != i) {
						swap(b, i, largest);
						heapify(b, comparator, n, largest, offset);
					}
				}
			""";

	private static final String HEAP_SORT = """
				/**
				 * Sorts a range of the specified {@link BUFFER_TYPE} in ascending order (lowest
				 * first). The sort is:
				 * <ul>
				 * <li>in-place
				 * <li>{@code O(n*log(n))} in the worst case
				 * <li>a good general-purpose sorting algorithm
				 * </ul>
				 *
				 * @param b         the buffer to be sorted
				 * @param fromIndex the index of the first element (inclusive) to be sorted
				 * @param toIndex   the index of the last element (exclusive) to be sorted
				 *
				 * @throws IllegalArgumentException  if {@code fromIndex > toIndex}
				 * @throws IndexOutOfBoundsException if
				 *                                   {@code fromIndex < 0 or toIndex > b.capacity()}
				 */
				public static void heapSort(BUFFER_TYPE b, int fromIndex, int toIndex) {
					rangeCheck(b.capacity(), fromIndex, toIndex);

					int n = toIndex - fromIndex;
					if (n <= 1)
						return;

					// Build max heap
					for (int i = fromIndex + n / 2 - 1; i >= fromIndex; i--)
						heapify(b, toIndex, i, fromIndex);

					// Heap sort
					for (int i = toIndex - 1; i >= fromIndex; i--) {
						swap(b, fromIndex, i);

						// Heapify root element
						heapify(b, i, fromIndex, fromIndex);
					}
				}

				// based on https://www.programiz.com/dsa/heap-sort
				private static void heapify(BUFFER_TYPE b, int n, int i, int offset) {
					// Find largest among root, left child and right child
					int largest = i;
					int l = 2 * i + 1 - offset;
					int r = l + 1;

					if (l < n && COMPARE_L)
						largest = l;

					if (r < n && COMPARE_R)
						largest = r;

					// Swap and continue heapifying if root is not largest
					if (largest != i) {
						swap(b, i, largest);
						heapify(b, n, largest, offset);
					}
				}

				private static void swap(BUFFER_TYPE b, int i, int j) {
					VAL_TYPE swap = b.get(i);
					b.put(i, b.get(j));
					b.put(j, swap);
				}
			""";

	private static final String PREFIX = """
			package tech.bitey.bufferstuff;

			import static tech.bitey.bufferstuff.BufferUtils.rangeCheck;

			import java.nio.ByteBuffer;
			import java.nio.DoubleBuffer;
			import java.nio.FloatBuffer;
			import java.nio.IntBuffer;
			import java.nio.LongBuffer;
			import java.nio.ShortBuffer;
			import java.util.function.IntBinaryOperator;

			/**
			 * Sorting algorithms for nio buffers.
			 *
			 * @author biteytech@protonmail.com, heap-sort adapted from <a
			 *         href=https://www.programiz.com/dsa/heap-sort>programiz.com</a>
			 */
			public class BufferSort {

				private static final int SMALL_RANGE = 100;
				private static final int LARGE_RANGE = 10_000_000;

				private static final int INT_HIGH_BIT = 1 << 31;
				private static final long LONG_HIGH_BIT = 1L << 63;
			""";
}
