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

	/**
	 * Sorts a range of the specified {@link IntBuffer} in ascending order (lowest
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
	public static void heapSort(IntBuffer b, int fromIndex, int toIndex) {
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
	private static void heapify(IntBuffer b, int n, int i, int offset) {
		// Find largest among root, left child and right child
		int largest = i;
		int l = 2 * i + 1 - offset;
		int r = l + 1;

		if (l < n && b.get(l) > b.get(largest))
			largest = l;

		if (r < n && b.get(r) > b.get(largest))
			largest = r;

		// Swap and continue heapifying if root is not largest
		if (largest != i) {
			swap(b, i, largest);
			heapify(b, n, largest, offset);
		}
	}

	private static void swap(IntBuffer b, int i, int j) {
		int swap = b.get(i);
		b.put(i, b.get(j));
		b.put(j, swap);
	}

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

	/**
	 * Sorts a range of the specified {@link LongBuffer} in ascending order (lowest
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
	public static void heapSort(LongBuffer b, int fromIndex, int toIndex) {
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
	private static void heapify(LongBuffer b, int n, int i, int offset) {
		// Find largest among root, left child and right child
		int largest = i;
		int l = 2 * i + 1 - offset;
		int r = l + 1;

		if (l < n && b.get(l) > b.get(largest))
			largest = l;

		if (r < n && b.get(r) > b.get(largest))
			largest = r;

		// Swap and continue heapifying if root is not largest
		if (largest != i) {
			swap(b, i, largest);
			heapify(b, n, largest, offset);
		}
	}

	private static void swap(LongBuffer b, int i, int j) {
		long swap = b.get(i);
		b.put(i, b.get(j));
		b.put(j, swap);
	}

	/**
	 * Sorts a range of the specified {@link ShortBuffer} in ascending order (lowest
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
	public static void heapSort(ShortBuffer b, int fromIndex, int toIndex) {
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
	private static void heapify(ShortBuffer b, int n, int i, int offset) {
		// Find largest among root, left child and right child
		int largest = i;
		int l = 2 * i + 1 - offset;
		int r = l + 1;

		if (l < n && b.get(l) > b.get(largest))
			largest = l;

		if (r < n && b.get(r) > b.get(largest))
			largest = r;

		// Swap and continue heapifying if root is not largest
		if (largest != i) {
			swap(b, i, largest);
			heapify(b, n, largest, offset);
		}
	}

	private static void swap(ShortBuffer b, int i, int j) {
		short swap = b.get(i);
		b.put(i, b.get(j));
		b.put(j, swap);
	}

	/**
	 * Sorts a range of the specified {@link ByteBuffer} in ascending order (lowest
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
	public static void heapSort(ByteBuffer b, int fromIndex, int toIndex) {
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
	private static void heapify(ByteBuffer b, int n, int i, int offset) {
		// Find largest among root, left child and right child
		int largest = i;
		int l = 2 * i + 1 - offset;
		int r = l + 1;

		if (l < n && b.get(l) > b.get(largest))
			largest = l;

		if (r < n && b.get(r) > b.get(largest))
			largest = r;

		// Swap and continue heapifying if root is not largest
		if (largest != i) {
			swap(b, i, largest);
			heapify(b, n, largest, offset);
		}
	}

	private static void swap(ByteBuffer b, int i, int j) {
		byte swap = b.get(i);
		b.put(i, b.get(j));
		b.put(j, swap);
	}

	/**
	 * Sorts a range of the specified {@link FloatBuffer} in ascending order (lowest
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
	public static void heapSort(FloatBuffer b, int fromIndex, int toIndex) {
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
	private static void heapify(FloatBuffer b, int n, int i, int offset) {
		// Find largest among root, left child and right child
		int largest = i;
		int l = 2 * i + 1 - offset;
		int r = l + 1;

		if (l < n && Float.compare(b.get(l), b.get(largest)) > 0)
			largest = l;

		if (r < n && Float.compare(b.get(r), b.get(largest)) > 0)
			largest = r;

		// Swap and continue heapifying if root is not largest
		if (largest != i) {
			swap(b, i, largest);
			heapify(b, n, largest, offset);
		}
	}

	private static void swap(FloatBuffer b, int i, int j) {
		float swap = b.get(i);
		b.put(i, b.get(j));
		b.put(j, swap);
	}

	/**
	 * Sorts a range of the specified {@link DoubleBuffer} in ascending order
	 * (lowest first). The sort is:
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
	public static void heapSort(DoubleBuffer b, int fromIndex, int toIndex) {
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
	private static void heapify(DoubleBuffer b, int n, int i, int offset) {
		// Find largest among root, left child and right child
		int largest = i;
		int l = 2 * i + 1 - offset;
		int r = l + 1;

		if (l < n && Double.compare(b.get(l), b.get(largest)) > 0)
			largest = l;

		if (r < n && Double.compare(b.get(r), b.get(largest)) > 0)
			largest = r;

		// Swap and continue heapifying if root is not largest
		if (largest != i) {
			swap(b, i, largest);
			heapify(b, n, largest, offset);
		}
	}

	private static void swap(DoubleBuffer b, int i, int j) {
		double swap = b.get(i);
		b.put(i, b.get(j));
		b.put(j, swap);
	}

	/**
	 * Sorts a range of the specified {@link IntBuffer} in ascending order (lowest
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
	public static void radixSort(IntBuffer b, int fromIndex, int toIndex) {
		rangeCheck(b.capacity(), fromIndex, toIndex);
		radixSort0(b, fromIndex, toIndex, INT_HIGH_BIT);
	}

	private static void radixSort0(IntBuffer b, int fromIndex, int toIndex, int bit) {

		int zero = fromIndex;
		int one = toIndex;

		final int direction = bit == INT_HIGH_BIT ? bit : 0;

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

	/**
	 * Sorts a range of the specified {@link LongBuffer} in ascending order (lowest
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
	public static void radixSort(LongBuffer b, int fromIndex, int toIndex) {
		rangeCheck(b.capacity(), fromIndex, toIndex);
		radixSort0(b, fromIndex, toIndex, LONG_HIGH_BIT);
	}

	private static void radixSort0(LongBuffer b, int fromIndex, int toIndex, long bit) {

		int zero = fromIndex;
		int one = toIndex;

		final long direction = bit == LONG_HIGH_BIT ? bit : 0;

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

	/**
	 * Sorts a range of the specified {@link ShortBuffer} in ascending order (lowest
	 * first). This sort is {@code O(n)} in the worst case, but it creates and
	 * iterates over an {@code int} array of length 2^16.
	 *
	 * @param b         the buffer to be sorted
	 * @param fromIndex the index of the first element (inclusive) to be sorted
	 * @param toIndex   the index of the last element (exclusive) to be sorted
	 *
	 * @throws IllegalArgumentException  if {@code fromIndex > toIndex}
	 * @throws IndexOutOfBoundsException if
	 *                                   {@code fromIndex < 0 or toIndex > b.capacity()}
	 */
	public static void countingSort(ShortBuffer b, int fromIndex, int toIndex) {
		rangeCheck(b.capacity(), fromIndex, toIndex);

		int[] counts = new int[1 << 16];

		for (int i = fromIndex; i < toIndex; i++)
			counts[b.get(i) & 0xFFFF]++;

		int k = fromIndex;

		// negative values
		for (int i = Short.MAX_VALUE + 1; i < counts.length; i++) {
			short s = (short) i;
			for (int j = 0; j < counts[i]; j++)
				b.put(k++, s);
		}

		// positive values
		for (int i = 0; i <= Short.MAX_VALUE; i++) {
			short s = (short) i;
			for (int j = 0; j < counts[i]; j++)
				b.put(k++, s);
		}
	}

	/**
	 * Sorts a range of the specified {@link ByteBuffer} in ascending order (lowest
	 * first). This sort is {@code O(n)} in the worst case, but it creates and
	 * iterates over an {@code int} array of length 2^8.
	 *
	 * @param b         the buffer to be sorted
	 * @param fromIndex the index of the first element (inclusive) to be sorted
	 * @param toIndex   the index of the last element (exclusive) to be sorted
	 *
	 * @throws IllegalArgumentException  if {@code fromIndex > toIndex}
	 * @throws IndexOutOfBoundsException if
	 *                                   {@code fromIndex < 0 or toIndex > b.capacity()}
	 */
	public static void countingSort(ByteBuffer b, int fromIndex, int toIndex) {
		rangeCheck(b.capacity(), fromIndex, toIndex);

		int[] counts = new int[1 << 8];

		for (int i = fromIndex; i < toIndex; i++)
			counts[b.get(i) & 0xFF]++;

		int k = fromIndex;

		// negative values
		for (int i = Byte.MAX_VALUE + 1; i < counts.length; i++) {
			byte s = (byte) i;
			for (int j = 0; j < counts[i]; j++)
				b.put(k++, s);
		}

		// positive values
		for (int i = 0; i <= Byte.MAX_VALUE; i++) {
			byte s = (byte) i;
			for (int j = 0; j < counts[i]; j++)
				b.put(k++, s);
		}
	}

	/**
	 * Sorts a range of the specified {@link IntBuffer} in ascending order (lowest
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
	public static void insertionSort(IntBuffer b, int fromIndex, int toIndex) {
		rangeCheck(b.capacity(), fromIndex, toIndex);

		for (int i = fromIndex + 1; i < toIndex; i++) {
			int x = b.get(i);
			int j = i - 1;
			for (int xj; j >= fromIndex && (xj = b.get(j)) > x; j--)
				b.put(j + 1, xj);
			b.put(j + 1, x);
		}
	}

	/**
	 * Sorts a range of the specified {@link LongBuffer} in ascending order (lowest
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
	public static void insertionSort(LongBuffer b, int fromIndex, int toIndex) {
		rangeCheck(b.capacity(), fromIndex, toIndex);

		for (int i = fromIndex + 1; i < toIndex; i++) {
			long x = b.get(i);
			int j = i - 1;
			for (long xj; j >= fromIndex && (xj = b.get(j)) > x; j--)
				b.put(j + 1, xj);
			b.put(j + 1, x);
		}
	}

	/**
	 * Sorts a range of the specified {@link ShortBuffer} in ascending order (lowest
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
	public static void insertionSort(ShortBuffer b, int fromIndex, int toIndex) {
		rangeCheck(b.capacity(), fromIndex, toIndex);

		for (int i = fromIndex + 1; i < toIndex; i++) {
			short x = b.get(i);
			int j = i - 1;
			for (short xj; j >= fromIndex && (xj = b.get(j)) > x; j--)
				b.put(j + 1, xj);
			b.put(j + 1, x);
		}
	}

	/**
	 * Sorts a range of the specified {@link ByteBuffer} in ascending order (lowest
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
	public static void insertionSort(ByteBuffer b, int fromIndex, int toIndex) {
		rangeCheck(b.capacity(), fromIndex, toIndex);

		for (int i = fromIndex + 1; i < toIndex; i++) {
			byte x = b.get(i);
			int j = i - 1;
			for (byte xj; j >= fromIndex && (xj = b.get(j)) > x; j--)
				b.put(j + 1, xj);
			b.put(j + 1, x);
		}
	}

	/**
	 * Sorts a range of the specified {@link FloatBuffer} in ascending order (lowest
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
	public static void insertionSort(FloatBuffer b, int fromIndex, int toIndex) {
		rangeCheck(b.capacity(), fromIndex, toIndex);

		for (int i = fromIndex + 1; i < toIndex; i++) {
			float x = b.get(i);
			int j = i - 1;
			for (float xj; j >= fromIndex && Float.compare(xj = b.get(j), x) > 0; j--)
				b.put(j + 1, xj);
			b.put(j + 1, x);
		}
	}

	/**
	 * Sorts a range of the specified {@link DoubleBuffer} in ascending order
	 * (lowest first). The sort is:
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
	public static void insertionSort(DoubleBuffer b, int fromIndex, int toIndex) {
		rangeCheck(b.capacity(), fromIndex, toIndex);

		for (int i = fromIndex + 1; i < toIndex; i++) {
			double x = b.get(i);
			int j = i - 1;
			for (double xj; j >= fromIndex && Double.compare(xj = b.get(j), x) > 0; j--)
				b.put(j + 1, xj);
			b.put(j + 1, x);
		}
	}

	/**
	 * Sorts a range of the specified {@link IntBuffer} in ascending order (lowest
	 * first). The actual sorting algorithm used depends on the length of the range:
	 * <table border=1 summary="Sorting algorithm by array length">
	 * <tr>
	 * <th>Length</th>
	 * <th>Algorithm</th>
	 * </tr>
	 * <tr>
	 * <td>{@code [0 - 100)}</td>
	 * <td>{@link BufferSort#insertionSort(IntBuffer, int, int) insertionSort}</td>
	 * </tr>
	 * <tr>
	 * <td>{@code [100 - 10^7)}</td>
	 * <td>{@link BufferSort#heapSort(IntBuffer, int, int) heapSort}</td>
	 * </tr>
	 * <tr>
	 * <td>{@code 10^7+}</td>
	 * <td>{@link BufferSort#radixSort(IntBuffer, int, int) radixSort}</td>
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
	public static void sort(IntBuffer b, int fromIndex, int toIndex) {

		final int length = toIndex - fromIndex;

		if (length < SMALL_RANGE)
			insertionSort(b, fromIndex, toIndex);
		else if (length < LARGE_RANGE)
			heapSort(b, fromIndex, toIndex);
		else
			radixSort(b, fromIndex, toIndex);
	}

	/**
	 * Sorts a range of the specified {@link LongBuffer} in ascending order (lowest
	 * first). The actual sorting algorithm used depends on the length of the range:
	 * <table border=1 summary="Sorting algorithm by array length">
	 * <tr>
	 * <th>Length</th>
	 * <th>Algorithm</th>
	 * </tr>
	 * <tr>
	 * <td>{@code [0 - 100)}</td>
	 * <td>{@link BufferSort#insertionSort(LongBuffer, int, int) insertionSort}</td>
	 * </tr>
	 * <tr>
	 * <td>{@code [100 - 10^7)}</td>
	 * <td>{@link BufferSort#heapSort(LongBuffer, int, int) heapSort}</td>
	 * </tr>
	 * <tr>
	 * <td>{@code 10^7+}</td>
	 * <td>{@link BufferSort#radixSort(LongBuffer, int, int) radixSort}</td>
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
	public static void sort(LongBuffer b, int fromIndex, int toIndex) {

		final int length = toIndex - fromIndex;

		if (length < SMALL_RANGE)
			insertionSort(b, fromIndex, toIndex);
		else if (length < LARGE_RANGE)
			heapSort(b, fromIndex, toIndex);
		else
			radixSort(b, fromIndex, toIndex);
	}

	/**
	 * Sorts a range of the specified {@link ShortBuffer} in ascending order (lowest
	 * first). The actual sorting algorithm used depends on the length of the range:
	 * <table border=1 summary="Sorting algorithm by array length">
	 * <tr>
	 * <th>Length</th>
	 * <th>Algorithm</th>
	 * </tr>
	 * <tr>
	 * <td>{@code [0 - 100)}</td>
	 * <td>{@link BufferSort#insertionSort(ShortBuffer, int, int)
	 * insertionSort}</td>
	 * </tr>
	 * <tr>
	 * <td>{@code [100 - 10^7)}</td>
	 * <td>{@link BufferSort#heapSort(ShortBuffer, int, int) heapSort}</td>
	 * </tr>
	 * <tr>
	 * <td>{@code 10^7+}</td>
	 * <td>{@link BufferSort#countingSort(ShortBuffer, int, int) countingSort}</td>
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
	public static void sort(ShortBuffer b, int fromIndex, int toIndex) {

		final int length = toIndex - fromIndex;

		if (length < SMALL_RANGE)
			insertionSort(b, fromIndex, toIndex);
		else if (length < LARGE_RANGE)
			heapSort(b, fromIndex, toIndex);
		else
			countingSort(b, fromIndex, toIndex);
	}

	/**
	 * Sorts a range of the specified {@link ByteBuffer} in ascending order (lowest
	 * first). The actual sorting algorithm used depends on the length of the range:
	 * <table border=1 summary="Sorting algorithm by array length">
	 * <tr>
	 * <th>Length</th>
	 * <th>Algorithm</th>
	 * </tr>
	 * <tr>
	 * <td>{@code [0 - 100)}</td>
	 * <td>{@link BufferSort#insertionSort(ByteBuffer, int, int) insertionSort}</td>
	 * </tr>
	 * <tr>
	 * <td>{@code [100 - 10^5)}</td>
	 * <td>{@link BufferSort#heapSort(ByteBuffer, int, int) heapSort}</td>
	 * </tr>
	 * <tr>
	 * <td>{@code 10^5+}</td>
	 * <td>{@link BufferSort#countingSort(ByteBuffer, int, int) countingSort}</td>
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
	public static void sort(ByteBuffer b, int fromIndex, int toIndex) {

		final int length = toIndex - fromIndex;

		if (length < SMALL_RANGE)
			insertionSort(b, fromIndex, toIndex);
		else if (length < 100000)
			heapSort(b, fromIndex, toIndex);
		else
			countingSort(b, fromIndex, toIndex);
	}

	/**
	 * Sorts a range of the specified {@link FloatBuffer} in ascending order (lowest
	 * first). The actual sorting algorithm used depends on the length of the range:
	 * <table border=1 summary="Sorting algorithm by array length">
	 * <tr>
	 * <th>Length</th>
	 * <th>Algorithm</th>
	 * </tr>
	 * <tr>
	 * <td>{@code [0 - 100)}</td>
	 * <td>{@link BufferSort#insertionSort(FloatBuffer, int, int)
	 * insertionSort}</td>
	 * </tr>
	 * <tr>
	 * <td>{@code 100+}</td>
	 * <td>{@link BufferSort#heapSort(FloatBuffer, int, int) heapSort}</td>
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
	public static void sort(FloatBuffer b, int fromIndex, int toIndex) {

		final int length = toIndex - fromIndex;

		if (length < SMALL_RANGE)
			insertionSort(b, fromIndex, toIndex);
		else
			heapSort(b, fromIndex, toIndex);
	}

	/**
	 * Sorts a range of the specified {@link DoubleBuffer} in ascending order
	 * (lowest first). The actual sorting algorithm used depends on the length of
	 * the range:
	 * <table border=1 summary="Sorting algorithm by array length">
	 * <tr>
	 * <th>Length</th>
	 * <th>Algorithm</th>
	 * </tr>
	 * <tr>
	 * <td>{@code [0 - 100)}</td>
	 * <td>{@link BufferSort#insertionSort(DoubleBuffer, int, int)
	 * insertionSort}</td>
	 * </tr>
	 * <tr>
	 * <td>{@code 100+}</td>
	 * <td>{@link BufferSort#heapSort(DoubleBuffer, int, int) heapSort}</td>
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
	public static void sort(DoubleBuffer b, int fromIndex, int toIndex) {

		final int length = toIndex - fromIndex;

		if (length < SMALL_RANGE)
			insertionSort(b, fromIndex, toIndex);
		else
			heapSort(b, fromIndex, toIndex);
	}

	/**
	 * Sorts a range of the specified {@link SmallIntBuffer} in ascending order
	 * (lowest first). The sort is:
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
	public static void heapSort(SmallIntBuffer b, int fromIndex, int toIndex) {
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
	private static void heapify(SmallIntBuffer b, int n, int i, int offset) {
		// Find largest among root, left child and right child
		int largest = i;
		int l = 2 * i + 1 - offset;
		int r = l + 1;

		if (l < n && b.get(l) > b.get(largest))
			largest = l;

		if (r < n && b.get(r) > b.get(largest))
			largest = r;

		// Swap and continue heapifying if root is not largest
		if (largest != i) {
			swap(b, i, largest);
			heapify(b, n, largest, offset);
		}
	}

	private static void swap(SmallIntBuffer b, int i, int j) {
		int swap = b.get(i);
		b.put(i, b.get(j));
		b.put(j, swap);
	}

	/**
	 * Sorts a range of the specified {@link SmallIntBuffer} in ascending order
	 * (lowest first). The sort is:
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
	public static void heapSort(SmallIntBuffer b, IntBinaryOperator comparator, int fromIndex, int toIndex) {
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
	private static void heapify(SmallIntBuffer b, IntBinaryOperator comparator, int n, int i, int offset) {
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

	/**
	 * Sorts a range of the specified {@link SmallLongBuffer} in ascending order
	 * (lowest first). The sort is:
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
	public static void heapSort(SmallLongBuffer b, int fromIndex, int toIndex) {
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
	private static void heapify(SmallLongBuffer b, int n, int i, int offset) {
		// Find largest among root, left child and right child
		int largest = i;
		int l = 2 * i + 1 - offset;
		int r = l + 1;

		if (l < n && b.get(l) > b.get(largest))
			largest = l;

		if (r < n && b.get(r) > b.get(largest))
			largest = r;

		// Swap and continue heapifying if root is not largest
		if (largest != i) {
			swap(b, i, largest);
			heapify(b, n, largest, offset);
		}
	}

	private static void swap(SmallLongBuffer b, int i, int j) {
		long swap = b.get(i);
		b.put(i, b.get(j));
		b.put(j, swap);
	}

	/**
	 * Sorts a range of the specified {@link SmallShortBuffer} in ascending order
	 * (lowest first). The sort is:
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
	public static void heapSort(SmallShortBuffer b, int fromIndex, int toIndex) {
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
	private static void heapify(SmallShortBuffer b, int n, int i, int offset) {
		// Find largest among root, left child and right child
		int largest = i;
		int l = 2 * i + 1 - offset;
		int r = l + 1;

		if (l < n && b.get(l) > b.get(largest))
			largest = l;

		if (r < n && b.get(r) > b.get(largest))
			largest = r;

		// Swap and continue heapifying if root is not largest
		if (largest != i) {
			swap(b, i, largest);
			heapify(b, n, largest, offset);
		}
	}

	private static void swap(SmallShortBuffer b, int i, int j) {
		short swap = b.get(i);
		b.put(i, b.get(j));
		b.put(j, swap);
	}

	/**
	 * Sorts a range of the specified {@link SmallByteBuffer} in ascending order
	 * (lowest first). The sort is:
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
	public static void heapSort(SmallByteBuffer b, int fromIndex, int toIndex) {
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
	private static void heapify(SmallByteBuffer b, int n, int i, int offset) {
		// Find largest among root, left child and right child
		int largest = i;
		int l = 2 * i + 1 - offset;
		int r = l + 1;

		if (l < n && b.get(l) > b.get(largest))
			largest = l;

		if (r < n && b.get(r) > b.get(largest))
			largest = r;

		// Swap and continue heapifying if root is not largest
		if (largest != i) {
			swap(b, i, largest);
			heapify(b, n, largest, offset);
		}
	}

	private static void swap(SmallByteBuffer b, int i, int j) {
		byte swap = b.get(i);
		b.put(i, b.get(j));
		b.put(j, swap);
	}

	/**
	 * Sorts a range of the specified {@link SmallFloatBuffer} in ascending order
	 * (lowest first). The sort is:
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
	public static void heapSort(SmallFloatBuffer b, int fromIndex, int toIndex) {
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
	private static void heapify(SmallFloatBuffer b, int n, int i, int offset) {
		// Find largest among root, left child and right child
		int largest = i;
		int l = 2 * i + 1 - offset;
		int r = l + 1;

		if (l < n && Float.compare(b.get(l), b.get(largest)) > 0)
			largest = l;

		if (r < n && Float.compare(b.get(r), b.get(largest)) > 0)
			largest = r;

		// Swap and continue heapifying if root is not largest
		if (largest != i) {
			swap(b, i, largest);
			heapify(b, n, largest, offset);
		}
	}

	private static void swap(SmallFloatBuffer b, int i, int j) {
		float swap = b.get(i);
		b.put(i, b.get(j));
		b.put(j, swap);
	}

	/**
	 * Sorts a range of the specified {@link SmallDoubleBuffer} in ascending order
	 * (lowest first). The sort is:
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
	public static void heapSort(SmallDoubleBuffer b, int fromIndex, int toIndex) {
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
	private static void heapify(SmallDoubleBuffer b, int n, int i, int offset) {
		// Find largest among root, left child and right child
		int largest = i;
		int l = 2 * i + 1 - offset;
		int r = l + 1;

		if (l < n && Double.compare(b.get(l), b.get(largest)) > 0)
			largest = l;

		if (r < n && Double.compare(b.get(r), b.get(largest)) > 0)
			largest = r;

		// Swap and continue heapifying if root is not largest
		if (largest != i) {
			swap(b, i, largest);
			heapify(b, n, largest, offset);
		}
	}

	private static void swap(SmallDoubleBuffer b, int i, int j) {
		double swap = b.get(i);
		b.put(i, b.get(j));
		b.put(j, swap);
	}

	/**
	 * Sorts a range of the specified {@link SmallIntBuffer} in ascending order
	 * (lowest first). The sort is:
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
	public static void radixSort(SmallIntBuffer b, int fromIndex, int toIndex) {
		rangeCheck(b.capacity(), fromIndex, toIndex);
		radixSort0(b, fromIndex, toIndex, INT_HIGH_BIT);
	}

	private static void radixSort0(SmallIntBuffer b, int fromIndex, int toIndex, int bit) {

		int zero = fromIndex;
		int one = toIndex;

		final int direction = bit == INT_HIGH_BIT ? bit : 0;

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

	/**
	 * Sorts a range of the specified {@link SmallLongBuffer} in ascending order
	 * (lowest first). The sort is:
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
	public static void radixSort(SmallLongBuffer b, int fromIndex, int toIndex) {
		rangeCheck(b.capacity(), fromIndex, toIndex);
		radixSort0(b, fromIndex, toIndex, LONG_HIGH_BIT);
	}

	private static void radixSort0(SmallLongBuffer b, int fromIndex, int toIndex, long bit) {

		int zero = fromIndex;
		int one = toIndex;

		final long direction = bit == LONG_HIGH_BIT ? bit : 0;

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

	/**
	 * Sorts a range of the specified {@link SmallShortBuffer} in ascending order
	 * (lowest first). This sort is {@code O(n)} in the worst case, but it creates
	 * and iterates over an {@code int} array of length 2^16.
	 *
	 * @param b         the buffer to be sorted
	 * @param fromIndex the index of the first element (inclusive) to be sorted
	 * @param toIndex   the index of the last element (exclusive) to be sorted
	 *
	 * @throws IllegalArgumentException  if {@code fromIndex > toIndex}
	 * @throws IndexOutOfBoundsException if
	 *                                   {@code fromIndex < 0 or toIndex > b.capacity()}
	 */
	public static void countingSort(SmallShortBuffer b, int fromIndex, int toIndex) {
		rangeCheck(b.capacity(), fromIndex, toIndex);

		int[] counts = new int[1 << 16];

		for (int i = fromIndex; i < toIndex; i++)
			counts[b.get(i) & 0xFFFF]++;

		int k = fromIndex;

		// negative values
		for (int i = Short.MAX_VALUE + 1; i < counts.length; i++) {
			short s = (short) i;
			for (int j = 0; j < counts[i]; j++)
				b.put(k++, s);
		}

		// positive values
		for (int i = 0; i <= Short.MAX_VALUE; i++) {
			short s = (short) i;
			for (int j = 0; j < counts[i]; j++)
				b.put(k++, s);
		}
	}

	/**
	 * Sorts a range of the specified {@link SmallByteBuffer} in ascending order
	 * (lowest first). This sort is {@code O(n)} in the worst case, but it creates
	 * and iterates over an {@code int} array of length 2^8.
	 *
	 * @param b         the buffer to be sorted
	 * @param fromIndex the index of the first element (inclusive) to be sorted
	 * @param toIndex   the index of the last element (exclusive) to be sorted
	 *
	 * @throws IllegalArgumentException  if {@code fromIndex > toIndex}
	 * @throws IndexOutOfBoundsException if
	 *                                   {@code fromIndex < 0 or toIndex > b.capacity()}
	 */
	public static void countingSort(SmallByteBuffer b, int fromIndex, int toIndex) {
		rangeCheck(b.capacity(), fromIndex, toIndex);

		int[] counts = new int[1 << 8];

		for (int i = fromIndex; i < toIndex; i++)
			counts[b.get(i) & 0xFF]++;

		int k = fromIndex;

		// negative values
		for (int i = Byte.MAX_VALUE + 1; i < counts.length; i++) {
			byte s = (byte) i;
			for (int j = 0; j < counts[i]; j++)
				b.put(k++, s);
		}

		// positive values
		for (int i = 0; i <= Byte.MAX_VALUE; i++) {
			byte s = (byte) i;
			for (int j = 0; j < counts[i]; j++)
				b.put(k++, s);
		}
	}

	/**
	 * Sorts a range of the specified {@link SmallIntBuffer} in ascending order
	 * (lowest first). The sort is:
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
	public static void insertionSort(SmallIntBuffer b, int fromIndex, int toIndex) {
		rangeCheck(b.capacity(), fromIndex, toIndex);

		for (int i = fromIndex + 1; i < toIndex; i++) {
			int x = b.get(i);
			int j = i - 1;
			for (int xj; j >= fromIndex && (xj = b.get(j)) > x; j--)
				b.put(j + 1, xj);
			b.put(j + 1, x);
		}
	}

	/**
	 * Sorts a range of the specified {@link SmallLongBuffer} in ascending order
	 * (lowest first). The sort is:
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
	public static void insertionSort(SmallLongBuffer b, int fromIndex, int toIndex) {
		rangeCheck(b.capacity(), fromIndex, toIndex);

		for (int i = fromIndex + 1; i < toIndex; i++) {
			long x = b.get(i);
			int j = i - 1;
			for (long xj; j >= fromIndex && (xj = b.get(j)) > x; j--)
				b.put(j + 1, xj);
			b.put(j + 1, x);
		}
	}

	/**
	 * Sorts a range of the specified {@link SmallShortBuffer} in ascending order
	 * (lowest first). The sort is:
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
	public static void insertionSort(SmallShortBuffer b, int fromIndex, int toIndex) {
		rangeCheck(b.capacity(), fromIndex, toIndex);

		for (int i = fromIndex + 1; i < toIndex; i++) {
			short x = b.get(i);
			int j = i - 1;
			for (short xj; j >= fromIndex && (xj = b.get(j)) > x; j--)
				b.put(j + 1, xj);
			b.put(j + 1, x);
		}
	}

	/**
	 * Sorts a range of the specified {@link SmallByteBuffer} in ascending order
	 * (lowest first). The sort is:
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
	public static void insertionSort(SmallByteBuffer b, int fromIndex, int toIndex) {
		rangeCheck(b.capacity(), fromIndex, toIndex);

		for (int i = fromIndex + 1; i < toIndex; i++) {
			byte x = b.get(i);
			int j = i - 1;
			for (byte xj; j >= fromIndex && (xj = b.get(j)) > x; j--)
				b.put(j + 1, xj);
			b.put(j + 1, x);
		}
	}

	/**
	 * Sorts a range of the specified {@link SmallFloatBuffer} in ascending order
	 * (lowest first). The sort is:
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
	public static void insertionSort(SmallFloatBuffer b, int fromIndex, int toIndex) {
		rangeCheck(b.capacity(), fromIndex, toIndex);

		for (int i = fromIndex + 1; i < toIndex; i++) {
			float x = b.get(i);
			int j = i - 1;
			for (float xj; j >= fromIndex && Float.compare(xj = b.get(j), x) > 0; j--)
				b.put(j + 1, xj);
			b.put(j + 1, x);
		}
	}

	/**
	 * Sorts a range of the specified {@link SmallDoubleBuffer} in ascending order
	 * (lowest first). The sort is:
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
	public static void insertionSort(SmallDoubleBuffer b, int fromIndex, int toIndex) {
		rangeCheck(b.capacity(), fromIndex, toIndex);

		for (int i = fromIndex + 1; i < toIndex; i++) {
			double x = b.get(i);
			int j = i - 1;
			for (double xj; j >= fromIndex && Double.compare(xj = b.get(j), x) > 0; j--)
				b.put(j + 1, xj);
			b.put(j + 1, x);
		}
	}

	/**
	 * Sorts a range of the specified {@link SmallIntBuffer} in ascending order
	 * (lowest first). The actual sorting algorithm used depends on the length of
	 * the range:
	 * <table border=1 summary="Sorting algorithm by array length">
	 * <tr>
	 * <th>Length</th>
	 * <th>Algorithm</th>
	 * </tr>
	 * <tr>
	 * <td>{@code [0 - 100)}</td>
	 * <td>{@link BufferSort#insertionSort(SmallIntBuffer, int, int)
	 * insertionSort}</td>
	 * </tr>
	 * <tr>
	 * <td>{@code [100 - 10^7)}</td>
	 * <td>{@link BufferSort#heapSort(SmallIntBuffer, int, int) heapSort}</td>
	 * </tr>
	 * <tr>
	 * <td>{@code 10^7+}</td>
	 * <td>{@link BufferSort#radixSort(SmallIntBuffer, int, int) radixSort}</td>
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
	public static void sort(SmallIntBuffer b, int fromIndex, int toIndex) {

		final int length = toIndex - fromIndex;

		if (length < SMALL_RANGE)
			insertionSort(b, fromIndex, toIndex);
		else if (length < LARGE_RANGE)
			heapSort(b, fromIndex, toIndex);
		else
			radixSort(b, fromIndex, toIndex);
	}

	/**
	 * Sorts a range of the specified {@link SmallLongBuffer} in ascending order
	 * (lowest first). The actual sorting algorithm used depends on the length of
	 * the range:
	 * <table border=1 summary="Sorting algorithm by array length">
	 * <tr>
	 * <th>Length</th>
	 * <th>Algorithm</th>
	 * </tr>
	 * <tr>
	 * <td>{@code [0 - 100)}</td>
	 * <td>{@link BufferSort#insertionSort(SmallLongBuffer, int, int)
	 * insertionSort}</td>
	 * </tr>
	 * <tr>
	 * <td>{@code [100 - 10^7)}</td>
	 * <td>{@link BufferSort#heapSort(SmallLongBuffer, int, int) heapSort}</td>
	 * </tr>
	 * <tr>
	 * <td>{@code 10^7+}</td>
	 * <td>{@link BufferSort#radixSort(SmallLongBuffer, int, int) radixSort}</td>
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
	public static void sort(SmallLongBuffer b, int fromIndex, int toIndex) {

		final int length = toIndex - fromIndex;

		if (length < SMALL_RANGE)
			insertionSort(b, fromIndex, toIndex);
		else if (length < LARGE_RANGE)
			heapSort(b, fromIndex, toIndex);
		else
			radixSort(b, fromIndex, toIndex);
	}

	/**
	 * Sorts a range of the specified {@link SmallShortBuffer} in ascending order
	 * (lowest first). The actual sorting algorithm used depends on the length of
	 * the range:
	 * <table border=1 summary="Sorting algorithm by array length">
	 * <tr>
	 * <th>Length</th>
	 * <th>Algorithm</th>
	 * </tr>
	 * <tr>
	 * <td>{@code [0 - 100)}</td>
	 * <td>{@link BufferSort#insertionSort(SmallShortBuffer, int, int)
	 * insertionSort}</td>
	 * </tr>
	 * <tr>
	 * <td>{@code [100 - 10^7)}</td>
	 * <td>{@link BufferSort#heapSort(SmallShortBuffer, int, int) heapSort}</td>
	 * </tr>
	 * <tr>
	 * <td>{@code 10^7+}</td>
	 * <td>{@link BufferSort#countingSort(SmallShortBuffer, int, int)
	 * countingSort}</td>
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
	public static void sort(SmallShortBuffer b, int fromIndex, int toIndex) {

		final int length = toIndex - fromIndex;

		if (length < SMALL_RANGE)
			insertionSort(b, fromIndex, toIndex);
		else if (length < LARGE_RANGE)
			heapSort(b, fromIndex, toIndex);
		else
			countingSort(b, fromIndex, toIndex);
	}

	/**
	 * Sorts a range of the specified {@link SmallByteBuffer} in ascending order
	 * (lowest first). The actual sorting algorithm used depends on the length of
	 * the range:
	 * <table border=1 summary="Sorting algorithm by array length">
	 * <tr>
	 * <th>Length</th>
	 * <th>Algorithm</th>
	 * </tr>
	 * <tr>
	 * <td>{@code [0 - 100)}</td>
	 * <td>{@link BufferSort#insertionSort(SmallByteBuffer, int, int)
	 * insertionSort}</td>
	 * </tr>
	 * <tr>
	 * <td>{@code [100 - 10^5)}</td>
	 * <td>{@link BufferSort#heapSort(SmallByteBuffer, int, int) heapSort}</td>
	 * </tr>
	 * <tr>
	 * <td>{@code 10^5+}</td>
	 * <td>{@link BufferSort#countingSort(SmallByteBuffer, int, int)
	 * countingSort}</td>
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
	public static void sort(SmallByteBuffer b, int fromIndex, int toIndex) {

		final int length = toIndex - fromIndex;

		if (length < SMALL_RANGE)
			insertionSort(b, fromIndex, toIndex);
		else if (length < 100000)
			heapSort(b, fromIndex, toIndex);
		else
			countingSort(b, fromIndex, toIndex);
	}

	/**
	 * Sorts a range of the specified {@link SmallFloatBuffer} in ascending order
	 * (lowest first). The actual sorting algorithm used depends on the length of
	 * the range:
	 * <table border=1 summary="Sorting algorithm by array length">
	 * <tr>
	 * <th>Length</th>
	 * <th>Algorithm</th>
	 * </tr>
	 * <tr>
	 * <td>{@code [0 - 100)}</td>
	 * <td>{@link BufferSort#insertionSort(SmallFloatBuffer, int, int)
	 * insertionSort}</td>
	 * </tr>
	 * <tr>
	 * <td>{@code 100+}</td>
	 * <td>{@link BufferSort#heapSort(SmallFloatBuffer, int, int) heapSort}</td>
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
	public static void sort(SmallFloatBuffer b, int fromIndex, int toIndex) {

		final int length = toIndex - fromIndex;

		if (length < SMALL_RANGE)
			insertionSort(b, fromIndex, toIndex);
		else
			heapSort(b, fromIndex, toIndex);
	}

	/**
	 * Sorts a range of the specified {@link SmallDoubleBuffer} in ascending order
	 * (lowest first). The actual sorting algorithm used depends on the length of
	 * the range:
	 * <table border=1 summary="Sorting algorithm by array length">
	 * <tr>
	 * <th>Length</th>
	 * <th>Algorithm</th>
	 * </tr>
	 * <tr>
	 * <td>{@code [0 - 100)}</td>
	 * <td>{@link BufferSort#insertionSort(SmallDoubleBuffer, int, int)
	 * insertionSort}</td>
	 * </tr>
	 * <tr>
	 * <td>{@code 100+}</td>
	 * <td>{@link BufferSort#heapSort(SmallDoubleBuffer, int, int) heapSort}</td>
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
	public static void sort(SmallDoubleBuffer b, int fromIndex, int toIndex) {

		final int length = toIndex - fromIndex;

		if (length < SMALL_RANGE)
			insertionSort(b, fromIndex, toIndex);
		else
			heapSort(b, fromIndex, toIndex);
	}

}
