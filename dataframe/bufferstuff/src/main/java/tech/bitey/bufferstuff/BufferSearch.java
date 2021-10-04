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

	/**
	 * Searches a range of the specified {@link IntBuffer} for the specified value
	 * using the binary search algorithm. The range must be sorted in ascending
	 * order prior to making this call. If it is not sorted, the results are
	 * undefined. If the range contains multiple elements with the specified value,
	 * there is no guarantee which one will be found.
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
	public static int binarySearch(IntBuffer b, int fromIndex, int toIndex, int key) {
		rangeCheck(b.capacity(), fromIndex, toIndex);
		return binarySearch0(b, fromIndex, toIndex, key);
	}

	private static int binarySearch0(IntBuffer b, int fromIndex, int toIndex, int key) {
		int low = fromIndex;
		int high = toIndex - 1;

		while (low <= high) {
			int mid = (low + high) >>> 1;
			int midVal = b.get(mid);

			if (midVal < key)
				low = mid + 1;
			else if (midVal > key)
				high = mid - 1;
			else
				return mid; // key found
		}
		return -(low + 1); // key not found.
	}

	/**
	 * Searches a range of the specified {@link LongBuffer} for the specified value
	 * using the binary search algorithm. The range must be sorted in ascending
	 * order prior to making this call. If it is not sorted, the results are
	 * undefined. If the range contains multiple elements with the specified value,
	 * there is no guarantee which one will be found.
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
	public static int binarySearch(LongBuffer b, int fromIndex, int toIndex, long key) {
		rangeCheck(b.capacity(), fromIndex, toIndex);
		return binarySearch0(b, fromIndex, toIndex, key);
	}

	private static int binarySearch0(LongBuffer b, int fromIndex, int toIndex, long key) {
		int low = fromIndex;
		int high = toIndex - 1;

		while (low <= high) {
			int mid = (low + high) >>> 1;
			long midVal = b.get(mid);

			if (midVal < key)
				low = mid + 1;
			else if (midVal > key)
				high = mid - 1;
			else
				return mid; // key found
		}
		return -(low + 1); // key not found.
	}

	/**
	 * Searches a range of the specified {@link ShortBuffer} for the specified value
	 * using the binary search algorithm. The range must be sorted in ascending
	 * order prior to making this call. If it is not sorted, the results are
	 * undefined. If the range contains multiple elements with the specified value,
	 * there is no guarantee which one will be found.
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
	public static int binarySearch(ShortBuffer b, int fromIndex, int toIndex, short key) {
		rangeCheck(b.capacity(), fromIndex, toIndex);
		return binarySearch0(b, fromIndex, toIndex, key);
	}

	private static int binarySearch0(ShortBuffer b, int fromIndex, int toIndex, short key) {
		int low = fromIndex;
		int high = toIndex - 1;

		while (low <= high) {
			int mid = (low + high) >>> 1;
			short midVal = b.get(mid);

			if (midVal < key)
				low = mid + 1;
			else if (midVal > key)
				high = mid - 1;
			else
				return mid; // key found
		}
		return -(low + 1); // key not found.
	}

	/**
	 * Searches a range of the specified {@link ByteBuffer} for the specified value
	 * using the binary search algorithm. The range must be sorted in ascending
	 * order prior to making this call. If it is not sorted, the results are
	 * undefined. If the range contains multiple elements with the specified value,
	 * there is no guarantee which one will be found.
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
	public static int binarySearch(ByteBuffer b, int fromIndex, int toIndex, byte key) {
		rangeCheck(b.capacity(), fromIndex, toIndex);
		return binarySearch0(b, fromIndex, toIndex, key);
	}

	private static int binarySearch0(ByteBuffer b, int fromIndex, int toIndex, byte key) {
		int low = fromIndex;
		int high = toIndex - 1;

		while (low <= high) {
			int mid = (low + high) >>> 1;
			byte midVal = b.get(mid);

			if (midVal < key)
				low = mid + 1;
			else if (midVal > key)
				high = mid - 1;
			else
				return mid; // key found
		}
		return -(low + 1); // key not found.
	}

	/**
	 * Searches a range of the specified {@link FloatBuffer} for the specified value
	 * using the binary search algorithm. The range must be sorted in ascending
	 * order prior to making this call. If it is not sorted, the results are
	 * undefined. If the range contains multiple elements with the specified value,
	 * there is no guarantee which one will be found. This method considers all NaN
	 * values to be equivalent and equal.
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
	public static int binarySearch(FloatBuffer b, int fromIndex, int toIndex, float key) {
		rangeCheck(b.capacity(), fromIndex, toIndex);
		return binarySearch0(b, fromIndex, toIndex, key);
	}

	private static int binarySearch0(FloatBuffer b, int fromIndex, int toIndex, float key) {
		int low = fromIndex;
		int high = toIndex - 1;

		while (low <= high) {
			int mid = (low + high) >>> 1;
			float midVal = b.get(mid);

			if (midVal < key)
				low = mid + 1; // Neither val is NaN, thisVal is smaller
			else if (midVal > key)
				high = mid - 1; // Neither val is NaN, thisVal is larger
			else {
				int midBits = Float.floatToIntBits(midVal);
				int keyBits = Float.floatToIntBits(key);
				if (midBits == keyBits) // Values are equal
					return mid; // Key found
				else if (midBits < keyBits) // (-0.0, 0.0) or (!NaN, NaN)
					low = mid + 1;
				else // (0.0, -0.0) or (NaN, !NaN)
					high = mid - 1;
			}
		}
		return -(low + 1); // key not found.
	}

	/**
	 * Searches a range of the specified {@link DoubleBuffer} for the specified
	 * value using the binary search algorithm. The range must be sorted in
	 * ascending order prior to making this call. If it is not sorted, the results
	 * are undefined. If the range contains multiple elements with the specified
	 * value, there is no guarantee which one will be found. This method considers
	 * all NaN values to be equivalent and equal.
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
	public static int binarySearch(DoubleBuffer b, int fromIndex, int toIndex, double key) {
		rangeCheck(b.capacity(), fromIndex, toIndex);
		return binarySearch0(b, fromIndex, toIndex, key);
	}

	private static int binarySearch0(DoubleBuffer b, int fromIndex, int toIndex, double key) {
		int low = fromIndex;
		int high = toIndex - 1;

		while (low <= high) {
			int mid = (low + high) >>> 1;
			double midVal = b.get(mid);

			if (midVal < key)
				low = mid + 1; // Neither val is NaN, thisVal is smaller
			else if (midVal > key)
				high = mid - 1; // Neither val is NaN, thisVal is larger
			else {
				long midBits = Double.doubleToLongBits(midVal);
				long keyBits = Double.doubleToLongBits(key);
				if (midBits == keyBits) // Values are equal
					return mid; // Key found
				else if (midBits < keyBits) // (-0.0, 0.0) or (!NaN, NaN)
					low = mid + 1;
				else // (0.0, -0.0) or (NaN, !NaN)
					high = mid - 1;
			}
		}
		return -(low + 1); // key not found.
	}

	/**
	 * Searches a range of the specified {@link IntBuffer} for the first occurrence
	 * of the value at the given {@code keyIndex}. The range must be sorted in
	 * ascending order prior to making this call. If it is not sorted, the results
	 * are undefined.
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
	public static int binaryFindFirst(IntBuffer b, int minIndex, int keyIndex) {
		rangeCheckInclusive(b.capacity(), minIndex, keyIndex);
		return binaryFindFirst(b, minIndex, keyIndex, b.get(keyIndex));
	}

	private static int binaryFindFirst(IntBuffer b, int minIndex, int fromIndex, int key) {

		while (minIndex != fromIndex && b.get(fromIndex - 1) == key) {

			int range = 1, rangeIndex;
			do {
				range <<= 1;
				rangeIndex = fromIndex - range;
			} while (minIndex <= rangeIndex && b.get(rangeIndex) == key);

			fromIndex -= range >> 1;
		}

		return fromIndex;
	}

	/**
	 * Searches a range of the specified {@link IntBuffer} for the last occurrence
	 * of the value at the given {@code keyIndex}. The range must be sorted in
	 * ascending order prior to making this call. If it is not sorted, the results
	 * are undefined.
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
	public static int binaryFindLast(IntBuffer b, int maxIndex, int keyIndex) {
		rangeCheck(b.capacity(), keyIndex, maxIndex);
		if (keyIndex == maxIndex)
			return keyIndex;
		else
			return binaryFindLast(b, keyIndex, maxIndex - 1, b.get(keyIndex));
	}

	private static int binaryFindLast(IntBuffer b, int fromIndex, int maxIndex, int key) {

		while (fromIndex != maxIndex && b.get(fromIndex + 1) == key) {

			int range = 1, rangeIndex;
			do {
				range <<= 1;
				rangeIndex = fromIndex + range;
			} while (rangeIndex <= maxIndex && b.get(rangeIndex) == key);

			fromIndex += range >> 1;
		}

		return fromIndex;
	}

	/**
	 * Searches a range of the specified {@link LongBuffer} for the first occurrence
	 * of the value at the given {@code keyIndex}. The range must be sorted in
	 * ascending order prior to making this call. If it is not sorted, the results
	 * are undefined.
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
	public static int binaryFindFirst(LongBuffer b, int minIndex, int keyIndex) {
		rangeCheckInclusive(b.capacity(), minIndex, keyIndex);
		return binaryFindFirst(b, minIndex, keyIndex, b.get(keyIndex));
	}

	private static int binaryFindFirst(LongBuffer b, int minIndex, int fromIndex, long key) {

		while (minIndex != fromIndex && b.get(fromIndex - 1) == key) {

			int range = 1, rangeIndex;
			do {
				range <<= 1;
				rangeIndex = fromIndex - range;
			} while (minIndex <= rangeIndex && b.get(rangeIndex) == key);

			fromIndex -= range >> 1;
		}

		return fromIndex;
	}

	/**
	 * Searches a range of the specified {@link LongBuffer} for the last occurrence
	 * of the value at the given {@code keyIndex}. The range must be sorted in
	 * ascending order prior to making this call. If it is not sorted, the results
	 * are undefined.
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
	public static int binaryFindLast(LongBuffer b, int maxIndex, int keyIndex) {
		rangeCheck(b.capacity(), keyIndex, maxIndex);
		if (keyIndex == maxIndex)
			return keyIndex;
		else
			return binaryFindLast(b, keyIndex, maxIndex - 1, b.get(keyIndex));
	}

	private static int binaryFindLast(LongBuffer b, int fromIndex, int maxIndex, long key) {

		while (fromIndex != maxIndex && b.get(fromIndex + 1) == key) {

			int range = 1, rangeIndex;
			do {
				range <<= 1;
				rangeIndex = fromIndex + range;
			} while (rangeIndex <= maxIndex && b.get(rangeIndex) == key);

			fromIndex += range >> 1;
		}

		return fromIndex;
	}

	/**
	 * Searches a range of the specified {@link ShortBuffer} for the first
	 * occurrence of the value at the given {@code keyIndex}. The range must be
	 * sorted in ascending order prior to making this call. If it is not sorted, the
	 * results are undefined.
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
	public static int binaryFindFirst(ShortBuffer b, int minIndex, int keyIndex) {
		rangeCheckInclusive(b.capacity(), minIndex, keyIndex);
		return binaryFindFirst(b, minIndex, keyIndex, b.get(keyIndex));
	}

	private static int binaryFindFirst(ShortBuffer b, int minIndex, int fromIndex, short key) {

		while (minIndex != fromIndex && b.get(fromIndex - 1) == key) {

			int range = 1, rangeIndex;
			do {
				range <<= 1;
				rangeIndex = fromIndex - range;
			} while (minIndex <= rangeIndex && b.get(rangeIndex) == key);

			fromIndex -= range >> 1;
		}

		return fromIndex;
	}

	/**
	 * Searches a range of the specified {@link ShortBuffer} for the last occurrence
	 * of the value at the given {@code keyIndex}. The range must be sorted in
	 * ascending order prior to making this call. If it is not sorted, the results
	 * are undefined.
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
	public static int binaryFindLast(ShortBuffer b, int maxIndex, int keyIndex) {
		rangeCheck(b.capacity(), keyIndex, maxIndex);
		if (keyIndex == maxIndex)
			return keyIndex;
		else
			return binaryFindLast(b, keyIndex, maxIndex - 1, b.get(keyIndex));
	}

	private static int binaryFindLast(ShortBuffer b, int fromIndex, int maxIndex, short key) {

		while (fromIndex != maxIndex && b.get(fromIndex + 1) == key) {

			int range = 1, rangeIndex;
			do {
				range <<= 1;
				rangeIndex = fromIndex + range;
			} while (rangeIndex <= maxIndex && b.get(rangeIndex) == key);

			fromIndex += range >> 1;
		}

		return fromIndex;
	}

	/**
	 * Searches a range of the specified {@link ByteBuffer} for the first occurrence
	 * of the value at the given {@code keyIndex}. The range must be sorted in
	 * ascending order prior to making this call. If it is not sorted, the results
	 * are undefined.
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
	public static int binaryFindFirst(ByteBuffer b, int minIndex, int keyIndex) {
		rangeCheckInclusive(b.capacity(), minIndex, keyIndex);
		return binaryFindFirst(b, minIndex, keyIndex, b.get(keyIndex));
	}

	private static int binaryFindFirst(ByteBuffer b, int minIndex, int fromIndex, byte key) {

		while (minIndex != fromIndex && b.get(fromIndex - 1) == key) {

			int range = 1, rangeIndex;
			do {
				range <<= 1;
				rangeIndex = fromIndex - range;
			} while (minIndex <= rangeIndex && b.get(rangeIndex) == key);

			fromIndex -= range >> 1;
		}

		return fromIndex;
	}

	/**
	 * Searches a range of the specified {@link ByteBuffer} for the last occurrence
	 * of the value at the given {@code keyIndex}. The range must be sorted in
	 * ascending order prior to making this call. If it is not sorted, the results
	 * are undefined.
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
	public static int binaryFindLast(ByteBuffer b, int maxIndex, int keyIndex) {
		rangeCheck(b.capacity(), keyIndex, maxIndex);
		if (keyIndex == maxIndex)
			return keyIndex;
		else
			return binaryFindLast(b, keyIndex, maxIndex - 1, b.get(keyIndex));
	}

	private static int binaryFindLast(ByteBuffer b, int fromIndex, int maxIndex, byte key) {

		while (fromIndex != maxIndex && b.get(fromIndex + 1) == key) {

			int range = 1, rangeIndex;
			do {
				range <<= 1;
				rangeIndex = fromIndex + range;
			} while (rangeIndex <= maxIndex && b.get(rangeIndex) == key);

			fromIndex += range >> 1;
		}

		return fromIndex;
	}

	/**
	 * Searches a range of the specified {@link FloatBuffer} for the first
	 * occurrence of the value at the given {@code keyIndex}. The range must be
	 * sorted in ascending order prior to making this call. If it is not sorted, the
	 * results are undefined. This method considers all NaN values to be equivalent
	 * and equal.
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
	public static int binaryFindFirst(FloatBuffer b, int minIndex, int keyIndex) {
		rangeCheckInclusive(b.capacity(), minIndex, keyIndex);
		return binaryFindFirst(b, minIndex, keyIndex, b.get(keyIndex));
	}

	private static int binaryFindFirst(FloatBuffer b, int minIndex, int fromIndex, float key) {

		while (minIndex != fromIndex && Float.compare(b.get(fromIndex - 1), key) == 0) {

			int range = 1, rangeIndex;
			do {
				range <<= 1;
				rangeIndex = fromIndex - range;
			} while (minIndex <= rangeIndex && Float.compare(b.get(rangeIndex), key) == 0);

			fromIndex -= range >> 1;
		}

		return fromIndex;
	}

	/**
	 * Searches a range of the specified {@link FloatBuffer} for the last occurrence
	 * of the value at the given {@code keyIndex}. The range must be sorted in
	 * ascending order prior to making this call. If it is not sorted, the results
	 * are undefined. This method considers all NaN values to be equivalent and
	 * equal.
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
	public static int binaryFindLast(FloatBuffer b, int maxIndex, int keyIndex) {
		rangeCheck(b.capacity(), keyIndex, maxIndex);
		if (keyIndex == maxIndex)
			return keyIndex;
		else
			return binaryFindLast(b, keyIndex, maxIndex - 1, b.get(keyIndex));
	}

	private static int binaryFindLast(FloatBuffer b, int fromIndex, int maxIndex, float key) {

		while (fromIndex != maxIndex && Float.compare(b.get(fromIndex + 1), key) == 0) {

			int range = 1, rangeIndex;
			do {
				range <<= 1;
				rangeIndex = fromIndex + range;
			} while (rangeIndex <= maxIndex && Float.compare(b.get(rangeIndex), key) == 0);

			fromIndex += range >> 1;
		}

		return fromIndex;
	}

	/**
	 * Searches a range of the specified {@link DoubleBuffer} for the first
	 * occurrence of the value at the given {@code keyIndex}. The range must be
	 * sorted in ascending order prior to making this call. If it is not sorted, the
	 * results are undefined. This method considers all NaN values to be equivalent
	 * and equal.
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
	public static int binaryFindFirst(DoubleBuffer b, int minIndex, int keyIndex) {
		rangeCheckInclusive(b.capacity(), minIndex, keyIndex);
		return binaryFindFirst(b, minIndex, keyIndex, b.get(keyIndex));
	}

	private static int binaryFindFirst(DoubleBuffer b, int minIndex, int fromIndex, double key) {

		while (minIndex != fromIndex && Double.compare(b.get(fromIndex - 1), key) == 0) {

			int range = 1, rangeIndex;
			do {
				range <<= 1;
				rangeIndex = fromIndex - range;
			} while (minIndex <= rangeIndex && Double.compare(b.get(rangeIndex), key) == 0);

			fromIndex -= range >> 1;
		}

		return fromIndex;
	}

	/**
	 * Searches a range of the specified {@link DoubleBuffer} for the last
	 * occurrence of the value at the given {@code keyIndex}. The range must be
	 * sorted in ascending order prior to making this call. If it is not sorted, the
	 * results are undefined. This method considers all NaN values to be equivalent
	 * and equal.
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
	public static int binaryFindLast(DoubleBuffer b, int maxIndex, int keyIndex) {
		rangeCheck(b.capacity(), keyIndex, maxIndex);
		if (keyIndex == maxIndex)
			return keyIndex;
		else
			return binaryFindLast(b, keyIndex, maxIndex - 1, b.get(keyIndex));
	}

	private static int binaryFindLast(DoubleBuffer b, int fromIndex, int maxIndex, double key) {

		while (fromIndex != maxIndex && Double.compare(b.get(fromIndex + 1), key) == 0) {

			int range = 1, rangeIndex;
			do {
				range <<= 1;
				rangeIndex = fromIndex + range;
			} while (rangeIndex <= maxIndex && Double.compare(b.get(rangeIndex), key) == 0);

			fromIndex += range >> 1;
		}

		return fromIndex;
	}
}
