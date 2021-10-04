package tech.bitey.bufferstuff;

import static java.util.Spliterator.IMMUTABLE;
import static java.util.Spliterator.NONNULL;
import static java.util.Spliterator.ORDERED;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.DoubleBuffer;
import java.nio.FloatBuffer;
import java.nio.IntBuffer;
import java.nio.LongBuffer;
import java.nio.ShortBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.StreamSupport;

/**
 * Utility methods for working with nio buffers.
 * 
 * @author biteytech@protonmail.com
 */
public enum BufferUtils {
	; // static methods only, enum prevents instantiation

	private static final boolean DIRECT = "true".equalsIgnoreCase(System.getProperty("tech.bitey.allocateDirect"));

	/**
	 * An empty, read-only {@link ByteBuffer} which has
	 * {@link ByteOrder#nativeOrder() native order}
	 */
	public static final ByteBuffer EMPTY_BUFFER = asReadOnlyBuffer(allocate(0));

	/**
	 * Allocates a new {@link ByteBuffer} with the specified capacity. The buffer
	 * will be direct if the {@code tech.bitey.allocateDirect} system property is
	 * set to "true", and will have {@link ByteOrder#nativeOrder() native order}.
	 * 
	 * @param capacity the new buffer's capacity, in bytes
	 * 
	 * @return the new native order {@code ByteBuffer}
	 */
	public static ByteBuffer allocate(int capacity) {
		return allocate(capacity, ByteOrder.nativeOrder());
	}

	/**
	 * Allocates a new {@link ByteBuffer} with the specified capacity. The buffer
	 * will be direct if the {@code tech.bitey.allocateDirect} system property is
	 * set to "true", and will have the specified {@link ByteOrder}.
	 * 
	 * @param capacity the new buffer's capacity, in bytes
	 * @param order    the {@code ByteOrder}
	 * 
	 * @return the new {@code ByteBuffer}
	 */
	public static ByteBuffer allocate(int capacity, ByteOrder order) {
		return (DIRECT ? ByteBuffer.allocateDirect(capacity) : ByteBuffer.allocate(capacity)).order(order);
	}

	/**
	 * Duplicate a {@link ByteBuffer} and preserve the order. Equivalent to:
	 * 
	 * <pre>
	 * b.duplicate().order(b.order())
	 * </pre>
	 * 
	 * @param b - the buffer to be duplicated
	 * 
	 * @return duplicated buffer with order preserved
	 * 
	 * @see ByteBuffer#duplicate()
	 * @see ByteBuffer#order()
	 */
	public static ByteBuffer duplicate(ByteBuffer b) {
		return b.duplicate().order(b.order());
	}

	/**
	 * Slice a {@link ByteBuffer} and preserve the order. Equivalent to:
	 * 
	 * <pre>
	 * b.slice().order(b.order())
	 * </pre>
	 * 
	 * @param b - the buffer to be sliced
	 * 
	 * @return sliced buffer with order preserved
	 * 
	 * @see ByteBuffer#slice()
	 * @see ByteBuffer#order()
	 */
	public static ByteBuffer slice(ByteBuffer b) {
		return b.slice().order(b.order());
	}

	/**
	 * Creates a new, read-only byte buffer that shares the specified buffer's
	 * content, and preserves it's order. Equivalent to:
	 * 
	 * <pre>
	 * b.asReadOnlyBuffer().order(b.order())
	 * </pre>
	 * 
	 * @param b - the buffer to be made read-only
	 * 
	 * @return read-only byte buffer that shares the specified buffer's content
	 * 
	 * @see ByteBuffer#asReadOnlyBuffer()
	 * @see ByteBuffer#order()
	 */
	public static ByteBuffer asReadOnlyBuffer(ByteBuffer b) {
		return b.asReadOnlyBuffer().order(b.order());
	}

	/**
	 * Slice a range from the specified {@link ByteBuffer}. The buffer's order is
	 * preserved.
	 * 
	 * @param b         - the buffer to be sliced
	 * @param fromIndex - the index of the first element in the range (inclusive)
	 * @param toIndex   - the index of the last element in the range (exclusive)
	 * 
	 * @return sliced buffer with order preserved
	 * 
	 * @throws IllegalArgumentException  if {@code fromIndex > toIndex}
	 * @throws IndexOutOfBoundsException if
	 *                                   {@code fromIndex < 0 or toIndex > b.capacity()}
	 * 
	 * @see ByteBuffer#slice()
	 * @see ByteBuffer#order()
	 */
	public static ByteBuffer slice(ByteBuffer b, int fromIndex, int toIndex) {
		rangeCheck(b.capacity(), fromIndex, toIndex);

		ByteBuffer dup = duplicate(b);
		dup.limit(toIndex);
		dup.position(fromIndex);
		return slice(dup);
	}

	/**
	 * Determines if the specified buffer is sorted inside the specified range. That
	 * is: {@code buffer[i] <= buffer[i + 1]} for all elements in the range. A range
	 * of length zero or one is considered sorted.
	 * 
	 * @param b         - the buffer to be checked
	 * @param fromIndex - the index of the first element (inclusive) to be checked
	 * @param toIndex   - the index of the last element (exclusive) to be checked
	 * 
	 * @return true if the buffer is sorted inside the specified range
	 * 
	 * @throws IllegalArgumentException  if {@code fromIndex > toIndex}
	 * @throws IndexOutOfBoundsException if
	 *                                   {@code fromIndex < 0 or toIndex > b.capacity()}
	 */
	public static boolean isSorted(IntBuffer b, int fromIndex, int toIndex) {
		rangeCheck(b.capacity(), fromIndex, toIndex);

		if (toIndex - fromIndex <= 1)
			return true;

		int prev = b.get(fromIndex);
		for (int i = fromIndex + 1; i < toIndex; i++) {
			int value = b.get(i);
			if (prev > value)
				return false;
			prev = value;
		}

		return true;
	}

	/**
	 * Determines if the specified buffer is sorted and distinct inside the
	 * specified range. That is: {@code buffer[i] < buffer[i + 1]} for all elements
	 * in the range. A range of length zero or one is considered sorted and
	 * distinct.
	 * 
	 * @param b         - the buffer to be checked
	 * @param fromIndex - the index of the first element (inclusive) to be checked
	 * @param toIndex   - the index of the last element (exclusive) to be checked
	 * 
	 * @return true if the buffer is sorted and distinct inside the specified range
	 * 
	 * @throws IllegalArgumentException  if {@code fromIndex > toIndex}
	 * @throws IndexOutOfBoundsException if
	 *                                   {@code fromIndex < 0 or toIndex > b.capacity()}
	 */
	public static boolean isSortedAndDistinct(IntBuffer b, int fromIndex, int toIndex) {
		rangeCheck(b.capacity(), fromIndex, toIndex);

		if (toIndex - fromIndex <= 1)
			return true;

		int prev = b.get(fromIndex);
		for (int i = fromIndex + 1; i < toIndex; i++) {
			int value = b.get(i);
			if (prev >= value)
				return false;
			prev = value;
		}

		return true;
	}

	/**
	 * Determines if the specified buffer is sorted inside the specified range. That
	 * is: {@code buffer[i] <= buffer[i + 1]} for all elements in the range. A range
	 * of length zero or one is considered sorted.
	 * 
	 * @param b         - the buffer to be checked
	 * @param fromIndex - the index of the first element (inclusive) to be checked
	 * @param toIndex   - the index of the last element (exclusive) to be checked
	 * 
	 * @return true if the buffer is sorted inside the specified range
	 * 
	 * @throws IllegalArgumentException  if {@code fromIndex > toIndex}
	 * @throws IndexOutOfBoundsException if
	 *                                   {@code fromIndex < 0 or toIndex > b.capacity()}
	 */
	public static boolean isSorted(LongBuffer b, int fromIndex, int toIndex) {
		rangeCheck(b.capacity(), fromIndex, toIndex);

		if (toIndex - fromIndex <= 1)
			return true;

		long prev = b.get(fromIndex);
		for (int i = fromIndex + 1; i < toIndex; i++) {
			long value = b.get(i);
			if (prev > value)
				return false;
			prev = value;
		}

		return true;
	}

	/**
	 * Determines if the specified buffer is sorted and distinct inside the
	 * specified range. That is: {@code buffer[i] < buffer[i + 1]} for all elements
	 * in the range. A range of length zero or one is considered sorted and
	 * distinct.
	 * 
	 * @param b         - the buffer to be checked
	 * @param fromIndex - the index of the first element (inclusive) to be checked
	 * @param toIndex   - the index of the last element (exclusive) to be checked
	 * 
	 * @return true if the buffer is sorted and distinct inside the specified range
	 * 
	 * @throws IllegalArgumentException  if {@code fromIndex > toIndex}
	 * @throws IndexOutOfBoundsException if
	 *                                   {@code fromIndex < 0 or toIndex > b.capacity()}
	 */
	public static boolean isSortedAndDistinct(LongBuffer b, int fromIndex, int toIndex) {
		rangeCheck(b.capacity(), fromIndex, toIndex);

		if (toIndex - fromIndex <= 1)
			return true;

		long prev = b.get(fromIndex);
		for (int i = fromIndex + 1; i < toIndex; i++) {
			long value = b.get(i);
			if (prev >= value)
				return false;
			prev = value;
		}

		return true;
	}

	/**
	 * Determines if the specified buffer is sorted inside the specified range. That
	 * is: {@code buffer[i] <= buffer[i + 1]} for all elements in the range. A range
	 * of length zero or one is considered sorted.
	 * 
	 * @param b         - the buffer to be checked
	 * @param fromIndex - the index of the first element (inclusive) to be checked
	 * @param toIndex   - the index of the last element (exclusive) to be checked
	 * 
	 * @return true if the buffer is sorted inside the specified range
	 * 
	 * @throws IllegalArgumentException  if {@code fromIndex > toIndex}
	 * @throws IndexOutOfBoundsException if
	 *                                   {@code fromIndex < 0 or toIndex > b.capacity()}
	 */
	public static boolean isSorted(ByteBuffer b, int fromIndex, int toIndex) {
		rangeCheck(b.capacity(), fromIndex, toIndex);

		if (toIndex - fromIndex <= 1)
			return true;

		byte prev = b.get(fromIndex);
		for (int i = fromIndex + 1; i < toIndex; i++) {
			byte value = b.get(i);
			if (prev > value)
				return false;
			prev = value;
		}

		return true;
	}

	/**
	 * Determines if the specified buffer is sorted and distinct inside the
	 * specified range. That is: {@code buffer[i] < buffer[i + 1]} for all elements
	 * in the range. A range of length zero or one is considered sorted and
	 * distinct.
	 * 
	 * @param b         - the buffer to be checked
	 * @param fromIndex - the index of the first element (inclusive) to be checked
	 * @param toIndex   - the index of the last element (exclusive) to be checked
	 * 
	 * @return true if the buffer is sorted and distinct inside the specified range
	 * 
	 * @throws IllegalArgumentException  if {@code fromIndex > toIndex}
	 * @throws IndexOutOfBoundsException if
	 *                                   {@code fromIndex < 0 or toIndex > b.capacity()}
	 */
	public static boolean isSortedAndDistinct(ByteBuffer b, int fromIndex, int toIndex) {
		rangeCheck(b.capacity(), fromIndex, toIndex);

		if (toIndex - fromIndex <= 1)
			return true;

		byte prev = b.get(fromIndex);
		for (int i = fromIndex + 1; i < toIndex; i++) {
			byte value = b.get(i);
			if (prev >= value)
				return false;
			prev = value;
		}

		return true;
	}

	/**
	 * Determines if the specified buffer is sorted inside the specified range. That
	 * is: {@code buffer[i] <= buffer[i + 1]} for all elements in the range. A range
	 * of length zero or one is considered sorted.
	 * 
	 * @param b         - the buffer to be checked
	 * @param fromIndex - the index of the first element (inclusive) to be checked
	 * @param toIndex   - the index of the last element (exclusive) to be checked
	 * 
	 * @return true if the buffer is sorted inside the specified range
	 * 
	 * @throws IllegalArgumentException  if {@code fromIndex > toIndex}
	 * @throws IndexOutOfBoundsException if
	 *                                   {@code fromIndex < 0 or toIndex > b.capacity()}
	 */
	public static boolean isSorted(ShortBuffer b, int fromIndex, int toIndex) {
		rangeCheck(b.capacity(), fromIndex, toIndex);

		if (toIndex - fromIndex <= 1)
			return true;

		short prev = b.get(fromIndex);
		for (int i = fromIndex + 1; i < toIndex; i++) {
			short value = b.get(i);
			if (prev > value)
				return false;
			prev = value;
		}

		return true;
	}

	/**
	 * Determines if the specified buffer is sorted and distinct inside the
	 * specified range. That is: {@code buffer[i] < buffer[i + 1]} for all elements
	 * in the range. A range of length zero or one is considered sorted and
	 * distinct.
	 * 
	 * @param b         - the buffer to be checked
	 * @param fromIndex - the index of the first element (inclusive) to be checked
	 * @param toIndex   - the index of the last element (exclusive) to be checked
	 * 
	 * @return true if the buffer is sorted and distinct inside the specified range
	 * 
	 * @throws IllegalArgumentException  if {@code fromIndex > toIndex}
	 * @throws IndexOutOfBoundsException if
	 *                                   {@code fromIndex < 0 or toIndex > b.capacity()}
	 */
	public static boolean isSortedAndDistinct(ShortBuffer b, int fromIndex, int toIndex) {
		rangeCheck(b.capacity(), fromIndex, toIndex);

		if (toIndex - fromIndex <= 1)
			return true;

		short prev = b.get(fromIndex);
		for (int i = fromIndex + 1; i < toIndex; i++) {
			short value = b.get(i);
			if (prev >= value)
				return false;
			prev = value;
		}

		return true;
	}

	/**
	 * Determines if the specified buffer is sorted inside the specified range. That
	 * is: {@code buffer[i] <= buffer[i + 1]} for all elements in the range. A range
	 * of length zero or one is considered sorted. The comparison of two values is
	 * consistent with {@link Float#compareTo(Float)}.
	 * 
	 * @param b         - the buffer to be checked
	 * @param fromIndex - the index of the first element (inclusive) to be checked
	 * @param toIndex   - the index of the last element (exclusive) to be checked
	 * 
	 * @return true if the buffer is sorted inside the specified range
	 * 
	 * @throws IllegalArgumentException  if {@code fromIndex > toIndex}
	 * @throws IndexOutOfBoundsException if
	 *                                   {@code fromIndex < 0 or toIndex > b.capacity()}
	 */
	public static boolean isSorted(FloatBuffer b, int fromIndex, int toIndex) {
		rangeCheck(b.capacity(), fromIndex, toIndex);

		if (toIndex - fromIndex <= 1)
			return true;

		float prev = b.get(fromIndex);
		for (int i = fromIndex + 1; i < toIndex; i++) {
			float value = b.get(i);
			if (Float.compare(prev, value) > 0)
				return false;
			prev = value;
		}

		return true;
	}

	/**
	 * Determines if the specified buffer is sorted and distinct inside the
	 * specified range. That is: {@code buffer[i] < buffer[i + 1]} for all elements
	 * in the range. A range of length zero or one is considered sorted and
	 * distinct. The comparison of two values is consistent with
	 * {@link Float#compareTo(Float)}.
	 * 
	 * @param b         - the buffer to be checked
	 * @param fromIndex - the index of the first element (inclusive) to be checked
	 * @param toIndex   - the index of the last element (exclusive) to be checked
	 * 
	 * @return true if the buffer is sorted and distinct inside the specified range
	 * 
	 * @throws IllegalArgumentException  if {@code fromIndex > toIndex}
	 * @throws IndexOutOfBoundsException if
	 *                                   {@code fromIndex < 0 or toIndex > b.capacity()}
	 */
	public static boolean isSortedAndDistinct(FloatBuffer b, int fromIndex, int toIndex) {
		rangeCheck(b.capacity(), fromIndex, toIndex);

		if (toIndex - fromIndex <= 1)
			return true;

		float prev = b.get(fromIndex);
		for (int i = fromIndex + 1; i < toIndex; i++) {
			float value = b.get(i);
			if (Float.compare(prev, value) >= 0)
				return false;
			prev = value;
		}

		return true;
	}

	/**
	 * Determines if the specified buffer is sorted inside the specified range. That
	 * is: {@code buffer[i] <= buffer[i + 1]} for all elements in the range. A range
	 * of length zero or one is considered sorted. The comparison of two values is
	 * consistent with {@link Double#compareTo(Double)}.
	 * 
	 * @param b         - the buffer to be checked
	 * @param fromIndex - the index of the first element (inclusive) to be checked
	 * @param toIndex   - the index of the last element (exclusive) to be checked
	 * 
	 * @return true if the buffer is sorted inside the specified range
	 * 
	 * @throws IllegalArgumentException  if {@code fromIndex > toIndex}
	 * @throws IndexOutOfBoundsException if
	 *                                   {@code fromIndex < 0 or toIndex > b.capacity()}
	 */
	public static boolean isSorted(DoubleBuffer b, int fromIndex, int toIndex) {
		rangeCheck(b.capacity(), fromIndex, toIndex);

		if (toIndex - fromIndex <= 1)
			return true;

		double prev = b.get(fromIndex);
		for (int i = fromIndex + 1; i < toIndex; i++) {
			double value = b.get(i);
			if (Double.compare(prev, value) > 0)
				return false;
			prev = value;
		}

		return true;
	}

	/**
	 * Determines if the specified buffer is sorted and distinct inside the
	 * specified range. That is: {@code buffer[i] < buffer[i + 1]} for all elements
	 * in the range. A range of length zero or one is considered sorted and
	 * distinct. The comparison of two values is consistent with
	 * {@link Double#compareTo(Double)}.
	 * 
	 * @param b         - the buffer to be checked
	 * @param fromIndex - the index of the first element (inclusive) to be checked
	 * @param toIndex   - the index of the last element (exclusive) to be checked
	 * 
	 * @return true if the buffer is sorted and distinct inside the specified range
	 * 
	 * @throws IllegalArgumentException  if {@code fromIndex > toIndex}
	 * @throws IndexOutOfBoundsException if
	 *                                   {@code fromIndex < 0 or toIndex > b.capacity()}
	 */
	public static boolean isSortedAndDistinct(DoubleBuffer b, int fromIndex, int toIndex) {
		rangeCheck(b.capacity(), fromIndex, toIndex);

		if (toIndex - fromIndex <= 1)
			return true;

		double prev = b.get(fromIndex);
		for (int i = fromIndex + 1; i < toIndex; i++) {
			double value = b.get(i);
			if (Double.compare(prev, value) >= 0)
				return false;
			prev = value;
		}

		return true;
	}

	/**
	 * Returns a copy of a range from the specified buffer. The new buffer will be
	 * direct iff the specified buffer is direct, and will have the same byte order.
	 * The capacity will be equal to the size of the specified range. The limit will
	 * be set to the capacity, and the position will be set to zero.
	 * 
	 * @param b         - the buffer to be checked
	 * @param fromIndex - the index of the first element (inclusive) to be checked
	 * @param toIndex   - the index of the last element (exclusive) to be checked
	 * 
	 * @return a copy of a range of data from the specified buffer
	 * 
	 * @throws IllegalArgumentException  if {@code fromIndex > toIndex}
	 * @throws IndexOutOfBoundsException if
	 *                                   {@code fromIndex < 0 or toIndex > b.capacity()}
	 */
	public static ByteBuffer copy(ByteBuffer b, int fromIndex, int toIndex) {

		ByteBuffer slice = slice(b, fromIndex, toIndex);

		ByteBuffer copy = b.isDirect() ? ByteBuffer.allocateDirect(slice.capacity())
				: ByteBuffer.allocate(slice.capacity());
		copy.order(b.order());

		copy.put(slice);
		copy.flip();

		return copy;
	}

	/**
	 * Returns a copy of a range from the specified buffer. The new buffer will be
	 * direct iff the specified buffer is direct, and will have the same byte order.
	 * The capacity will be equal to the size of the specified range. The limit will
	 * be set to the capacity, and the position will be set to zero.
	 * 
	 * @param b         - the buffer to be checked
	 * @param fromIndex - the index of the first element (inclusive) to be checked
	 * @param toIndex   - the index of the last element (exclusive) to be checked
	 * 
	 * @return a copy of a range of data from the specified buffer
	 * 
	 * @throws IllegalArgumentException  if {@code fromIndex > toIndex}
	 * @throws IndexOutOfBoundsException if
	 *                                   {@code fromIndex < 0 or toIndex > b.capacity()}
	 */
	public static IntBuffer copy(IntBuffer b, int fromIndex, int toIndex) {
		rangeCheck(b.capacity(), fromIndex, toIndex);

		IntBuffer dup = b.duplicate();
		dup.limit(toIndex);
		dup.position(fromIndex);

		ByteBuffer copy = b.isDirect() ? ByteBuffer.allocateDirect(dup.remaining() * 4)
				: ByteBuffer.allocate(dup.remaining() * 4);
		copy.order(b.order());

		IntBuffer view = copy.asIntBuffer();
		view.put(dup);
		view.flip();

		return view;
	}

	/**
	 * Returns a copy of a range from the specified buffer. The new buffer will be
	 * direct iff the specified buffer is direct, and will have the same byte order.
	 * The capacity will be equal to the size of the specified range. The limit will
	 * be set to the capacity, and the position will be set to zero.
	 * 
	 * @param b         - the buffer to be checked
	 * @param fromIndex - the index of the first element (inclusive) to be checked
	 * @param toIndex   - the index of the last element (exclusive) to be checked
	 * 
	 * @return a copy of a range of data from the specified buffer
	 * 
	 * @throws IllegalArgumentException  if {@code fromIndex > toIndex}
	 * @throws IndexOutOfBoundsException if
	 *                                   {@code fromIndex < 0 or toIndex > b.capacity()}
	 */
	public static LongBuffer copy(LongBuffer b, int fromIndex, int toIndex) {
		rangeCheck(b.capacity(), fromIndex, toIndex);

		LongBuffer dup = b.duplicate();
		dup.limit(toIndex);
		dup.position(fromIndex);

		ByteBuffer copy = b.isDirect() ? ByteBuffer.allocateDirect(dup.remaining() * 8)
				: ByteBuffer.allocate(dup.remaining() * 8);
		copy.order(b.order());

		LongBuffer view = copy.asLongBuffer();
		view.put(dup);
		view.flip();

		return view;
	}

	/**
	 * Returns a copy of a range from the specified buffer. The new buffer will be
	 * direct iff the specified buffer is direct, and will have the same byte order.
	 * The capacity will be equal to the size of the specified range. The limit will
	 * be set to the capacity, and the position will be set to zero.
	 * 
	 * @param b         - the buffer to be checked
	 * @param fromIndex - the index of the first element (inclusive) to be checked
	 * @param toIndex   - the index of the last element (exclusive) to be checked
	 * 
	 * @return a copy of a range of data from the specified buffer
	 * 
	 * @throws IllegalArgumentException  if {@code fromIndex > toIndex}
	 * @throws IndexOutOfBoundsException if
	 *                                   {@code fromIndex < 0 or toIndex > b.capacity()}
	 */
	public static ShortBuffer copy(ShortBuffer b, int fromIndex, int toIndex) {
		rangeCheck(b.capacity(), fromIndex, toIndex);

		ShortBuffer dup = b.duplicate();
		dup.limit(toIndex);
		dup.position(fromIndex);

		ByteBuffer copy = b.isDirect() ? ByteBuffer.allocateDirect(dup.remaining() * 2)
				: ByteBuffer.allocate(dup.remaining() * 2);
		copy.order(b.order());

		ShortBuffer view = copy.asShortBuffer();
		view.put(dup);
		view.flip();

		return view;
	}

	/**
	 * Returns a copy of a range from the specified buffer. The new buffer will be
	 * direct iff the specified buffer is direct, and will have the same byte order.
	 * The capacity will be equal to the size of the specified range. The limit will
	 * be set to the capacity, and the position will be set to zero.
	 * 
	 * @param b         - the buffer to be checked
	 * @param fromIndex - the index of the first element (inclusive) to be checked
	 * @param toIndex   - the index of the last element (exclusive) to be checked
	 * 
	 * @return a copy of a range of data from the specified buffer
	 * 
	 * @throws IllegalArgumentException  if {@code fromIndex > toIndex}
	 * @throws IndexOutOfBoundsException if
	 *                                   {@code fromIndex < 0 or toIndex > b.capacity()}
	 */
	public static FloatBuffer copy(FloatBuffer b, int fromIndex, int toIndex) {
		rangeCheck(b.capacity(), fromIndex, toIndex);

		FloatBuffer dup = b.duplicate();
		dup.limit(toIndex);
		dup.position(fromIndex);

		ByteBuffer copy = b.isDirect() ? ByteBuffer.allocateDirect(dup.remaining() * 4)
				: ByteBuffer.allocate(dup.remaining() * 4);
		copy.order(b.order());

		FloatBuffer view = copy.asFloatBuffer();
		view.put(dup);
		view.flip();

		return view;
	}

	/**
	 * Returns a copy of a range from the specified buffer. The new buffer will be
	 * direct iff the specified buffer is direct, and will have the same byte order.
	 * The capacity will be equal to the size of the specified range. The limit will
	 * be set to the capacity, and the position will be set to zero.
	 * 
	 * @param b         - the buffer to be checked
	 * @param fromIndex - the index of the first element (inclusive) to be checked
	 * @param toIndex   - the index of the last element (exclusive) to be checked
	 * 
	 * @return a copy of a range of data from the specified buffer
	 * 
	 * @throws IllegalArgumentException  if {@code fromIndex > toIndex}
	 * @throws IndexOutOfBoundsException if
	 *                                   {@code fromIndex < 0 or toIndex > b.capacity()}
	 */
	public static DoubleBuffer copy(DoubleBuffer b, int fromIndex, int toIndex) {
		rangeCheck(b.capacity(), fromIndex, toIndex);

		DoubleBuffer dup = b.duplicate();
		dup.limit(toIndex);
		dup.position(fromIndex);

		ByteBuffer copy = b.isDirect() ? ByteBuffer.allocateDirect(dup.remaining() * 8)
				: ByteBuffer.allocate(dup.remaining() * 8);
		copy.order(b.order());

		DoubleBuffer view = copy.asDoubleBuffer();
		view.put(dup);
		view.flip();

		return view;
	}

	/**
	 * Deduplicates a range of the specified {@link IntBuffer}. The range must be
	 * sorted in ascending order prior to making this call. If it is not sorted, the
	 * results are undefined.
	 * <p>
	 * This method is useful as a post-processing step after a sort on a buffer
	 * which contains duplicate elements.
	 *
	 * @param b         the buffer to be deduplicated
	 * @param fromIndex - the index of the first element (inclusive) to be
	 *                  deduplicated
	 * @param toIndex   - the index of the last element (exclusive) to be
	 *                  deduplicated
	 * 
	 * @return the (exclusive) highest index in use after deduplicating
	 * 
	 * @throws IllegalArgumentException  if {@code fromIndex > toIndex}
	 * @throws IndexOutOfBoundsException if
	 *                                   {@code fromIndex < 0 or toIndex > b.capacity()}
	 */
	public static int deduplicate(IntBuffer b, int fromIndex, int toIndex) {
		rangeCheck(b.capacity(), fromIndex, toIndex);

		if (toIndex - fromIndex < 2)
			return toIndex;

		int prev = b.get(fromIndex);
		int highest = fromIndex + 1;

		for (int i = fromIndex + 1; i < toIndex; i++) {
			int value = b.get(i);

			if (value != prev) {
				if (highest < i)
					b.put(highest, value);

				highest++;
				prev = value;
			}
		}

		return highest;
	}

	/**
	 * Deduplicates a range of the specified {@link LongBuffer}. The range must be
	 * sorted in ascending order prior to making this call. If it is not sorted, the
	 * results are undefined.
	 * <p>
	 * This method is useful as a post-processing step after a sort on a buffer
	 * which contains duplicate elements.
	 *
	 * @param b         the buffer to be deduplicated
	 * @param fromIndex - the index of the first element (inclusive) to be
	 *                  deduplicated
	 * @param toIndex   - the index of the last element (exclusive) to be
	 *                  deduplicated
	 * 
	 * @return the (exclusive) highest index in use after deduplicating
	 * 
	 * @throws IllegalArgumentException  if {@code fromIndex > toIndex}
	 * @throws IndexOutOfBoundsException if
	 *                                   {@code fromIndex < 0 or toIndex > b.capacity()}
	 */
	public static int deduplicate(LongBuffer b, int fromIndex, int toIndex) {
		rangeCheck(b.capacity(), fromIndex, toIndex);

		if (toIndex - fromIndex < 2)
			return toIndex;

		long prev = b.get(fromIndex);
		int highest = fromIndex + 1;

		for (int i = fromIndex + 1; i < toIndex; i++) {
			long value = b.get(i);

			if (value != prev) {
				if (highest < i)
					b.put(highest, value);

				highest++;
				prev = value;
			}
		}

		return highest;
	}

	/**
	 * Deduplicates a range of the specified {@link ShortBuffer}. The range must be
	 * sorted in ascending order prior to making this call. If it is not sorted, the
	 * results are undefined.
	 * <p>
	 * This method is useful as a post-processing step after a sort on a buffer
	 * which contains duplicate elements.
	 *
	 * @param b         the buffer to be deduplicated
	 * @param fromIndex - the index of the first element (inclusive) to be
	 *                  deduplicated
	 * @param toIndex   - the index of the last element (exclusive) to be
	 *                  deduplicated
	 * 
	 * @return the (exclusive) highest index in use after deduplicating
	 * 
	 * @throws IllegalArgumentException  if {@code fromIndex > toIndex}
	 * @throws IndexOutOfBoundsException if
	 *                                   {@code fromIndex < 0 or toIndex > b.capacity()}
	 */
	public static int deduplicate(ShortBuffer b, int fromIndex, int toIndex) {
		rangeCheck(b.capacity(), fromIndex, toIndex);

		if (toIndex - fromIndex < 2)
			return toIndex;

		short prev = b.get(fromIndex);
		int highest = fromIndex + 1;

		for (int i = fromIndex + 1; i < toIndex; i++) {
			short value = b.get(i);

			if (value != prev) {
				if (highest < i)
					b.put(highest, value);

				highest++;
				prev = value;
			}
		}

		return highest;
	}

	/**
	 * Deduplicates a range of the specified {@link ByteBuffer}. The range must be
	 * sorted in ascending order prior to making this call. If it is not sorted, the
	 * results are undefined.
	 * <p>
	 * This method is useful as a post-processing step after a sort on a buffer
	 * which contains duplicate elements.
	 *
	 * @param b         the buffer to be deduplicated
	 * @param fromIndex - the index of the first element (inclusive) to be
	 *                  deduplicated
	 * @param toIndex   - the index of the last element (exclusive) to be
	 *                  deduplicated
	 * 
	 * @return the (exclusive) highest index in use after deduplicating
	 * 
	 * @throws IllegalArgumentException  if {@code fromIndex > toIndex}
	 * @throws IndexOutOfBoundsException if
	 *                                   {@code fromIndex < 0 or toIndex > b.capacity()}
	 */
	public static int deduplicate(ByteBuffer b, int fromIndex, int toIndex) {
		rangeCheck(b.capacity(), fromIndex, toIndex);

		if (toIndex - fromIndex < 2)
			return toIndex;

		byte prev = b.get(fromIndex);
		int highest = fromIndex + 1;

		for (int i = fromIndex + 1; i < toIndex; i++) {
			byte value = b.get(i);

			if (value != prev) {
				if (highest < i)
					b.put(highest, value);

				highest++;
				prev = value;
			}
		}

		return highest;
	}

	/**
	 * Deduplicates a range of the specified {@link FloatBuffer}. The range must be
	 * sorted in ascending order prior to making this call. If it is not sorted, the
	 * results are undefined. This method considers all NaN values to be equivalent
	 * and equal.
	 * <p>
	 * This method is useful as a post-processing step after a sort on a buffer
	 * which contains duplicate elements.
	 *
	 * @param b         the buffer to be deduplicated
	 * @param fromIndex - the index of the first element (inclusive) to be
	 *                  deduplicated
	 * @param toIndex   - the index of the last element (exclusive) to be
	 *                  deduplicated
	 * 
	 * @return the (exclusive) highest index in use after deduplicating
	 * 
	 * @throws IllegalArgumentException  if {@code fromIndex > toIndex}
	 * @throws IndexOutOfBoundsException if
	 *                                   {@code fromIndex < 0 or toIndex > b.capacity()}
	 */
	public static int deduplicate(FloatBuffer b, int fromIndex, int toIndex) {
		rangeCheck(b.capacity(), fromIndex, toIndex);

		if (toIndex - fromIndex < 2)
			return toIndex;

		float prev = b.get(fromIndex);
		int highest = fromIndex + 1;

		for (int i = fromIndex + 1; i < toIndex; i++) {
			float value = b.get(i);

			if (Float.compare(value, prev) != 0) {
				if (highest < i)
					b.put(highest, value);

				highest++;
				prev = value;
			}
		}

		return highest;
	}

	/**
	 * Deduplicates a range of the specified {@link DoubleBuffer}. The range must be
	 * sorted in ascending order prior to making this call. If it is not sorted, the
	 * results are undefined. This method considers all NaN values to be equivalent
	 * and equal.
	 * <p>
	 * This method is useful as a post-processing step after a sort on a buffer
	 * which contains duplicate elements.
	 *
	 * @param b         the buffer to be deduplicated
	 * @param fromIndex - the index of the first element (inclusive) to be
	 *                  deduplicated
	 * @param toIndex   - the index of the last element (exclusive) to be
	 *                  deduplicated
	 * 
	 * @return the (exclusive) highest index in use after deduplicating
	 * 
	 * @throws IllegalArgumentException  if {@code fromIndex > toIndex}
	 * @throws IndexOutOfBoundsException if
	 *                                   {@code fromIndex < 0 or toIndex > b.capacity()}
	 */
	public static int deduplicate(DoubleBuffer b, int fromIndex, int toIndex) {
		rangeCheck(b.capacity(), fromIndex, toIndex);

		if (toIndex - fromIndex < 2)
			return toIndex;

		double prev = b.get(fromIndex);
		int highest = fromIndex + 1;

		for (int i = fromIndex + 1; i < toIndex; i++) {
			double value = b.get(i);

			if (Double.compare(value, prev) != 0) {
				if (highest < i)
					b.put(highest, value);

				highest++;
				prev = value;
			}
		}

		return highest;
	}

	/**
	 * Returns a sequential {@link IntStream} with the specified buffer as its
	 * source.
	 * <p>
	 * <b>Note:</b> ignores {@link IntBuffer#position() position} and
	 * {@link IntBuffer#limit() limit}, can pass a {@link IntBuffer#slice() slice}
	 * instead.
	 *
	 * @param buffer the buffer, assumed to be unmodified during use
	 * @return an {@code IntStream} for the buffer
	 */
	public static IntStream stream(IntBuffer buffer) {
		return stream(buffer, 0, buffer.capacity(), 0);
	}

	/**
	 * Returns a sequential {@link IntStream} with the specified range of the
	 * specified buffer as its source.
	 *
	 * @param buffer          the buffer, assumed to be unmodified during use
	 * @param startInclusive  the first index to cover, inclusive
	 * @param endExclusive    index immediately past the last index to cover
	 * @param characteristics characteristics of this spliterator's source or
	 *                        elements beyond {@code SIZED}, {@code SUBSIZED},
	 *                        {@code ORDERED}, {@code NONNULL}, and
	 *                        {@code IMMUTABLE}, which are are always reported
	 * @return an {@code IntStream} for the buffer range
	 * @throws ArrayIndexOutOfBoundsException if {@code startInclusive} is negative,
	 *                                        {@code endExclusive} is less than
	 *                                        {@code startInclusive}, or
	 *                                        {@code endExclusive} is greater than
	 *                                        the buffer's
	 *                                        {@link IntBuffer#capacity() capacity}
	 */
	public static IntStream stream(IntBuffer buffer, int startInclusive, int endExclusive, int characteristics) {
		return StreamSupport.intStream(new BufferSpliterators.IntBufferSpliterator(buffer, startInclusive, endExclusive,
				characteristics | ORDERED | NONNULL | IMMUTABLE), false);
	}

	/**
	 * Returns a sequential {@link LongStream} with the specified buffer as its
	 * source.
	 * <p>
	 * <b>Note:</b> ignores {@link LongBuffer#position() position} and
	 * {@link LongBuffer#limit() limit}, can pass a {@link LongBuffer#slice() slice}
	 * instead.
	 *
	 * @param buffer the buffer, assumed to be unmodified during use
	 * @return an {@code LongStream} for the buffer
	 */
	public static LongStream stream(LongBuffer buffer) {
		return stream(buffer, 0, buffer.capacity(), 0);
	}

	/**
	 * Returns a sequential {@link LongStream} with the specified range of the
	 * specified buffer as its source.
	 *
	 * @param buffer          the buffer, assumed to be unmodified during use
	 * @param startInclusive  the first index to cover, inclusive
	 * @param endExclusive    index immediately past the last index to cover
	 * @param characteristics characteristics of this spliterator's source or
	 *                        elements beyond {@code SIZED}, {@code SUBSIZED},
	 *                        {@code ORDERED}, {@code NONNULL}, and
	 *                        {@code IMMUTABLE}, which are are always reported
	 * @return an {@code LongStream} for the buffer range
	 * @throws ArrayIndexOutOfBoundsException if {@code startInclusive} is negative,
	 *                                        {@code endExclusive} is less than
	 *                                        {@code startInclusive}, or
	 *                                        {@code endExclusive} is greater than
	 *                                        the buffer's
	 *                                        {@link LongBuffer#capacity() capacity}
	 */
	public static LongStream stream(LongBuffer buffer, int startInclusive, int endExclusive, int characteristics) {
		return StreamSupport.longStream(new BufferSpliterators.LongBufferSpliterator(buffer, startInclusive,
				endExclusive, characteristics | ORDERED | NONNULL | IMMUTABLE), false);
	}

	/**
	 * Returns a sequential {@link DoubleStream} with the specified buffer as its
	 * source.
	 * <p>
	 * <b>Note:</b> ignores {@link DoubleBuffer#position() position} and
	 * {@link DoubleBuffer#limit() limit}, can pass a {@link DoubleBuffer#slice()
	 * slice} instead.
	 *
	 * @param buffer the buffer, assumed to be unmodified during use
	 * @return an {@code DoubleStream} for the buffer
	 */
	public static DoubleStream stream(DoubleBuffer buffer) {
		return stream(buffer, 0, buffer.capacity(), 0);
	}

	/**
	 * Returns a sequential {@link DoubleStream} with the specified range of the
	 * specified buffer as its source.
	 *
	 * @param buffer          the buffer, assumed to be unmodified during use
	 * @param startInclusive  the first index to cover, inclusive
	 * @param endExclusive    index immediately past the last index to cover
	 * @param characteristics characteristics of this spliterator's source or
	 *                        elements beyond {@code SIZED}, {@code SUBSIZED},
	 *                        {@code ORDERED}, {@code NONNULL}, and
	 *                        {@code IMMUTABLE}, which are are always reported
	 * @return an {@code DoubleStream} for the buffer range
	 * @throws ArrayIndexOutOfBoundsException if {@code startInclusive} is negative,
	 *                                        {@code endExclusive} is less than
	 *                                        {@code startInclusive}, or
	 *                                        {@code endExclusive} is greater than
	 *                                        the buffer's
	 *                                        {@link DoubleBuffer#capacity()
	 *                                        capacity}
	 */
	public static DoubleStream stream(DoubleBuffer buffer, int startInclusive, int endExclusive, int characteristics) {
		return StreamSupport.doubleStream(new BufferSpliterators.DoubleBufferSpliterator(buffer, startInclusive,
				endExclusive, characteristics | ORDERED | NONNULL | IMMUTABLE), false);
	}

	/**
	 * Checks that {@code fromIndex} and {@code toIndex} are in the range and throws
	 * an exception if they aren't.
	 * 
	 * @param bufferCapacity - capacity of the buffer being checked
	 * @param fromIndex      - the index of the first element (inclusive) to be
	 *                       checked
	 * @param toIndex        - the index of the last element (exclusive) to be
	 *                       checked
	 */
	static void rangeCheck(int bufferCapacity, int fromIndex, int toIndex) {
		if (fromIndex > toIndex) {
			throw new IllegalArgumentException("fromIndex(" + fromIndex + ") > toIndex(" + toIndex + ")");
		}
		if (fromIndex < 0) {
			throw new IndexOutOfBoundsException("fromIndex(" + fromIndex + ") < 0");
		}
		if (toIndex > bufferCapacity) {
			throw new IndexOutOfBoundsException("toIndex(" + toIndex + ") > " + bufferCapacity);
		}
	}

	/**
	 * Checks that {@code fromIndex} and {@code toIndex} are in the range and throws
	 * an exception if they aren't.
	 * 
	 * @param bufferCapacity - capacity of the buffer being checked
	 * @param fromIndex      - the index of the first element (inclusive) to be
	 *                       checked
	 * @param toIndex        - the index of the last element (inclusive) to be
	 *                       checked
	 */
	static void rangeCheckInclusive(int bufferCapacity, int fromIndex, int toIndex) {
		if (fromIndex > toIndex) {
			throw new IllegalArgumentException("fromIndex(" + fromIndex + ") > toIndex(" + toIndex + ")");
		}
		if (fromIndex < 0) {
			throw new IndexOutOfBoundsException("fromIndex(" + fromIndex + ") < 0");
		}
		if (toIndex >= bufferCapacity) {
			throw new IndexOutOfBoundsException("toIndex(" + toIndex + ") >= " + bufferCapacity);
		}
	}

	/**
	 * Writes all {@link ByteBuffer#remaining() remaining} bytes from the given
	 * {@link ByteBuffer buffer} into the given {@link WritableByteChannel channel}.
	 * 
	 * @param channel - the channel being written to
	 * @param buffer  - the buffer being read from
	 * @throws IOException
	 */
	public static void writeFully(WritableByteChannel channel, ByteBuffer buffer) throws IOException {
		while (buffer.remaining() > 0)
			channel.write(buffer);
	}

	/**
	 * Reads bytes from the given {@link ReadableByteChannel channel} until the
	 * given buffer is full ({@link ByteBuffer#remaining() remaining} is {@code 0}).
	 * 
	 * @param channel - the channel being read from
	 * @param buffer  - the buffer being written to
	 * @throws IOException
	 */
	public static void readFully(ReadableByteChannel channel, ByteBuffer buffer) throws IOException {
		while (buffer.remaining() > 0)
			channel.read(buffer);
	}
}
