package tech.bitey.bufferstuff.codegen;

import java.io.BufferedWriter;

public class GenBufferUtils implements GenBufferCode {

	@Override
	public void run() throws Exception {
		try (BufferedWriter out = open("BufferUtils.java")) {

			section(out, PREFIX);

			section(out, isSorted("int", "IntBuffer", "prev > value"));
			section(out, isDistinct("int", "IntBuffer", "prev >= value"));
			section(out, isSorted("long", "LongBuffer", "prev > value"));
			section(out, isDistinct("long", "LongBuffer", "prev >= value"));
			section(out, isSorted("byte", "ByteBuffer", "prev > value"));
			section(out, isDistinct("byte", "ByteBuffer", "prev >= value"));
			section(out, isSorted("short", "ShortBuffer", "prev > value"));
			section(out, isDistinct("short", "ShortBuffer", "prev >= value"));
			section(out, isSorted("float", "FloatBuffer", "Float.compare(prev, value) > 0"));
			section(out, isDistinct("float", "FloatBuffer", "Float.compare(prev, value) >= 0"));
			section(out, isSorted("double", "DoubleBuffer", "Double.compare(prev, value) > 0"));
			section(out, isDistinct("double", "DoubleBuffer", "Double.compare(prev, value) >= 0"));

			section(out, COPY_BYTE_BUFFER);
			section(out, copy("IntBuffer", 4));
			section(out, copy("LongBuffer", 8));
			section(out, copy("ShortBuffer", 2));
			section(out, copy("FloatBuffer", 4));
			section(out, copy("DoubleBuffer", 8));

			section(out, deduplicate("int", "IntBuffer", "value != prev"));
			section(out, deduplicate("long", "LongBuffer", "value != prev"));
			section(out, deduplicate("short", "ShortBuffer", "value != prev"));
			section(out, deduplicate("byte", "ByteBuffer", "value != prev"));
			section(out, deduplicate("float", "FloatBuffer", "Float.compare(value, prev) != 0"));
			section(out, deduplicate("double", "DoubleBuffer", "Double.compare(value, prev) != 0"));

			section(out, stream("int", "IntBuffer", "IntStream"));
			section(out, stream("long", "LongBuffer", "LongStream"));
			section(out, stream("double", "DoubleBuffer", "DoubleStream"));

			section(out, SUFFIX);
		}
	}

	private static String isSorted(String valType, String bufferType, String compare) {
		return IS_SORTED.replace(VAL_TYPE, valType).replace(BUFFER_TYPE, bufferType).replace(COMPARE, compare);
	}

	private static String isDistinct(String valType, String bufferType, String compare) {
		return IS_DISTINCT.replace(VAL_TYPE, valType).replace(BUFFER_TYPE, bufferType).replace(COMPARE, compare);
	}

	private static String copy(String bufferType, int valSize) {
		return COPY_BUFFER.replace(BUFFER_TYPE, bufferType).replace(VAL_SIZE, "" + valSize);
	}

	private static String deduplicate(String valType, String bufferType, String compare) {
		return DEDUPLICATE.replace(VAL_TYPE, valType).replace(BUFFER_TYPE, bufferType).replace(COMPARE, compare);
	}

	private static String stream(String valType, String bufferType, String streamType) {
		return STREAM.replace(VAL_TYPE, valType).replace(BUFFER_TYPE, bufferType).replace(STREAM_TYPE, streamType);
	}

//	private static final String SHORT_BOX_TYPE = "SHORT_BOX_TYPE";
//	private static final String LONG_BOX_TYPE = "LONG_BOX_TYPE";
	private static final String BUFFER_TYPE = "BUFFER_TYPE";
	private static final String VAL_TYPE = "VAL_TYPE";
	private static final String VAL_SIZE = "VAL_SIZE";
	private static final String COMPARE = "COMPARE";
	private static final String STREAM_TYPE = "STREAM_TYPE";

	private static final String STREAM = """
				/**
				 * Returns a sequential {@link STREAM_TYPE} with the specified buffer as its
				 * source.
				 * <p>
				 * <b>Note:</b> ignores {@link BUFFER_TYPE#position() position} and
				 * {@link BUFFER_TYPE#limit() limit}, can pass a {@link BUFFER_TYPE#slice() slice}
				 * instead.
				 *
				 * @param buffer the buffer, assumed to be unmodified during use
				 * @return an {@code STREAM_TYPE} for the buffer
				 */
				public static STREAM_TYPE stream(BUFFER_TYPE buffer) {
					return stream(buffer, 0, buffer.capacity(), 0);
				}

				/**
				 * Returns a sequential {@link STREAM_TYPE} with the specified range of the
				 * specified buffer as its source.
				 *
				 * @param buffer          the buffer, assumed to be unmodified during use
				 * @param startInclusive  the first index to cover, inclusive
				 * @param endExclusive    index immediately past the last index to cover
				 * @param characteristics characteristics of this spliterator's source or
				 *                        elements beyond {@code SIZED}, {@code SUBSIZED},
				 *                        {@code ORDERED}, {@code NONNULL}, and
				 *                        {@code IMMUTABLE}, which are are always reported
				 * @return an {@code STREAM_TYPE} for the buffer range
				 * @throws ArrayIndexOutOfBoundsException if {@code startInclusive} is negative,
				 *                                        {@code endExclusive} is less than
				 *                                        {@code startInclusive}, or
				 *                                        {@code endExclusive} is greater than
				 *                                        the buffer's
				 *                                        {@link BUFFER_TYPE#capacity() capacity}
				 */
				public static STREAM_TYPE stream(BUFFER_TYPE buffer, int startInclusive, int endExclusive, int characteristics) {
					return StreamSupport.VAL_TYPEStream(new BufferSpliterators.BUFFER_TYPESpliterator(buffer, startInclusive, endExclusive,
							characteristics | ORDERED | NONNULL | IMMUTABLE), false);
				}
			""";

	private static final String DEDUPLICATE = """
				/**
				 * Deduplicates a range of the specified {@link BUFFER_TYPE}. The range must be
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
				public static int deduplicate(BUFFER_TYPE b, int fromIndex, int toIndex) {
					rangeCheck(b.capacity(), fromIndex, toIndex);

					if (toIndex - fromIndex < 2)
						return toIndex;

					VAL_TYPE prev = b.get(fromIndex);
					int highest = fromIndex + 1;

					for (int i = fromIndex + 1; i < toIndex; i++) {
						VAL_TYPE value = b.get(i);

						if (COMPARE) {
							if (highest < i)
								b.put(highest, value);

							highest++;
							prev = value;
						}
					}

					return highest;
				}
			""";

	private static final String COPY_BUFFER = """
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
				public static BUFFER_TYPE copy(BUFFER_TYPE b, int fromIndex, int toIndex) {
					rangeCheck(b.capacity(), fromIndex, toIndex);

					BUFFER_TYPE dup = b.duplicate();
					dup.limit(toIndex);
					dup.position(fromIndex);

					ByteBuffer copy = b.isDirect() ? ByteBuffer.allocateDirect(dup.remaining() * VAL_SIZE)
							: ByteBuffer.allocate(dup.remaining() * VAL_SIZE);
					copy.order(b.order());

					BUFFER_TYPE view = copy.asBUFFER_TYPE();
					view.put(dup);
					view.flip();

					return view;
				}
			""";

	private static final String COPY_BYTE_BUFFER = """
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
			""";

	private static final String IS_SORTED = """
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
				public static boolean isSorted(BUFFER_TYPE b, int fromIndex, int toIndex) {
					rangeCheck(b.capacity(), fromIndex, toIndex);

					if (toIndex - fromIndex <= 1)
						return true;

					VAL_TYPE prev = b.get(fromIndex);
					for (int i = fromIndex + 1; i < toIndex; i++) {
						VAL_TYPE value = b.get(i);
						if (COMPARE)
							return false;
						prev = value;
					}

					return true;
				}
			""";

	private static final String IS_DISTINCT = """
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
				public static boolean isSortedAndDistinct(BUFFER_TYPE b, int fromIndex, int toIndex) {
					rangeCheck(b.capacity(), fromIndex, toIndex);

					if (toIndex - fromIndex <= 1)
						return true;

					VAL_TYPE prev = b.get(fromIndex);
					for (int i = fromIndex + 1; i < toIndex; i++) {
						VAL_TYPE value = b.get(i);
						if (COMPARE)
							return false;
						prev = value;
					}

					return true;
				}
			""";

	private static final String PREFIX = """
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
				""";

	private static final String SUFFIX = """
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
			""";
}
