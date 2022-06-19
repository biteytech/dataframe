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

public class GenBigByteBuffer implements GenBufferCode {

	@Override
	public void run() throws Exception {
		try (BufferedWriter out = open("BigByteBuffer.java")) {

			section(out, PREFIX);

			section(out, type("short", "Short", "two"));
			section(out, type("int", "Int", "four"));
			section(out, type("long", "Long", "eight"));
			section(out, type("float", "Float", "four"));
			section(out, type("double", "Double", "eight"));

			out.write("}\n");
		}
	}

	private static String type(String valType, String funcName, String numBytes) {
		return TYPE.replace(VAL_TYPE, valType).replace(FUNC_NAME, funcName).replace(NUM_BYTES, numBytes);
	}

	private static final String VAL_TYPE = "VAL_TYPE";
	private static final String FUNC_NAME = "FUNC_NAME";
	private static final String NUM_BYTES = "NUM_BYTES";

	private static final String TYPE = """
				/**
				 * Relative <i>put</i> method for writing a VAL_TYPE.
				 * <p>
				 * Writes NUM_BYTES bytes containing the given VAL_TYPE value, in the current byte order,
				 * into this buffer at the current position, and then increments the position by
				 * NUM_BYTES.
				 *
				 * @param value The VAL_TYPE value to be written
				 *
				 * @return This buffer
				 *
				 * @throws BufferOverflowException If there are fewer than NUM_BYTES bytes remaining
				 *                                 in this buffer
				 */
				BigByteBuffer putFUNC_NAME(VAL_TYPE value);

				/**
				 * Absolute <i>put</i> method for writing a VAL_TYPE.
				 * <p>
				 * Writes NUM_BYTES bytes containing the given VAL_TYPE value, in the current byte order,
				 * into this buffer at the given index.
				 *
				 * @param index The index at which the bytes will be written
				 *
				 * @param value The VAL_TYPE value to be written
				 *
				 * @return This buffer
				 *
				 * @throws IndexOutOfBoundsException If {@code index} is negative or not smaller
				 *                                   than the buffer's limit, minus one
				 */
				BigByteBuffer putFUNC_NAME(long index, VAL_TYPE value);

				/**
				 * Relative bulk <i>put</i> method
				 * <p>
				 * This method transfers the entire content of the given source VAL_TYPE array into
				 * this buffer.
				 *
				 * @param src The source array
				 *
				 * @return This buffer
				 *
				 * @throws BufferOverflowException If there is insufficient space in this buffer
				 */
				BigByteBuffer putFUNC_NAME(VAL_TYPE[] src);

				/**
				 * Relative bulk <i>put</i> method
				 * <p>
				 * This method transfers the VAL_TYPEs remaining in the given source buffer into
				 * this buffer. If there are more VAL_TYPEs remaining in the source buffer than in
				 * this buffer, that is, if
				 * {@code src.remaining()}&nbsp;{@code >}&nbsp;{@code remaining()}, then no
				 * shorts are transferred and a {@link BufferOverflowException} is thrown.
				 * <p>
				 * Otherwise, this method copies <i>n</i>&nbsp;=&nbsp;{@code src.remaining()}
				 * VAL_TYPEs from the given buffer into this buffer, starting at each buffer's
				 * current position. The positions of both buffers are then incremented by
				 * <i>n</i>.
				 *
				 * <p>
				 * In other words, an invocation of this method of the form {@code dst.put(src)}
				 * has exactly the same effect as the loop
				 *
				 * <pre>
				 * while (src.hasRemaining())
				 * 	dst.put(src.get());
				 * </pre>
				 *
				 * except that it first checks that there is sufficient space in this buffer and
				 * it is potentially much more efficient. If this buffer and the source buffer
				 * share the same backing array or memory, then the result will be as if the
				 * source elements were first copied to an intermediate location before being
				 * written into this buffer.
				 *
				 * @param src The source buffer from which VAL_TYPEs are to be read; must not be
				 *            this buffer
				 *
				 * @return This buffer
				 *
				 * @throws BufferOverflowException If there is insufficient space in this buffer
				 *                                 for the remaining VAL_TYPEs in the source buffer
				 */
				BigByteBuffer putFUNC_NAME(FUNC_NAMEBuffer src);

				/**
				 * Relative <i>get</i> method for reading a VAL_TYPE value.
				 *
				 * <p>
				 * Reads the next NUM_BYTES bytes at this buffer's current position, composing them
				 * into a VAL_TYPE value according to the current byte order, and then increments
				 * the position by NUM_BYTES.
				 * </p>
				 *
				 * @return The VAL_TYPE value at the buffer's current position
				 *
				 * @throws BufferUnderflowException If there are fewer than NUM_BYTES bytes remaining
				 *                                  in this buffer
				 */
				VAL_TYPE getFUNC_NAME();

				/**
				 * Absolute <i>get</i> method for reading a VAL_TYPE value.
				 * <p>
				 * Reads NUM_BYTES bytes at the given index, composing them into a VAL_TYPE value
				 * according to the current byte order.
				 *
				 * @param index The index from which the bytes will be read
				 *
				 * @return The VAL_TYPE value at the given index
				 *
				 * @throws IndexOutOfBoundsException If {@code index} is negative or not smaller
				 *                                   than the buffer's limit, minus one
				 */
				VAL_TYPE getFUNC_NAME(long index);

				/**
				 * Creates a view of this byte buffer as a VAL_TYPE buffer.
				 * <p>
				 * The content of the new buffer will start at this buffer's current position.
				 * Changes to this buffer's content will be visible in the new buffer, and vice
				 * versa; the two buffers' position and limit values will be independent.
				 * <p>
				 * The new buffer's position will be zero, its capacity and its limit will be
				 * the number of bytes remaining in this buffer divided by NUM_BYTES, and its byte
				 * order will be that of the byte buffer at the moment the view is created.
				 *
				 * @return A new VAL_TYPE buffer
				 */
				default SmallFUNC_NAMEBuffer asFUNC_NAMEBuffer() {
					return new SmallFUNC_NAMEBuffer(duplicate());
				}
			""";

	private static final String PREFIX = """
			package tech.bitey.bufferstuff;

			import java.io.InputStream;
			import java.nio.BufferOverflowException;
			import java.nio.BufferUnderflowException;
			import java.nio.ByteBuffer;
			import java.nio.ByteOrder;
			import java.nio.DoubleBuffer;
			import java.nio.FloatBuffer;
			import java.nio.IntBuffer;
			import java.nio.LongBuffer;
			import java.nio.ShortBuffer;

			/**
			 * This class has an API similar to {@link ByteBuffer}, but is addressable with
			 * long indices. Implementations are backed by one or more {@code ByteBuffers}.
			 * <p>
			 * Differences from {@code ByteBuffer} include:
			 * <ul>
			 * <li>mark and reset are not supported
			 * <li>read-only is not supported
			 * <li>byte order is preserved in {@link #duplicate()} and {@link #slice()}.
			 * </ul>
			 *
			 * biteytech@protonmail.com, adapted from {@link ByteBuffer}
			 */
			public sealed interface BigByteBuffer permits AbstractBigByteBuffer {

				/**
				 * Returns the underlying {@link ByteBuffer buffers}.
				 *
				 * @return the underlying buffers.
				 */
				ByteBuffer[] buffers();

				/**
				 * Returns this buffer's position.
				 *
				 * @return The position of this buffer
				 */
				long position();

				/**
				 * Returns this buffer's limit.
				 *
				 * @return The limit of this buffer
				 */
				long limit();

				/**
				 * Returns this buffer's capacity.
				 *
				 * @return The capacity of this buffer
				 */
				long capacity();

				/**
				 * Sets this buffer's position.
				 *
				 * @param newPosition The new position value; must be non-negative and no larger
				 *                    than the current limit
				 *
				 * @return this buffer
				 *
				 * @throws IllegalArgumentException If the preconditions on {@code newPosition}
				 *                                  do not hold
				 */
				BigByteBuffer position(long newPosition);

				/**
				 * Sets this buffer's limit. If the position is larger than the new limit then
				 * it is set to the new limit.
				 *
				 * @param newLimit The new limit value; must be non-negative and no larger than
				 *                 this buffer's capacity
				 *
				 * @return this buffer
				 *
				 * @throws IllegalArgumentException If the preconditions on {@code newLimit} do
				 *                                  not hold
				 */
				BigByteBuffer limit(long newLimit);

				/**
				 * Returns the number of bytes between the current position and the limit.
				 *
				 * @return The number of bytes remaining in this buffer
				 */
				long remaining();

				/**
				 * Tells whether there are any bytes between the current position and the limit.
				 *
				 * @return {@code true} if, and only if, there is at least one byte remaining in
				 *         this buffer
				 */
				boolean hasRemaining();

				/**
				 * Retrieves this buffer's byte order.
				 *
				 * <p>
				 * The byte order is used when reading or writing multibyte values, and when
				 * creating buffers that are views of this byte buffer. The order of a
				 * newly-created byte buffer is always {@link ByteOrder#BIG_ENDIAN BIG_ENDIAN}.
				 * </p>
				 *
				 * @return This buffer's byte order
				 */
				ByteOrder order();

				/**
				 * Modifies this buffer's byte order.
				 *
				 * @param order The new byte order, either {@link ByteOrder#BIG_ENDIAN
				 *              BIG_ENDIAN} or {@link ByteOrder#LITTLE_ENDIAN LITTLE_ENDIAN}
				 *
				 * @return this buffer
				 */
				BigByteBuffer order(ByteOrder order);

				/**
				 * Creates a new byte buffer that shares this buffer's content.
				 * <p>
				 * The content of the new buffer will be that of this buffer. Changes to this
				 * buffer's content will be visible in the new buffer, and vice versa; the two
				 * buffers' position and limit values will be independent.
				 * <p>
				 * The new buffer's capacity, limit, position, and byte order values will be
				 * identical to those of this buffer. The new buffer will be direct if, and only
				 * if, this buffer is direct.
				 *
				 * @return The new byte buffer
				 */
				BigByteBuffer duplicate();

				/**
				 * Creates a new byte buffer whose content is a shared subsequence of this
				 * buffer's content.
				 * <p>
				 * The content of the new buffer will start at this buffer's current position.
				 * Changes to this buffer's content will be visible in the new buffer, and vice
				 * versa; the two buffers' position and limit values will be independent.
				 * <p>
				 * The new buffer's position will be zero, its capacity and its limit will be
				 * the number of bytes remaining in this buffer, and the byte order will be the
				 * same as this buffer. The new buffer will be direct if, and only if, this
				 * buffer is direct.
				 *
				 * @return The new byte buffer
				 */
				BigByteBuffer slice();

				/**
				 * Creates a new byte buffer whose content is a shared subsequence of this
				 * buffer's content.
				 *
				 * @return The new byte buffer
				 */
			    BigByteBuffer slice(long fromIndex, long toIndex);

				/**
				 * Creates a new {@link ByteBuffer} whose content is a shared subsequence of this
				 * buffer's content.
				 * <p>
				 * The content of the new buffer will start at this buffer's current position.
				 * Changes to this buffer's content will be visible in the new buffer, and vice
				 * versa; the two buffers' position and limit values will be independent.
				 * <p>
				 * The new buffer's position will be zero, its capacity and its limit will be
				 * the number of bytes remaining in this buffer, and the byte order will be the
				 * same as this buffer. The new buffer will be direct if, and only if, this
				 * buffer is direct.
				 *
				 * @return The new byte buffer
				 */
			    ByteBuffer smallSlice();

				/**
				 * Creates a new {@link ByteBuffer} whose content is a shared subsequence of this
				 * buffer's content.
				 *
				 * @return The new byte buffer
				 */
			    ByteBuffer smallSlice(long fromIndex, long toIndex);

				/**
				 * Returns a copy of a range from this buffer.
				 *
				 * @return a copy of a range from this buffer.
				 */
			    BigByteBuffer copy(long fromIndex, long toIndex);

				/**
				 * Clears this buffer. The position is set to zero and the limit is set to the
				 * capacity.
				 * <p>
				 * This method does not actually erase the data in the buffer, but it is named
				 * as if it did because it will most often be used in situations in which that
				 * might as well be the case.
				 *
				 * @return This buffer
				 */
				BigByteBuffer clear();

				/**
				 * Flips this buffer. The limit is set to the current position and then the
				 * position is set to zero.
				 *
				 * @return This buffer
				 */
				BigByteBuffer flip();

				/**
				 * Relative <i>put</i> method.
				 * <p>
				 * Writes the given byte into this buffer at the current position, and then
				 * increments the position.
				 *
				 * @param value The byte to be written
				 *
				 * @return This buffer
				 *
				 * @throws BufferOverflowException If this buffer's current position is not
				 *                                 smaller than its limit
				 */
				BigByteBuffer put(byte value);

				/**
				 * Absolute <i>put</i> method.
				 * <p>
				 * Writes the given byte into this buffer at the given index.
				 *
				 * @param index The index at which the byte will be written
				 *
				 * @param value The byte value to be written
				 *
				 * @return This buffer
				 *
				 * @throws IndexOutOfBoundsException If {@code index} is negative or not smaller
				 *                                   than the buffer's limit
				 */
				BigByteBuffer put(long index, byte value);

				/**
				 * Relative bulk <i>put</i> method
				 * <p>
				 * This method transfers the bytes remaining in the given source buffer into
				 * this buffer. If there are more bytes remaining in the source buffer than in
				 * this buffer, that is, if
				 * {@code src.remaining()}&nbsp;{@code >}&nbsp;{@code remaining()}, then no
				 * bytes are transferred and a {@link BufferOverflowException} is thrown.
				 * <p>
				 * Otherwise, this method copies <i>n</i>&nbsp;=&nbsp;{@code src.remaining()}
				 * bytes from the given buffer into this buffer, starting at each buffer's
				 * current position. The positions of both buffers are then incremented by
				 * <i>n</i>.
				 *
				 * <p>
				 * In other words, an invocation of this method of the form {@code dst.put(src)}
				 * has exactly the same effect as the loop
				 *
				 * <pre>
				 * while (src.hasRemaining())
				 * 	dst.put(src.get());
				 * </pre>
				 *
				 * except that it first checks that there is sufficient space in this buffer and
				 * it is potentially much more efficient. If this buffer and the source buffer
				 * share the same backing array or memory, then the result will be as if the
				 * source elements were first copied to an intermediate location before being
				 * written into this buffer.
				 *
				 * @param src The source buffer from which bytes are to be read; must not be
				 *            this buffer
				 *
				 * @return This buffer
				 *
				 * @throws BufferOverflowException  If there is insufficient space in this
				 *                                  buffer for the remaining bytes in the source
				 *                                  buffer
				 *
				 * @throws IllegalArgumentException If the source buffer is this buffer
				 */
				BigByteBuffer put(BigByteBuffer src);

				/**
				 * Relative bulk <i>put</i> method
				 * <p>
				 * This method transfers the bytes remaining in the given source buffer into
				 * this buffer. If there are more bytes remaining in the source buffer than in
				 * this buffer, that is, if
				 * {@code src.remaining()}&nbsp;{@code >}&nbsp;{@code remaining()}, then no
				 * shorts are transferred and a {@link BufferOverflowException} is thrown.
				 * <p>
				 * Otherwise, this method copies <i>n</i>&nbsp;=&nbsp;{@code src.remaining()}
				 * doubles from the given buffer into this buffer, starting at each buffer's
				 * current position. The positions of both buffers are then incremented by
				 * <i>n</i>.
				 *
				 * <p>
				 * In other words, an invocation of this method of the form {@code dst.put(src)}
				 * has exactly the same effect as the loop
				 *
				 * <pre>
				 * while (src.hasRemaining())
				 * 	dst.put(src.get());
				 * </pre>
				 *
				 * except that it first checks that there is sufficient space in this buffer and
				 * it is potentially much more efficient. If this buffer and the source buffer
				 * share the same backing array or memory, then the result will be as if the
				 * source elements were first copied to an intermediate location before being
				 * written into this buffer.
				 *
				 * @param src The source buffer from which bytes are to be read; must not be
				 *            this buffer
				 *
				 * @return This buffer
				 *
				 * @throws BufferOverflowException If there is insufficient space in this buffer
				 *                                 for the remaining bytes in the source buffer
				 */
			    BigByteBuffer put(ByteBuffer src);

				/**
				 * Relative bulk <i>put</i> method
				 * <p>
				 * This method transfers the entire content of the given source byte array into
				 * this buffer.
				 *
				 * @param src The source array
				 *
				 * @return This buffer
				 *
				 * @throws BufferOverflowException If there is insufficient space in this buffer
				 */
				BigByteBuffer put(byte[] src);

				/**
				 * Absolute bulk <i>put</i> method.
				 *
				 * <p>
				 * This method transfers {@code length} bytes into this buffer from the given
				 * source buffer, starting at the given {@code offset} in the source buffer and
				 * the given {@code index} in this buffer. The positions of both buffers are
				 * unchanged.
				 *
				 * <p>
				 * In other words, an invocation of this method of the form
				 * <code>dst.put(index,&nbsp;src,&nbsp;offset,&nbsp;length)</code> has exactly
				 * the same effect as the loop
				 *
				 * <pre>{@code
				 * for (int i = offset, j = index; i < offset + length; i++, j++)
				 * 	dst.put(j, src.get(i));
				 * }</pre>
				 *
				 * except that it first checks the consistency of the supplied parameters and it
				 * is potentially much more efficient. If this buffer and the source buffer
				 * share the same backing array or memory, then the result will be as if the
				 * source elements were first copied to an intermediate location before being
				 * written into this buffer.
				 *
				 * @param index  The index in this buffer at which the first byte will be
				 *               written; must be non-negative and less than {@code limit()}
				 *
				 * @param src    The buffer from which bytes are to be read
				 *
				 * @param offset The index within the source buffer of the first byte to be
				 *               read; must be non-negative and less than {@code src.limit()}
				 *
				 * @param length The number of bytes to be read from the given buffer; must be
				 *               non-negative and no larger than the smaller of
				 *               {@code limit() - index} and {@code src.limit() - offset}
				 *
				 * @return This buffer
				 *
				 * @throws IndexOutOfBoundsException If the preconditions on the {@code index},
				 *                                   {@code offset}, and {@code length}
				 *                                   parameters do not hold
				 */
				BigByteBuffer put(long index, ByteBuffer src, int offset, int length);

				/**
				 * Relative <i>get</i> method. Reads the byte at this buffer's current position,
				 * and then increments the position.
				 *
				 * @return The byte at the buffer's current position
				 *
				 * @throws BufferUnderflowException If the buffer's current position is not
				 *                                  smaller than its limit
				 */
				byte get();

				/**
				 * Absolute <i>get</i> method. Reads the byte at the given index.
				 *
				 * @param index The index from which the byte will be read
				 *
				 * @return The byte at the given index
				 *
				 * @throws IndexOutOfBoundsException If {@code index} is negative or not smaller
				 *                                   than the buffer's limit
				 */
				byte get(long index);

				/**
				 * Relative bulk <i>get</i> method.
				 *
				 * <p> This method transfers bytes from this buffer into the given
				 * destination array.  If there are fewer bytes remaining in the
				 * buffer than are required to satisfy the request, that is, if
				 * {@code length}&nbsp;{@code >}&nbsp;{@code remaining()}, then no
				 * bytes are transferred and a {@link BufferUnderflowException} is
				 * thrown.
				 *
				 * <p> Otherwise, this method copies {@code length} bytes from this
				 * buffer into the given array, starting at the current position of this
				 * buffer and at the given offset in the array.  The position of this
				 * buffer is then incremented by {@code length}.
				 *
				 * <p> In other words, an invocation of this method of the form
				 * <code>src.get(dst,&nbsp;off,&nbsp;len)</code> has exactly the same effect as
				 * the loop
				 *
				 * <pre>{@code
				 *     for (int i = off; i < off + len; i++)
				 *         dst[i] = src.get();
				 * }</pre>
				 *
				 * except that it first checks that there are sufficient bytes in
				 * this buffer and it is potentially much more efficient.
				 *
				 * @param  dst
				 *         The array into which bytes are to be written
				 *
				 * @param  offset
				 *         The offset within the array of the first byte to be
				 *         written; must be non-negative and no larger than
				 *         {@code dst.length}
				 *
				 * @param  length
				 *         The maximum number of bytes to be written to the given
				 *         array; must be non-negative and no larger than
				 *         {@code dst.length - offset}
				 *
				 * @return  This buffer
				 *
				 * @throws  BufferUnderflowException
				 *          If there are fewer than {@code length} bytes
				 *          remaining in this buffer
				 *
				 * @throws  IndexOutOfBoundsException
				 *          If the preconditions on the {@code offset} and {@code length}
				 *          parameters do not hold
				 */
				BigByteBuffer get(byte[] dst, int offset, int length);

				/**
				 * Relative bulk <i>get</i> method.
				 *
				 * <p> This method transfers bytes from this buffer into the given
				 * destination array.  An invocation of this method of the form
				 * {@code src.get(a)} behaves in exactly the same way as the invocation
				 *
				 * <pre>
				 *     src.get(a, 0, a.length) </pre>
				 *
				 * @param   dst
				 *          The destination array
				 *
				 * @return  This buffer
				 *
				 * @throws  BufferUnderflowException
				 *          If there are fewer than {@code length} bytes
				 *          remaining in this buffer
				 */
				default BigByteBuffer get(byte[] dst) {
					return get(dst, 0, dst.length);
				}

				/**
				 * Creates a view of this byte buffer as a byte buffer.
				 * <p>
				 * The content of the new buffer will start at this buffer's current position.
				 * Changes to this buffer's content will be visible in the new buffer, and vice
				 * versa; the two buffers' position and limit values will be independent.
				 * <p>
				 * The new buffer's position will be zero, its capacity and its limit will be
				 * the number of bytes remaining in this buffer, and its byte
				 * order will be that of the byte buffer at the moment the view is created.
				 *
				 * @return A new byte buffer
				 */
				default SmallByteBuffer asByteBuffer() {
					return new SmallByteBuffer(duplicate());
				}

				/**
				 * Creates a new {@link InputStream} which streams this buffer's content.
				 * <p>
				 * The stream will start at this buffer's current position, and end at this buffer's limit.
				 *
				 * @return the new {@code InputStream}
				 */
				InputStream toInputStream();
				""";
}
