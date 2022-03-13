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
public interface BigByteBuffer {

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
	 * Creates a new {@link ByteBuffer} whose content is a shared subsequence of
	 * this buffer's content.
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
	public BigByteBuffer put(BigByteBuffer src);

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
	public BigByteBuffer put(ByteBuffer src);

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
	public BigByteBuffer put(byte[] src);

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
	 * Creates a view of this byte buffer as a byte buffer.
	 * <p>
	 * The content of the new buffer will start at this buffer's current position.
	 * Changes to this buffer's content will be visible in the new buffer, and vice
	 * versa; the two buffers' position and limit values will be independent.
	 * <p>
	 * The new buffer's position will be zero, its capacity and its limit will be
	 * the number of bytes remaining in this buffer, and its byte order will be that
	 * of the byte buffer at the moment the view is created.
	 *
	 * @return A new byte buffer
	 */
	default SmallByteBuffer asByteBuffer() {
		return new SmallByteBuffer(duplicate());
	}

	/**
	 * Relative <i>put</i> method for writing a short.
	 * <p>
	 * Writes two bytes containing the given short value, in the current byte order,
	 * into this buffer at the current position, and then increments the position by
	 * two.
	 *
	 * @param value The short value to be written
	 *
	 * @return This buffer
	 *
	 * @throws BufferOverflowException If there are fewer than two bytes remaining
	 *                                 in this buffer
	 */
	BigByteBuffer putShort(short value);

	/**
	 * Absolute <i>put</i> method for writing a short.
	 * <p>
	 * Writes two bytes containing the given short value, in the current byte order,
	 * into this buffer at the given index.
	 *
	 * @param index The index at which the bytes will be written
	 *
	 * @param value The short value to be written
	 *
	 * @return This buffer
	 *
	 * @throws IndexOutOfBoundsException If {@code index} is negative or not smaller
	 *                                   than the buffer's limit, minus one
	 */
	BigByteBuffer putShort(long index, short value);

	/**
	 * Relative bulk <i>put</i> method
	 * <p>
	 * This method transfers the entire content of the given source short array into
	 * this buffer.
	 *
	 * @param src The source array
	 *
	 * @return This buffer
	 *
	 * @throws BufferOverflowException If there is insufficient space in this buffer
	 */
	public BigByteBuffer putShort(short[] src);

	/**
	 * Relative bulk <i>put</i> method
	 * <p>
	 * This method transfers the shorts remaining in the given source buffer into
	 * this buffer. If there are more shorts remaining in the source buffer than in
	 * this buffer, that is, if
	 * {@code src.remaining()}&nbsp;{@code >}&nbsp;{@code remaining()}, then no
	 * shorts are transferred and a {@link BufferOverflowException} is thrown.
	 * <p>
	 * Otherwise, this method copies <i>n</i>&nbsp;=&nbsp;{@code src.remaining()}
	 * shorts from the given buffer into this buffer, starting at each buffer's
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
	 * @param src The source buffer from which shorts are to be read; must not be
	 *            this buffer
	 *
	 * @return This buffer
	 *
	 * @throws BufferOverflowException If there is insufficient space in this buffer
	 *                                 for the remaining shorts in the source buffer
	 */
	public BigByteBuffer putShort(ShortBuffer src);

	/**
	 * Relative <i>get</i> method for reading a short value.
	 *
	 * <p>
	 * Reads the next two bytes at this buffer's current position, composing them
	 * into a short value according to the current byte order, and then increments
	 * the position by two.
	 * </p>
	 *
	 * @return The short value at the buffer's current position
	 *
	 * @throws BufferUnderflowException If there are fewer than two bytes remaining
	 *                                  in this buffer
	 */
	short getShort();

	/**
	 * Absolute <i>get</i> method for reading a short value.
	 * <p>
	 * Reads two bytes at the given index, composing them into a short value
	 * according to the current byte order.
	 *
	 * @param index The index from which the bytes will be read
	 *
	 * @return The short value at the given index
	 *
	 * @throws IndexOutOfBoundsException If {@code index} is negative or not smaller
	 *                                   than the buffer's limit, minus one
	 */
	short getShort(long index);

	/**
	 * Creates a view of this byte buffer as a short buffer.
	 * <p>
	 * The content of the new buffer will start at this buffer's current position.
	 * Changes to this buffer's content will be visible in the new buffer, and vice
	 * versa; the two buffers' position and limit values will be independent.
	 * <p>
	 * The new buffer's position will be zero, its capacity and its limit will be
	 * the number of bytes remaining in this buffer divided by two, and its byte
	 * order will be that of the byte buffer at the moment the view is created.
	 *
	 * @return A new short buffer
	 */
	default SmallShortBuffer asShortBuffer() {
		return new SmallShortBuffer(duplicate());
	}

	/**
	 * Relative <i>put</i> method for writing a int.
	 * <p>
	 * Writes four bytes containing the given int value, in the current byte order,
	 * into this buffer at the current position, and then increments the position by
	 * four.
	 *
	 * @param value The int value to be written
	 *
	 * @return This buffer
	 *
	 * @throws BufferOverflowException If there are fewer than four bytes remaining
	 *                                 in this buffer
	 */
	BigByteBuffer putInt(int value);

	/**
	 * Absolute <i>put</i> method for writing a int.
	 * <p>
	 * Writes four bytes containing the given int value, in the current byte order,
	 * into this buffer at the given index.
	 *
	 * @param index The index at which the bytes will be written
	 *
	 * @param value The int value to be written
	 *
	 * @return This buffer
	 *
	 * @throws IndexOutOfBoundsException If {@code index} is negative or not smaller
	 *                                   than the buffer's limit, minus one
	 */
	BigByteBuffer putInt(long index, int value);

	/**
	 * Relative bulk <i>put</i> method
	 * <p>
	 * This method transfers the entire content of the given source int array into
	 * this buffer.
	 *
	 * @param src The source array
	 *
	 * @return This buffer
	 *
	 * @throws BufferOverflowException If there is insufficient space in this buffer
	 */
	public BigByteBuffer putInt(int[] src);

	/**
	 * Relative bulk <i>put</i> method
	 * <p>
	 * This method transfers the ints remaining in the given source buffer into this
	 * buffer. If there are more ints remaining in the source buffer than in this
	 * buffer, that is, if
	 * {@code src.remaining()}&nbsp;{@code >}&nbsp;{@code remaining()}, then no
	 * shorts are transferred and a {@link BufferOverflowException} is thrown.
	 * <p>
	 * Otherwise, this method copies <i>n</i>&nbsp;=&nbsp;{@code src.remaining()}
	 * ints from the given buffer into this buffer, starting at each buffer's
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
	 * @param src The source buffer from which ints are to be read; must not be this
	 *            buffer
	 *
	 * @return This buffer
	 *
	 * @throws BufferOverflowException If there is insufficient space in this buffer
	 *                                 for the remaining ints in the source buffer
	 */
	public BigByteBuffer putInt(IntBuffer src);

	/**
	 * Relative <i>get</i> method for reading a int value.
	 *
	 * <p>
	 * Reads the next four bytes at this buffer's current position, composing them
	 * into a int value according to the current byte order, and then increments the
	 * position by four.
	 * </p>
	 *
	 * @return The int value at the buffer's current position
	 *
	 * @throws BufferUnderflowException If there are fewer than four bytes remaining
	 *                                  in this buffer
	 */
	int getInt();

	/**
	 * Absolute <i>get</i> method for reading a int value.
	 * <p>
	 * Reads four bytes at the given index, composing them into a int value
	 * according to the current byte order.
	 *
	 * @param index The index from which the bytes will be read
	 *
	 * @return The int value at the given index
	 *
	 * @throws IndexOutOfBoundsException If {@code index} is negative or not smaller
	 *                                   than the buffer's limit, minus one
	 */
	int getInt(long index);

	/**
	 * Creates a view of this byte buffer as a int buffer.
	 * <p>
	 * The content of the new buffer will start at this buffer's current position.
	 * Changes to this buffer's content will be visible in the new buffer, and vice
	 * versa; the two buffers' position and limit values will be independent.
	 * <p>
	 * The new buffer's position will be zero, its capacity and its limit will be
	 * the number of bytes remaining in this buffer divided by four, and its byte
	 * order will be that of the byte buffer at the moment the view is created.
	 *
	 * @return A new int buffer
	 */
	default SmallIntBuffer asIntBuffer() {
		return new SmallIntBuffer(duplicate());
	}

	/**
	 * Relative <i>put</i> method for writing a long.
	 * <p>
	 * Writes eight bytes containing the given long value, in the current byte
	 * order, into this buffer at the current position, and then increments the
	 * position by eight.
	 *
	 * @param value The long value to be written
	 *
	 * @return This buffer
	 *
	 * @throws BufferOverflowException If there are fewer than eight bytes remaining
	 *                                 in this buffer
	 */
	BigByteBuffer putLong(long value);

	/**
	 * Absolute <i>put</i> method for writing a long.
	 * <p>
	 * Writes eight bytes containing the given long value, in the current byte
	 * order, into this buffer at the given index.
	 *
	 * @param index The index at which the bytes will be written
	 *
	 * @param value The long value to be written
	 *
	 * @return This buffer
	 *
	 * @throws IndexOutOfBoundsException If {@code index} is negative or not smaller
	 *                                   than the buffer's limit, minus one
	 */
	BigByteBuffer putLong(long index, long value);

	/**
	 * Relative bulk <i>put</i> method
	 * <p>
	 * This method transfers the entire content of the given source long array into
	 * this buffer.
	 *
	 * @param src The source array
	 *
	 * @return This buffer
	 *
	 * @throws BufferOverflowException If there is insufficient space in this buffer
	 */
	public BigByteBuffer putLong(long[] src);

	/**
	 * Relative bulk <i>put</i> method
	 * <p>
	 * This method transfers the longs remaining in the given source buffer into
	 * this buffer. If there are more longs remaining in the source buffer than in
	 * this buffer, that is, if
	 * {@code src.remaining()}&nbsp;{@code >}&nbsp;{@code remaining()}, then no
	 * shorts are transferred and a {@link BufferOverflowException} is thrown.
	 * <p>
	 * Otherwise, this method copies <i>n</i>&nbsp;=&nbsp;{@code src.remaining()}
	 * longs from the given buffer into this buffer, starting at each buffer's
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
	 * @param src The source buffer from which longs are to be read; must not be
	 *            this buffer
	 *
	 * @return This buffer
	 *
	 * @throws BufferOverflowException If there is insufficient space in this buffer
	 *                                 for the remaining longs in the source buffer
	 */
	public BigByteBuffer putLong(LongBuffer src);

	/**
	 * Relative <i>get</i> method for reading a long value.
	 *
	 * <p>
	 * Reads the next eight bytes at this buffer's current position, composing them
	 * into a long value according to the current byte order, and then increments
	 * the position by eight.
	 * </p>
	 *
	 * @return The long value at the buffer's current position
	 *
	 * @throws BufferUnderflowException If there are fewer than eight bytes
	 *                                  remaining in this buffer
	 */
	long getLong();

	/**
	 * Absolute <i>get</i> method for reading a long value.
	 * <p>
	 * Reads eight bytes at the given index, composing them into a long value
	 * according to the current byte order.
	 *
	 * @param index The index from which the bytes will be read
	 *
	 * @return The long value at the given index
	 *
	 * @throws IndexOutOfBoundsException If {@code index} is negative or not smaller
	 *                                   than the buffer's limit, minus one
	 */
	long getLong(long index);

	/**
	 * Creates a view of this byte buffer as a long buffer.
	 * <p>
	 * The content of the new buffer will start at this buffer's current position.
	 * Changes to this buffer's content will be visible in the new buffer, and vice
	 * versa; the two buffers' position and limit values will be independent.
	 * <p>
	 * The new buffer's position will be zero, its capacity and its limit will be
	 * the number of bytes remaining in this buffer divided by eight, and its byte
	 * order will be that of the byte buffer at the moment the view is created.
	 *
	 * @return A new long buffer
	 */
	default SmallLongBuffer asLongBuffer() {
		return new SmallLongBuffer(duplicate());
	}

	/**
	 * Relative <i>put</i> method for writing a float.
	 * <p>
	 * Writes four bytes containing the given float value, in the current byte
	 * order, into this buffer at the current position, and then increments the
	 * position by four.
	 *
	 * @param value The float value to be written
	 *
	 * @return This buffer
	 *
	 * @throws BufferOverflowException If there are fewer than four bytes remaining
	 *                                 in this buffer
	 */
	BigByteBuffer putFloat(float value);

	/**
	 * Absolute <i>put</i> method for writing a float.
	 * <p>
	 * Writes four bytes containing the given float value, in the current byte
	 * order, into this buffer at the given index.
	 *
	 * @param index The index at which the bytes will be written
	 *
	 * @param value The float value to be written
	 *
	 * @return This buffer
	 *
	 * @throws IndexOutOfBoundsException If {@code index} is negative or not smaller
	 *                                   than the buffer's limit, minus one
	 */
	BigByteBuffer putFloat(long index, float value);

	/**
	 * Relative bulk <i>put</i> method
	 * <p>
	 * This method transfers the entire content of the given source float array into
	 * this buffer.
	 *
	 * @param src The source array
	 *
	 * @return This buffer
	 *
	 * @throws BufferOverflowException If there is insufficient space in this buffer
	 */
	public BigByteBuffer putFloat(float[] src);

	/**
	 * Relative bulk <i>put</i> method
	 * <p>
	 * This method transfers the floats remaining in the given source buffer into
	 * this buffer. If there are more floats remaining in the source buffer than in
	 * this buffer, that is, if
	 * {@code src.remaining()}&nbsp;{@code >}&nbsp;{@code remaining()}, then no
	 * shorts are transferred and a {@link BufferOverflowException} is thrown.
	 * <p>
	 * Otherwise, this method copies <i>n</i>&nbsp;=&nbsp;{@code src.remaining()}
	 * floats from the given buffer into this buffer, starting at each buffer's
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
	 * @param src The source buffer from which floats are to be read; must not be
	 *            this buffer
	 *
	 * @return This buffer
	 *
	 * @throws BufferOverflowException If there is insufficient space in this buffer
	 *                                 for the remaining floats in the source buffer
	 */
	public BigByteBuffer putFloat(FloatBuffer src);

	/**
	 * Relative <i>get</i> method for reading a float value.
	 *
	 * <p>
	 * Reads the next four bytes at this buffer's current position, composing them
	 * into a float value according to the current byte order, and then increments
	 * the position by four.
	 * </p>
	 *
	 * @return The float value at the buffer's current position
	 *
	 * @throws BufferUnderflowException If there are fewer than four bytes remaining
	 *                                  in this buffer
	 */
	float getFloat();

	/**
	 * Absolute <i>get</i> method for reading a float value.
	 * <p>
	 * Reads four bytes at the given index, composing them into a float value
	 * according to the current byte order.
	 *
	 * @param index The index from which the bytes will be read
	 *
	 * @return The float value at the given index
	 *
	 * @throws IndexOutOfBoundsException If {@code index} is negative or not smaller
	 *                                   than the buffer's limit, minus one
	 */
	float getFloat(long index);

	/**
	 * Creates a view of this byte buffer as a float buffer.
	 * <p>
	 * The content of the new buffer will start at this buffer's current position.
	 * Changes to this buffer's content will be visible in the new buffer, and vice
	 * versa; the two buffers' position and limit values will be independent.
	 * <p>
	 * The new buffer's position will be zero, its capacity and its limit will be
	 * the number of bytes remaining in this buffer divided by four, and its byte
	 * order will be that of the byte buffer at the moment the view is created.
	 *
	 * @return A new float buffer
	 */
	default SmallFloatBuffer asFloatBuffer() {
		return new SmallFloatBuffer(duplicate());
	}

	/**
	 * Relative <i>put</i> method for writing a double.
	 * <p>
	 * Writes eight bytes containing the given double value, in the current byte
	 * order, into this buffer at the current position, and then increments the
	 * position by eight.
	 *
	 * @param value The double value to be written
	 *
	 * @return This buffer
	 *
	 * @throws BufferOverflowException If there are fewer than eight bytes remaining
	 *                                 in this buffer
	 */
	BigByteBuffer putDouble(double value);

	/**
	 * Absolute <i>put</i> method for writing a double.
	 * <p>
	 * Writes eight bytes containing the given double value, in the current byte
	 * order, into this buffer at the given index.
	 *
	 * @param index The index at which the bytes will be written
	 *
	 * @param value The double value to be written
	 *
	 * @return This buffer
	 *
	 * @throws IndexOutOfBoundsException If {@code index} is negative or not smaller
	 *                                   than the buffer's limit, minus one
	 */
	BigByteBuffer putDouble(long index, double value);

	/**
	 * Relative bulk <i>put</i> method
	 * <p>
	 * This method transfers the entire content of the given source double array
	 * into this buffer.
	 *
	 * @param src The source array
	 *
	 * @return This buffer
	 *
	 * @throws BufferOverflowException If there is insufficient space in this buffer
	 */
	public BigByteBuffer putDouble(double[] src);

	/**
	 * Relative bulk <i>put</i> method
	 * <p>
	 * This method transfers the doubles remaining in the given source buffer into
	 * this buffer. If there are more doubles remaining in the source buffer than in
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
	 * @param src The source buffer from which doubles are to be read; must not be
	 *            this buffer
	 *
	 * @return This buffer
	 *
	 * @throws BufferOverflowException If there is insufficient space in this buffer
	 *                                 for the remaining doubles in the source
	 *                                 buffer
	 */
	public BigByteBuffer putDouble(DoubleBuffer src);

	/**
	 * Relative <i>get</i> method for reading a double value.
	 *
	 * <p>
	 * Reads the next eight bytes at this buffer's current position, composing them
	 * into a double value according to the current byte order, and then increments
	 * the position by eight.
	 * </p>
	 *
	 * @return The double value at the buffer's current position
	 *
	 * @throws BufferUnderflowException If there are fewer than eight bytes
	 *                                  remaining in this buffer
	 */
	double getDouble();

	/**
	 * Absolute <i>get</i> method for reading a double value.
	 * <p>
	 * Reads eight bytes at the given index, composing them into a double value
	 * according to the current byte order.
	 *
	 * @param index The index from which the bytes will be read
	 *
	 * @return The double value at the given index
	 *
	 * @throws IndexOutOfBoundsException If {@code index} is negative or not smaller
	 *                                   than the buffer's limit, minus one
	 */
	double getDouble(long index);

	/**
	 * Creates a view of this byte buffer as a double buffer.
	 * <p>
	 * The content of the new buffer will start at this buffer's current position.
	 * Changes to this buffer's content will be visible in the new buffer, and vice
	 * versa; the two buffers' position and limit values will be independent.
	 * <p>
	 * The new buffer's position will be zero, its capacity and its limit will be
	 * the number of bytes remaining in this buffer divided by eight, and its byte
	 * order will be that of the byte buffer at the moment the view is created.
	 *
	 * @return A new double buffer
	 */
	default SmallDoubleBuffer asDoubleBuffer() {
		return new SmallDoubleBuffer(duplicate());
	}

}
