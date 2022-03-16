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

/**
 * This class has an API similar to {@link ByteBuffer}. All implementations are
 * backed by a {@link BigByteBuffer}. "Small" refers to the fact that these
 * buffers are indexed by ints rather than longs.
 * <p>
 * Differences from {@code ByteBuffer} include:
 * <ul>
 * <li>mark and reset are not supported
 * <li>read-only is not supported
 * <li>byte order is preserved in {@link #duplicate()} and {@link #slice()}.
 * </ul>
 */
public final class SmallByteBuffer extends SmallBuffer {

	private static final int SHIFT = 0;

	SmallByteBuffer(BigByteBuffer buffer) {
		super(buffer);
	}

	/**
	 * Relative <i>put</i> method
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
	public SmallByteBuffer put(byte value) {
		buffer.put(value);
		return this;
	}

	/**
	 * Absolute <i>put</i> method
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
	public SmallByteBuffer put(int index, byte value) {
		buffer.put((long) index << SHIFT, value);
		return this;
	}

	/**
	 * Relative bulk <i>put</i> method
	 * <p>
	 * This method transfers the entire content of the given source Byte array into
	 * this buffer.
	 *
	 * @param src The source array
	 *
	 * @return This buffer
	 *
	 * @throws BufferOverflowException If there is insufficient space in this buffer
	 */
	public final SmallByteBuffer put(byte[] src) {
		buffer.put(src);
		return this;
	}

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
	 * @throws BufferOverflowException If there is insufficient space in this buffer
	 *                                 for the remaining bytes in the source buffer
	 */
	public SmallByteBuffer put(ByteBuffer src) {
		buffer.put(src);
		return this;
	}

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
	 * @throws BufferOverflowException If there is insufficient space in this buffer
	 *                                 for the remaining bytes in the source buffer
	 */
	public SmallByteBuffer put(SmallByteBuffer src) {
		buffer.put(src.buffer);
		return this;
	}

	/**
	 * Relative <i>get</i> method. Reads the byte at this buffer's current position,
	 * and then increments the position.
	 *
	 * @return The byte at the buffer's current position
	 *
	 * @throws BufferUnderflowException If the buffer's current position is not
	 *                                  smaller than its limit
	 */
	public byte get() {
		return buffer.get();
	}

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
	public byte get(int index) {
		return buffer.get((long) index << SHIFT);
	}

	@Override
	int shift() {
		return SHIFT;
	}

	@Override
	public SmallByteBuffer duplicate() {
		return new SmallByteBuffer(buffer.duplicate());
	}

	@Override
	public SmallByteBuffer slice() {
		return new SmallByteBuffer(buffer.slice());
	}

	@Override
	public String toString() {
		return "[pos=%d lim=%d cap=%d]".formatted(buffer.position(), buffer.limit(), buffer.capacity());
	}

}
