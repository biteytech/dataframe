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
import java.nio.LongBuffer;

/**
 * This class has an API similar to {@link LongBuffer}. All implementations are
 * backed by a {@link BigByteBuffer}. "Small" refers to the fact that these
 * buffers are indexed by ints rather than longs.
 * <p>
 * Differences from {@code LongBuffer} include:
 * <ul>
 * <li>mark and reset are not supported
 * <li>read-only is not supported
 * <li>byte order is preserved in {@link #duplicate()} and {@link #slice()}.
 * </ul>
 */
public final class SmallLongBuffer extends SmallBuffer {

	private static final int SHIFT = 3;

	SmallLongBuffer(BigByteBuffer buffer) {
		super(buffer);
	}

	/**
	 * Relative <i>put</i> method
	 * <p>
	 * Writes the given long into this buffer at the current position, and then
	 * increments the position.
	 *
	 * @param value The long to be written
	 *
	 * @return This buffer
	 *
	 * @throws BufferOverflowException If this buffer's current position is not
	 *                                 smaller than its limit
	 */
	public SmallLongBuffer put(long value) {
		buffer.putLong(value);
		return this;
	}

	/**
	 * Absolute <i>put</i> method
	 * <p>
	 * Writes the given long into this buffer at the given index.
	 *
	 * @param index The index at which the long will be written
	 *
	 * @param value The long value to be written
	 *
	 * @return This buffer
	 *
	 * @throws IndexOutOfBoundsException If {@code index} is negative or not smaller
	 *                                   than the buffer's limit
	 */
	public SmallLongBuffer put(int index, long value) {
		buffer.putLong((long) index << SHIFT, value);
		return this;
	}

	/**
	 * Relative bulk <i>put</i> method
	 * <p>
	 * This method transfers the entire content of the given source Long array into
	 * this buffer.
	 *
	 * @param src The source array
	 *
	 * @return This buffer
	 *
	 * @throws BufferOverflowException If there is insufficient space in this buffer
	 */
	public final SmallLongBuffer put(long[] src) {
		buffer.putLong(src);
		return this;
	}

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
	public SmallLongBuffer put(LongBuffer src) {
		buffer.putLong(src);
		return this;
	}

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
	public SmallLongBuffer put(SmallLongBuffer src) {
		buffer.put(src.buffer);
		return this;
	}

	/**
	 * Relative <i>get</i> method. Reads the long at this buffer's current position,
	 * and then increments the position.
	 *
	 * @return The long at the buffer's current position
	 *
	 * @throws BufferUnderflowException If the buffer's current position is not
	 *                                  smaller than its limit
	 */
	public long get() {
		return buffer.getLong();
	}

	/**
	 * Absolute <i>get</i> method. Reads the long at the given index.
	 *
	 * @param index The index from which the long will be read
	 *
	 * @return The long at the given index
	 *
	 * @throws IndexOutOfBoundsException If {@code index} is negative or not smaller
	 *                                   than the buffer's limit
	 */
	public long get(int index) {
		return buffer.getLong((long) index << SHIFT);
	}

	@Override
	int shift() {
		return SHIFT;
	}

	@Override
	public SmallLongBuffer duplicate() {
		return new SmallLongBuffer(buffer.duplicate());
	}

	@Override
	public SmallLongBuffer slice() {
		return new SmallLongBuffer(buffer.slice());
	}

	@Override
	public String toString() {
		return "[pos=%d lim=%d cap=%d]".formatted(buffer.position(), buffer.limit(), buffer.capacity());
	}

}
