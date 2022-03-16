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
import java.nio.DoubleBuffer;

/**
 * This class has an API similar to {@link DoubleBuffer}. All implementations
 * are backed by a {@link BigByteBuffer}. "Small" refers to the fact that these
 * buffers are indexed by ints rather than longs.
 * <p>
 * Differences from {@code DoubleBuffer} include:
 * <ul>
 * <li>mark and reset are not supported
 * <li>read-only is not supported
 * <li>byte order is preserved in {@link #duplicate()} and {@link #slice()}.
 * </ul>
 */
public final class SmallDoubleBuffer extends SmallBuffer {

	private static final int SHIFT = 3;

	SmallDoubleBuffer(BigByteBuffer buffer) {
		super(buffer);
	}

	/**
	 * Relative <i>put</i> method
	 * <p>
	 * Writes the given double into this buffer at the current position, and then
	 * increments the position.
	 *
	 * @param value The double to be written
	 *
	 * @return This buffer
	 *
	 * @throws BufferOverflowException If this buffer's current position is not
	 *                                 smaller than its limit
	 */
	public SmallDoubleBuffer put(double value) {
		buffer.putDouble(value);
		return this;
	}

	/**
	 * Absolute <i>put</i> method
	 * <p>
	 * Writes the given double into this buffer at the given index.
	 *
	 * @param index The index at which the double will be written
	 *
	 * @param value The double value to be written
	 *
	 * @return This buffer
	 *
	 * @throws IndexOutOfBoundsException If {@code index} is negative or not smaller
	 *                                   than the buffer's limit
	 */
	public SmallDoubleBuffer put(int index, double value) {
		buffer.putDouble((long) index << SHIFT, value);
		return this;
	}

	/**
	 * Relative bulk <i>put</i> method
	 * <p>
	 * This method transfers the entire content of the given source Double array
	 * into this buffer.
	 *
	 * @param src The source array
	 *
	 * @return This buffer
	 *
	 * @throws BufferOverflowException If there is insufficient space in this buffer
	 */
	public final SmallDoubleBuffer put(double[] src) {
		buffer.putDouble(src);
		return this;
	}

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
	public SmallDoubleBuffer put(DoubleBuffer src) {
		buffer.putDouble(src);
		return this;
	}

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
	public SmallDoubleBuffer put(SmallDoubleBuffer src) {
		buffer.put(src.buffer);
		return this;
	}

	/**
	 * Relative <i>get</i> method. Reads the double at this buffer's current
	 * position, and then increments the position.
	 *
	 * @return The double at the buffer's current position
	 *
	 * @throws BufferUnderflowException If the buffer's current position is not
	 *                                  smaller than its limit
	 */
	public double get() {
		return buffer.getDouble();
	}

	/**
	 * Absolute <i>get</i> method. Reads the double at the given index.
	 *
	 * @param index The index from which the double will be read
	 *
	 * @return The double at the given index
	 *
	 * @throws IndexOutOfBoundsException If {@code index} is negative or not smaller
	 *                                   than the buffer's limit
	 */
	public double get(int index) {
		return buffer.getDouble((long) index << SHIFT);
	}

	@Override
	int shift() {
		return SHIFT;
	}

	@Override
	public SmallDoubleBuffer duplicate() {
		return new SmallDoubleBuffer(buffer.duplicate());
	}

	@Override
	public SmallDoubleBuffer slice() {
		return new SmallDoubleBuffer(buffer.slice());
	}

	@Override
	public String toString() {
		return "[pos=%d lim=%d cap=%d]".formatted(buffer.position(), buffer.limit(), buffer.capacity());
	}

}
