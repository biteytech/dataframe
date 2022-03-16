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
import java.nio.FloatBuffer;

/**
 * This class has an API similar to {@link FloatBuffer}. All implementations are
 * backed by a {@link BigByteBuffer}. "Small" refers to the fact that these
 * buffers are indexed by ints rather than longs.
 * <p>
 * Differences from {@code FloatBuffer} include:
 * <ul>
 * <li>mark and reset are not supported
 * <li>read-only is not supported
 * <li>byte order is preserved in {@link #duplicate()} and {@link #slice()}.
 * </ul>
 */
public final class SmallFloatBuffer extends SmallBuffer {

	private static final int SHIFT = 2;

	SmallFloatBuffer(BigByteBuffer buffer) {
		super(buffer);
	}

	/**
	 * Relative <i>put</i> method
	 * <p>
	 * Writes the given float into this buffer at the current position, and then
	 * increments the position.
	 *
	 * @param value The float to be written
	 *
	 * @return This buffer
	 *
	 * @throws BufferOverflowException If this buffer's current position is not
	 *                                 smaller than its limit
	 */
	public SmallFloatBuffer put(float value) {
		buffer.putFloat(value);
		return this;
	}

	/**
	 * Absolute <i>put</i> method
	 * <p>
	 * Writes the given float into this buffer at the given index.
	 *
	 * @param index The index at which the float will be written
	 *
	 * @param value The float value to be written
	 *
	 * @return This buffer
	 *
	 * @throws IndexOutOfBoundsException If {@code index} is negative or not smaller
	 *                                   than the buffer's limit
	 */
	public SmallFloatBuffer put(int index, float value) {
		buffer.putFloat((long) index << SHIFT, value);
		return this;
	}

	/**
	 * Relative bulk <i>put</i> method
	 * <p>
	 * This method transfers the entire content of the given source Float array into
	 * this buffer.
	 *
	 * @param src The source array
	 *
	 * @return This buffer
	 *
	 * @throws BufferOverflowException If there is insufficient space in this buffer
	 */
	public final SmallFloatBuffer put(float[] src) {
		buffer.putFloat(src);
		return this;
	}

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
	public SmallFloatBuffer put(FloatBuffer src) {
		buffer.putFloat(src);
		return this;
	}

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
	public SmallFloatBuffer put(SmallFloatBuffer src) {
		buffer.put(src.buffer);
		return this;
	}

	/**
	 * Relative <i>get</i> method. Reads the float at this buffer's current
	 * position, and then increments the position.
	 *
	 * @return The float at the buffer's current position
	 *
	 * @throws BufferUnderflowException If the buffer's current position is not
	 *                                  smaller than its limit
	 */
	public float get() {
		return buffer.getFloat();
	}

	/**
	 * Absolute <i>get</i> method. Reads the float at the given index.
	 *
	 * @param index The index from which the float will be read
	 *
	 * @return The float at the given index
	 *
	 * @throws IndexOutOfBoundsException If {@code index} is negative or not smaller
	 *                                   than the buffer's limit
	 */
	public float get(int index) {
		return buffer.getFloat((long) index << SHIFT);
	}

	@Override
	int shift() {
		return SHIFT;
	}

	@Override
	public SmallFloatBuffer duplicate() {
		return new SmallFloatBuffer(buffer.duplicate());
	}

	@Override
	public SmallFloatBuffer slice() {
		return new SmallFloatBuffer(buffer.slice());
	}

	@Override
	public String toString() {
		return "[pos=%d lim=%d cap=%d]".formatted(buffer.position(), buffer.limit(), buffer.capacity());
	}

}
