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

import java.nio.Buffer;
import java.nio.ByteOrder;

/**
 * This class has an API similar to {@link Buffer}. All implementations are
 * backed by a {@link BigByteBuffer}. "Small" refers to the fact that these
 * buffers are indexed by ints rather than longs.
 * <p>
 * Differences from {@code Buffer} include:
 * <ul>
 * <li>mark and reset are not supported
 * <li>read-only is not supported
 * <li>byte order is preserved in {@link #duplicate()} and {@link #slice()}.
 * </ul>
 *
 * @author biteytech@protonmail.com, adapted from {@link Buffer}
 */
public sealed abstract class SmallBuffer permits SmallByteBuffer,SmallShortBuffer,SmallIntBuffer,SmallLongBuffer,SmallFloatBuffer,SmallDoubleBuffer {

	final BigByteBuffer buffer;

	SmallBuffer(BigByteBuffer buffer) {
		this.buffer = buffer;
	}

	/**
	 * Returns {@code log_2(element size in bytes)}.
	 * <ul>
	 * <li>1 byte: 0
	 * <li>2 bytes: 1
	 * <li>4 bytes: 2
	 * <li>8 bytes: 3
	 * </ul>
	 * 
	 * @return element size in bytes
	 */
	abstract int shift();

	/**
	 * Returns the underlying {@link BigByteBuffer buffer}.
	 *
	 * @return the underlying buffer.
	 */
	public BigByteBuffer unwrap() {
		return buffer;
	}

	/**
	 * Returns this buffer's position.
	 *
	 * @return The position of this buffer
	 */
	public int position() {
		return (int) (buffer.position() >> shift());
	}

	/**
	 * Returns this buffer's limit.
	 *
	 * @return The limit of this buffer
	 */
	public int limit() {
		return (int) (buffer.limit() >> shift());
	}

	/**
	 * Returns this buffer's capacity.
	 *
	 * @return The capacity of this buffer
	 */
	public int capacity() {
		return (int) (buffer.capacity() >> shift());
	}

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
	public SmallBuffer position(int newPosition) {
		buffer.position((long) newPosition << shift());
		return this;
	}

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
	public SmallBuffer limit(int newLimit) {
		buffer.limit((long) newLimit << shift());
		return this;
	}

	/**
	 * Returns the number of elements between the current position and the limit.
	 *
	 * @return The number of elements remaining in this buffer
	 */
	public int remaining() {
		return (int) (buffer.remaining() >> shift());
	}

	/**
	 * Tells whether there are any elements between the current position and the
	 * limit.
	 *
	 * @return {@code true} if, and only if, there is at least one element remaining
	 *         in this buffer
	 */
	public boolean hasRemaining() {
		return buffer.hasRemaining();
	}

	/**
	 * Retrieves this buffer's byte order.
	 *
	 * <p>
	 * The byte order is used when reading or writing multibyte values, and when
	 * creating buffers that are views of this byte buffer.
	 * </p>
	 *
	 * @return This buffer's byte order
	 */
	public ByteOrder order() {
		return buffer.order();
	}

	/**
	 * Creates a new buffer that shares this buffer's content.
	 * <p>
	 * The content of the new buffer will be that of this buffer. Changes to this
	 * buffer's content will be visible in the new buffer, and vice versa; the two
	 * buffers' position and limit values will be independent.
	 * <p>
	 * The new buffer's capacity, limit, position, and byte order values will be
	 * identical to those of this buffer. The new buffer will be direct if, and only
	 * if, this buffer is direct.
	 *
	 * @return The new buffer
	 */
	public abstract SmallBuffer duplicate();

	/**
	 * Creates a new buffer whose content is a shared subsequence of this buffer's
	 * content.
	 * <p>
	 * The content of the new buffer will start at this buffer's current position.
	 * Changes to this buffer's content will be visible in the new buffer, and vice
	 * versa; the two buffers' position and limit values will be independent.
	 * <p>
	 * The new buffer's position will be zero, its capacity and its limit will be
	 * the number of elements remaining in this buffer, and the byte order will be
	 * the same as this buffer. The new buffer will be direct if, and only if, this
	 * buffer is direct.
	 *
	 * @return The new buffer
	 */
	public abstract SmallBuffer slice();

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
	public SmallBuffer clear() {
		buffer.clear();
		return this;
	}

	/**
	 * Flips this buffer. The limit is set to the current position and then the
	 * position is set to zero.
	 *
	 * @return This buffer
	 */
	public SmallBuffer flip() {
		buffer.flip();
		return this;
	}
}
