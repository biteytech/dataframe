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

import static java.lang.Math.toIntExact;

import java.io.IOException;
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
import java.util.Objects;

final class CompoundBigByteBuffer extends AbstractBigByteBuffer {

	static final int CHUNK_BITS = 30;
	static final int CHUNK_SIZE = 1 << CHUNK_BITS;
	static final int CHUNK_MASK = CHUNK_SIZE - 1;

	/*
	 * Only the first and last buffers in this array can have capacity < CHUNK_SIZE
	 */
	private final ByteBuffer[] buffers;

	private long position;
	private long limit;
	private final long capacity;
	private final int capacity0; // capacity of first buffer

	private CompoundBigByteBuffer(ByteBuffer[] buffers, long position, long limit, long capacity) {

		if (position < 0)
			throw new IllegalArgumentException("positition cannot be negative");
		else if (position > limit)
			throw new IllegalArgumentException("positition cannot be greater than limit");
		else if (limit > capacity)
			throw new IllegalArgumentException("limit cannot be greater than capacity");

		this.buffers = buffers;
		this.position = position;
		this.limit = limit;
		this.capacity = capacity;
		this.capacity0 = buffers[0].capacity();
	}

	CompoundBigByteBuffer(ByteBuffer[] buffers) {

		long capacity = 0;
		for (int i = 0; i < buffers.length; i++) {

			var b = buffers[i];

			if (b.position() != 0)
				throw new IllegalArgumentException("position must be 0");
			else if (b.limit() != b.capacity())
				throw new IllegalArgumentException("limit must equal capacity");
			else if (i != 0 && i < buffers.length - 1 && b.capacity() != CHUNK_SIZE)
				throw new IllegalArgumentException("internal chunks must have max size");

			capacity += b.capacity();
		}

		this.buffers = buffers;
		this.position = 0;
		this.limit = capacity;
		this.capacity = capacity;
		this.capacity0 = buffers[0].capacity();
	}

	@Override
	public ByteBuffer[] buffers() {
		return buffers;
	}

	@Override
	public long position() {
		return position;
	}

	@Override
	public long limit() {
		return limit;
	}

	@Override
	public long capacity() {
		return capacity;
	}

	@Override
	public BigByteBuffer position(long newPosition) {
		if (newPosition > limit || newPosition < 0)
			throw createPositionException(newPosition);
		position = newPosition;
		return this;
	}

	/**
	 * Verify that {@code 0 < newPosition <= limit}
	 *
	 * @param newPosition The new position value
	 *
	 * @throws IllegalArgumentException If the specified position is out of bounds.
	 */
	private IllegalArgumentException createPositionException(long newPosition) {
		String msg = null;

		if (newPosition > limit) {
			msg = "newPosition > limit: (" + newPosition + " > " + limit + ")";
		} else { // assume negative
			msg = "newPosition < 0: (" + newPosition + " < 0)";
		}

		return new IllegalArgumentException(msg);
	}

	@Override
	public BigByteBuffer limit(long newLimit) {
		if (newLimit > capacity || newLimit < 0)
			throw createLimitException(newLimit);
		limit = newLimit;
		if (position > newLimit)
			position = newLimit;
		return this;
	}

	/**
	 * Verify that {@code 0 < newLimit <= capacity}
	 *
	 * @param newLimit The new limit value
	 *
	 * @throws IllegalArgumentException If the specified limit is out of bounds.
	 */
	private IllegalArgumentException createLimitException(long newLimit) {
		String msg = null;

		if (newLimit > capacity) {
			msg = "newLimit > capacity: (" + newLimit + " > " + capacity + ")";
		} else { // assume negative
			msg = "newLimit < 0: (" + newLimit + " < 0)";
		}

		return new IllegalArgumentException(msg);
	}

	@Override
	public long remaining() {
		return limit - position;
	}

	@Override
	public boolean hasRemaining() {
		return position < limit;
	}

	@Override
	public ByteOrder order() {
		return buffers[0].order();
	}

	@Override
	public BigByteBuffer order(ByteOrder order) {
		for (ByteBuffer b : buffers)
			b.order(order);
		return this;
	}

	@Override
	public BigByteBuffer duplicate() {
		return new CompoundBigByteBuffer(buffers, position, limit, capacity);
	}

	@Override
	public BigByteBuffer slice() {
		return slice(position, limit);
	}

	private void checkRange(long fromIndex, long toIndex) {
		if (fromIndex < 0)
			throw new IllegalArgumentException("fromIndex cannot be negative");
		else if (toIndex > limit)
			throw new IllegalArgumentException("toIndex cannot be greater than limit");
		else if (fromIndex > toIndex)
			throw new IllegalArgumentException("fromIndex cannot be greater than toIndex");
	}

	@Override
	public BigByteBuffer slice(long fromIndex, long toIndex) {

		checkRange(fromIndex, toIndex);

		if (fromIndex == toIndex)
			return BufferUtils.EMPTY_BIG_BUFFER;

		int fromBuf = buf(fromIndex);
		int fromByt = byt(fromIndex);
		int toBuf = buf(toIndex);
		int toByt = byt(toIndex);

		if (toByt == 0)
			toByt = buffers[--toBuf].capacity();

		final ByteBuffer[] buffers;

		if (fromBuf == toBuf) {
			buffers = new ByteBuffer[] { BufferUtils.slice(this.buffers[fromBuf], fromByt, toByt) };
		} else {
			buffers = new ByteBuffer[toBuf - fromBuf + 1];
			buffers[0] = BufferUtils.slice(this.buffers[fromBuf], fromByt, this.buffers[fromBuf].capacity());
			for (int i = 1; i < buffers.length - 1; i++)
				buffers[i] = BufferUtils.duplicate(this.buffers[fromBuf + i]);
			buffers[buffers.length - 1] = BufferUtils.slice(this.buffers[toBuf], 0, toByt);
		}

		return BufferUtils.wrap(buffers);
	}

	@Override
	public ByteBuffer smallSlice() {
		return smallSlice(position, limit);
	}

	@Override
	public ByteBuffer smallSlice(long fromIndex, long toIndex) {

		checkRange(fromIndex, toIndex);

		if (fromIndex == toIndex)
			return BufferUtils.EMPTY_BUFFER;

		int fromBuf = buf(fromIndex);
		int fromByt = byt(fromIndex);
		int toBuf = buf(toIndex);
		int toByt = byt(toIndex);

		if (toByt == 0)
			toByt = buffers[--toBuf].capacity();

		if (fromBuf == toBuf) {
			return BufferUtils.slice(this.buffers[fromBuf], fromByt, toByt);
		} else {
			ByteBuffer result = BufferUtils.allocate(toIntExact(toIndex - fromIndex), order());

			result.put(BufferUtils.slice(this.buffers[fromBuf], fromByt, this.buffers[fromBuf].capacity()));
			for (int i = fromBuf + 1; i < toBuf; i++)
				result.put(BufferUtils.duplicate(this.buffers[i]));
			result.put(BufferUtils.slice(this.buffers[toBuf], 0, toByt));

			return result.flip();
		}
	}

	@Override
	public BigByteBuffer copy(long fromIndex, long toIndex) {

		checkRange(fromIndex, toIndex);

		if (fromIndex == toIndex)
			return BufferUtils.EMPTY_BIG_BUFFER;

		int fromBuf = buf(fromIndex);
		int fromByt = byt(fromIndex);
		int toBuf = buf(toIndex);
		int toByt = byt(toIndex);

		if (toByt == 0)
			toByt = buffers[--toBuf].capacity();

		final ByteBuffer[] buffers;

		if (fromBuf == toBuf) {
			buffers = new ByteBuffer[] { BufferUtils.copy(this.buffers[fromBuf], fromByt, toByt) };
		} else {
			buffers = new ByteBuffer[toBuf - fromBuf + 1];
			buffers[0] = BufferUtils.copy(this.buffers[fromBuf], fromByt, this.buffers[fromBuf].capacity());
			for (int i = 1; i < buffers.length - 1; i++)
				buffers[i] = BufferUtils.copy(this.buffers[fromBuf + i], 0, this.buffers[fromBuf + i].capacity());
			buffers[buffers.length - 1] = BufferUtils.copy(this.buffers[toBuf], 0, toByt);
		}

		return BufferUtils.wrap(buffers);
	}

	@Override
	public BigByteBuffer clear() {
		position = 0;
		limit = capacity;
		return this;
	}

	@Override
	public BigByteBuffer flip() {
		limit = position;
		position = 0;
		return this;
	}

	// index into buffers array
	private int buf(long index) {

		if (index < capacity0)
			return 0;

		index -= capacity0;

		return (int) (index >> CHUNK_BITS) + 1;
	}

	// byte index in buffer
	private int byt(long index) {

		if (index < capacity0)
			return (int) index;

		index -= capacity0;

		return (int) (index & CHUNK_MASK);
	}

	// bytes remaining in the given buffer
	private int rem(int buf, int byt) {
		return buffers[buf].capacity() - byt;
	}

	private int rem(long index) {
		return rem(buf(index), byt(index));
	}

	private long nextPutIndex() {
		long p = position;
		if (p >= limit)
			throw new BufferOverflowException();
		position = p + 1;
		return p;
	}

	private long nextPutIndex(int nb) {
		long p = position;
		if (limit - p < nb)
			throw new BufferOverflowException();
		position = p + nb;
		return p;
	}

	private long nextGetIndex() {
		long p = position;
		if (p >= limit)
			throw new BufferUnderflowException();
		position = p + 1;
		return p;
	}

	private long nextGetIndex(int nb) {
		long p = position;
		if (limit - p < nb)
			throw new BufferUnderflowException();
		position = p + nb;
		return p;
	}

	@Override
	public BigByteBuffer put(byte value) {
		return put(nextPutIndex(), value);
	}

	@Override
	public BigByteBuffer put(long index, byte value) {
		buffers[buf(index)].put(byt(index), value);
		return this;
	}

	@Override
	public BigByteBuffer put(BigByteBuffer src) {

		if (src.remaining() > remaining())
			throw new BufferOverflowException();

		for (var b : src.slice().buffers())
			position += put0(position, b);

		return this;
	}

	@Override
	public BigByteBuffer put(ByteBuffer src) {

		if (src.remaining() > remaining())
			throw new BufferOverflowException();

		position += put0(position, src);

		return this;
	}

	private int put0(long index, ByteBuffer src) {

		final int total = src.remaining();
		int copied = 0;

		while (copied < total) {

			int buf = buf(index);
			int byt = byt(index);

			int length = Math.min(total - copied, rem(buf, byt));

			buffers[buf].put(byt, src, copied, length);

			copied += length;
			index += length;
		}

		src.position(src.limit());

		return copied;
	}

	@Override
	public BigByteBuffer put(byte[] src) {
		return put(ByteBuffer.wrap(src));
	}

	@Override
	public byte get() {
		return get(nextGetIndex());
	}

	@Override
	public byte get(long index) {
		return buffers[buf(index)].get(byt(index));
	}

	@SuppressWarnings("unlikely-arg-type")
	@Override
	public boolean equals(Object o) {
		if (o instanceof SimpleBigByteBuffer rhs) {
			return rhs.equals(this);
		} else if (o instanceof CompoundBigByteBuffer rhs) {

			final long total = remaining();

			if (total != rhs.remaining())
				return false;

			long progress = 0;

			while (progress < total) {

				long lstart = this.position + progress;
				long rstart = rhs.position + progress;

				int rem = Math.min(rem(lstart), rhs.rem(rstart));

				if (!smallSlice(lstart, lstart + rem).equals(rhs.smallSlice(rstart, rstart + rem)))
					return false;

				progress += rem;
			}

			return true;

		} else {
			return false;
		}
	}

	// ===================================================================================================

	@Override
	public BigByteBuffer putShort(short value) {
		return putShort0(nextPutIndex(2), value);
	}

	@Override
	public BigByteBuffer putShort(long index, short value) {

		if (index > capacity - 2)
			throw new BufferOverflowException();

		return putShort0(index, value);
	}

	private BigByteBuffer putShort0(long index, short value) {

		int buf = buf(index);
		int byt = byt(index);

		if (rem(buf, byt) >= 2) {
			// happy path
			buffers[buf].putShort(byt, value);
		} else {
			// rare
			ByteBuffer b = BufferUtils.allocate(2, order());
			b.asShortBuffer().put(0, value);
			put0(index, b);
		}

		return this;
	}

	@Override
	public BigByteBuffer putShort(short[] src) {
		return putShort(ShortBuffer.wrap(src));
	}

	@Override
	public BigByteBuffer putShort(ShortBuffer src) {

		if ((long) src.remaining() * 2 > remaining())
			throw new BufferOverflowException();

		// TODO: optimize this
		while (src.hasRemaining())
			putShort(src.get());

		return this;
	}

	@Override
	public short getShort() {
		return getShort0(nextGetIndex(2));
	}

	@Override
	public short getShort(long index) {

		if (index > capacity - 2)
			throw new BufferUnderflowException();

		return getShort0(index);
	}

	private short getShort0(long index) {

		int buf = buf(index);
		int byt = byt(index);

		if (rem(buf, byt) >= 2) {
			// happy path
			return buffers[buf].getShort(byt);
		} else {
			// rare
			ByteBuffer b = BufferUtils.allocate(2, order());
			b.put(BufferUtils.slice(buffers[buf], byt, buffers[buf].capacity()));
			b.put(BufferUtils.slice(buffers[buf + 1], 0, b.remaining()));
			return b.getShort(0);
		}
	}

	// ===================================================================================================

	@Override
	public BigByteBuffer putInt(int value) {
		return putInt0(nextPutIndex(4), value);
	}

	@Override
	public BigByteBuffer putInt(long index, int value) {

		if (index > capacity - 4)
			throw new BufferOverflowException();

		return putInt0(index, value);
	}

	private BigByteBuffer putInt0(long index, int value) {

		int buf = buf(index);
		int byt = byt(index);

		if (rem(buf, byt) >= 4) {
			// happy path
			buffers[buf].putInt(byt, value);
		} else {
			// rare
			ByteBuffer b = BufferUtils.allocate(4, order());
			b.asIntBuffer().put(0, value);
			put0(index, b);
		}

		return this;
	}

	@Override
	public BigByteBuffer putInt(int[] src) {
		return putInt(IntBuffer.wrap(src));
	}

	@Override
	public BigByteBuffer putInt(IntBuffer src) {

		if ((long) src.remaining() * 4 > remaining())
			throw new BufferOverflowException();

		// TODO: optimize this
		while (src.hasRemaining())
			putInt(src.get());

		return this;
	}

	@Override
	public int getInt() {
		return getInt0(nextGetIndex(4));
	}

	@Override
	public int getInt(long index) {

		if (index > capacity - 4)
			throw new BufferUnderflowException();

		return getInt0(index);
	}

	private int getInt0(long index) {

		int buf = buf(index);
		int byt = byt(index);

		if (rem(buf, byt) >= 4) {
			// happy path
			return buffers[buf].getInt(byt);
		} else {
			// rare
			ByteBuffer b = BufferUtils.allocate(4, order());
			b.put(BufferUtils.slice(buffers[buf], byt, buffers[buf].capacity()));
			b.put(BufferUtils.slice(buffers[buf + 1], 0, b.remaining()));
			return b.getInt(0);
		}
	}

	// ===================================================================================================

	@Override
	public BigByteBuffer putLong(long value) {
		return putLong0(nextPutIndex(8), value);
	}

	@Override
	public BigByteBuffer putLong(long index, long value) {

		if (index > capacity - 8)
			throw new BufferOverflowException();

		return putLong0(index, value);
	}

	private BigByteBuffer putLong0(long index, long value) {

		int buf = buf(index);
		int byt = byt(index);

		if (rem(buf, byt) >= 8) {
			// happy path
			buffers[buf].putLong(byt, value);
		} else {
			// rare
			ByteBuffer b = BufferUtils.allocate(8, order());
			b.asLongBuffer().put(0, value);
			put0(index, b);
		}

		return this;
	}

	@Override
	public BigByteBuffer putLong(long[] src) {
		return putLong(LongBuffer.wrap(src));
	}

	@Override
	public BigByteBuffer putLong(LongBuffer src) {

		if ((long) src.remaining() * 8 > remaining())
			throw new BufferOverflowException();

		// TODO: optimize this
		while (src.hasRemaining())
			putLong(src.get());

		return this;
	}

	@Override
	public long getLong() {
		return getLong0(nextGetIndex(8));
	}

	@Override
	public long getLong(long index) {

		if (index > capacity - 8)
			throw new BufferUnderflowException();

		return getLong0(index);
	}

	private long getLong0(long index) {

		int buf = buf(index);
		int byt = byt(index);

		if (rem(buf, byt) >= 8) {
			// happy path
			return buffers[buf].getLong(byt);
		} else {
			// rare
			ByteBuffer b = BufferUtils.allocate(8, order());
			b.put(BufferUtils.slice(buffers[buf], byt, buffers[buf].capacity()));
			b.put(BufferUtils.slice(buffers[buf + 1], 0, b.remaining()));
			return b.getLong(0);
		}
	}

	// ===================================================================================================

	@Override
	public BigByteBuffer putFloat(float value) {
		return putFloat0(nextPutIndex(4), value);
	}

	@Override
	public BigByteBuffer putFloat(long index, float value) {

		if (index > capacity - 4)
			throw new BufferOverflowException();

		return putFloat0(index, value);
	}

	private BigByteBuffer putFloat0(long index, float value) {

		int buf = buf(index);
		int byt = byt(index);

		if (rem(buf, byt) >= 4) {
			// happy path
			buffers[buf].putFloat(byt, value);
		} else {
			// rare
			ByteBuffer b = BufferUtils.allocate(4, order());
			b.asFloatBuffer().put(0, value);
			put0(index, b);
		}

		return this;
	}

	@Override
	public BigByteBuffer putFloat(float[] src) {
		return putFloat(FloatBuffer.wrap(src));
	}

	@Override
	public BigByteBuffer putFloat(FloatBuffer src) {

		if ((long) src.remaining() * 4 > remaining())
			throw new BufferOverflowException();

		// TODO: optimize this
		while (src.hasRemaining())
			putFloat(src.get());

		return this;
	}

	@Override
	public float getFloat() {
		return getFloat0(nextGetIndex(4));
	}

	@Override
	public float getFloat(long index) {

		if (index > capacity - 4)
			throw new BufferUnderflowException();

		return getFloat0(index);
	}

	private float getFloat0(long index) {

		int buf = buf(index);
		int byt = byt(index);

		if (rem(buf, byt) >= 4) {
			// happy path
			return buffers[buf].getFloat(byt);
		} else {
			// rare
			ByteBuffer b = BufferUtils.allocate(4, order());
			b.put(BufferUtils.slice(buffers[buf], byt, buffers[buf].capacity()));
			b.put(BufferUtils.slice(buffers[buf + 1], 0, b.remaining()));
			return b.getFloat(0);
		}
	}

	// ===================================================================================================

	@Override
	public BigByteBuffer putDouble(double value) {
		return putDouble0(nextPutIndex(8), value);
	}

	@Override
	public BigByteBuffer putDouble(long index, double value) {

		if (index > capacity - 8)
			throw new BufferOverflowException();

		return putDouble0(index, value);
	}

	private BigByteBuffer putDouble0(long index, double value) {

		int buf = buf(index);
		int byt = byt(index);

		if (rem(buf, byt) >= 8) {
			// happy path
			buffers[buf].putDouble(byt, value);
		} else {
			// rare
			ByteBuffer b = BufferUtils.allocate(8, order());
			b.asDoubleBuffer().put(0, value);
			put0(index, b);
		}

		return this;
	}

	@Override
	public BigByteBuffer putDouble(double[] src) {
		return putDouble(DoubleBuffer.wrap(src));
	}

	@Override
	public BigByteBuffer putDouble(DoubleBuffer src) {

		if ((long) src.remaining() * 8 > remaining())
			throw new BufferOverflowException();

		// TODO: optimize this
		while (src.hasRemaining())
			putDouble(src.get());

		return this;
	}

	@Override
	public double getDouble() {
		return getDouble0(nextGetIndex(8));
	}

	@Override
	public double getDouble(long index) {

		if (index > capacity - 8)
			throw new BufferUnderflowException();

		return getDouble0(index);
	}

	private double getDouble0(long index) {

		int buf = buf(index);
		int byt = byt(index);

		if (rem(buf, byt) >= 8) {
			// happy path
			return buffers[buf].getDouble(byt);
		} else {
			// rare
			ByteBuffer b = BufferUtils.allocate(8, order());
			b.put(BufferUtils.slice(buffers[buf], byt, buffers[buf].capacity()));
			b.put(BufferUtils.slice(buffers[buf + 1], 0, b.remaining()));
			return b.getDouble(0);
		}
	}

	// ===================================================================================================

	@Override
	public InputStream toInputStream() {
		return new InputStream() {

			final BigByteBuffer buf = slice();

			@Override
			public int available() throws IOException {
				return buf.remaining() <= Integer.MAX_VALUE ? (int) buf.remaining() : Integer.MAX_VALUE;
			}

			@Override
			public int read() throws IOException {

				return buf.hasRemaining() ? buf.get() & 0xFF : -1;
			}

			@Override
			public int read(byte[] bytes, int off, int len) throws IOException {

				if (!buf.hasRemaining())
					return -1;

				len = (int) Math.min(len, buf.remaining());
				buf.get(bytes, off, len);
				return len;
			}

			@Override
			public String toString() {
				return CompoundBigByteBuffer.this.toString();
			}
		};
	}

	@Override
	public BigByteBuffer get(byte[] dst, int offset, int length) {
		Objects.checkFromIndexSize(offset, length, dst.length);

		if (length > remaining())
			throw new BufferUnderflowException();

		while (length > 0) {

			ByteBuffer buf = buffers[buf(position)];
			int byt = byt(position);

			int read = Math.min(buf.capacity() - byt, length);
			buf.get(byt, dst, offset, read);

			offset += read;
			position += read;
			length -= read;
		}

		return this;
	}

	@Override
	public BigByteBuffer put(long index, ByteBuffer src, int offset, int length) {
		put0(index, BufferUtils.slice(src, offset, offset + length));
		return this;
	}
}
