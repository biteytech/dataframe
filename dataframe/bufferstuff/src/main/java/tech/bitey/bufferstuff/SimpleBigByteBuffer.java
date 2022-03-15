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

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.DoubleBuffer;
import java.nio.FloatBuffer;
import java.nio.IntBuffer;
import java.nio.LongBuffer;
import java.nio.ShortBuffer;

/**
 * Delegates everything to a single {@link ByteBuffer}.
 * 
 * @author biteytech@protonmail.com
 */
final class SimpleBigByteBuffer implements BigByteBuffer {

	private final ByteBuffer buffer;

	SimpleBigByteBuffer(ByteBuffer buffer) {
		this.buffer = buffer;
	}

	@Override
	public ByteBuffer[] buffers() {
		return new ByteBuffer[] { buffer };
	}

	@Override
	public long position() {
		return buffer.position();
	}

	@Override
	public long limit() {
		return buffer.limit();
	}

	@Override
	public long capacity() {
		return buffer.capacity();
	}

	@Override
	public BigByteBuffer position(long newPosition) {
		buffer.position(toIntExact(newPosition));
		return this;
	}

	@Override
	public BigByteBuffer limit(long newLimit) {
		buffer.limit(toIntExact(newLimit));
		return this;
	}

	@Override
	public long remaining() {
		return buffer.remaining();
	}

	@Override
	public boolean hasRemaining() {
		return buffer.hasRemaining();
	}

	@Override
	public ByteOrder order() {
		return buffer.order();
	}

	@Override
	public BigByteBuffer order(ByteOrder order) {
		buffer.order(order);
		return this;
	}

	@Override
	public BigByteBuffer duplicate() {
		return new SimpleBigByteBuffer(buffer.duplicate().order(buffer.order()));
	}

	@Override
	public BigByteBuffer slice() {
		return new SimpleBigByteBuffer(buffer.slice().order(buffer.order()));
	}

	@Override
	public BigByteBuffer slice(long fromIndex, long toIndex) {
		return new SimpleBigByteBuffer(
				buffer.slice(toIntExact(fromIndex), toIntExact(toIndex - fromIndex)).order(buffer.order()));
	}

	@Override
	public ByteBuffer smallSlice(long fromIndex, long toIndex) {
		return slice(fromIndex, toIndex).buffers()[0];
	}

	@Override
	public BigByteBuffer copy(long fromIndex, long toIndex) {

		return new SimpleBigByteBuffer(BufferUtils.copy(buffer, toIntExact(fromIndex), toIntExact(toIndex)));
	}

	@Override
	public BigByteBuffer clear() {
		buffer.clear();
		return this;
	}

	@Override
	public BigByteBuffer flip() {
		buffer.flip();
		return this;
	}

	@Override
	public BigByteBuffer put(byte value) {
		buffer.put(value);
		return this;
	}

	@Override
	public BigByteBuffer put(long index, byte value) {
		buffer.put(toIntExact(index), value);
		return this;
	}

	@Override
	public BigByteBuffer put(BigByteBuffer src) {

		if (src.remaining() > remaining())
			throw new BufferUnderflowException();

		for (ByteBuffer b : src.slice().buffers())
			buffer.put(b);

		return this;
	}

	@Override
	public byte get() {
		return buffer.get();
	}

	@Override
	public byte get(long index) {
		return buffer.get(toIntExact(index));
	}

	@Override
	public BigByteBuffer putShort(short value) {
		buffer.putShort(value);
		return this;
	}

	@Override
	public BigByteBuffer putShort(long index, short value) {
		buffer.putShort(toIntExact(index), value);
		return this;
	}

	@Override
	public short getShort() {
		return buffer.getShort();
	}

	@Override
	public short getShort(long index) {
		return buffer.getShort(toIntExact(index));
	}

	@Override
	public BigByteBuffer putInt(int value) {
		buffer.putInt(value);
		return this;
	}

	@Override
	public BigByteBuffer putInt(long index, int value) {
		buffer.putInt(toIntExact(index), value);
		return this;
	}

	@Override
	public int getInt() {
		return buffer.getInt();
	}

	@Override
	public int getInt(long index) {
		return buffer.getInt(toIntExact(index));
	}

	@Override
	public BigByteBuffer putLong(long value) {
		buffer.putLong(value);
		return this;
	}

	@Override
	public BigByteBuffer putLong(long index, long value) {
		buffer.putLong(toIntExact(index), value);
		return this;
	}

	@Override
	public long getLong() {
		return buffer.getLong();
	}

	@Override
	public long getLong(long index) {
		return buffer.getLong(toIntExact(index));
	}

	@Override
	public BigByteBuffer putFloat(float value) {
		buffer.putFloat(value);
		return this;
	}

	@Override
	public BigByteBuffer putFloat(long index, float value) {
		buffer.putFloat(toIntExact(index), value);
		return this;
	}

	@Override
	public float getFloat() {
		return buffer.getFloat();
	}

	@Override
	public float getFloat(long index) {
		return buffer.getFloat(toIntExact(index));
	}

	@Override
	public BigByteBuffer putDouble(double value) {
		buffer.putDouble(value);
		return this;
	}

	@Override
	public BigByteBuffer putDouble(long index, double value) {
		buffer.putDouble(toIntExact(index), value);
		return this;
	}

	@Override
	public double getDouble() {
		return buffer.getDouble();
	}

	@Override
	public double getDouble(long index) {
		return buffer.getDouble(toIntExact(index));
	}

	@Override
	public BigByteBuffer putShort(short[] src) {
		buffer.asShortBuffer().put(src);
		buffer.position(buffer.position() + src.length * 2);
		return this;
	}

	@Override
	public BigByteBuffer putInt(int[] src) {
		buffer.asIntBuffer().put(src);
		buffer.position(buffer.position() + src.length * 4);
		return this;
	}

	@Override
	public BigByteBuffer putLong(long[] src) {
		buffer.asLongBuffer().put(src);
		buffer.position(buffer.position() + src.length * 8);
		return this;
	}

	@Override
	public BigByteBuffer putFloat(float[] src) {
		buffer.asFloatBuffer().put(src);
		buffer.position(buffer.position() + src.length * 4);
		return this;
	}

	@Override
	public BigByteBuffer putDouble(double[] src) {
		buffer.asDoubleBuffer().put(src);
		buffer.position(buffer.position() + src.length * 8);
		return this;
	}

	@Override
	public BigByteBuffer put(byte[] src) {
		buffer.put(src);
		return this;
	}

	@Override
	public BigByteBuffer putShort(ShortBuffer src) {
		int remaining = src.remaining();
		buffer.asShortBuffer().put(src);
		buffer.position(buffer.position() + remaining * 2);
		return this;
	}

	@Override
	public BigByteBuffer putInt(IntBuffer src) {
		int remaining = src.remaining();
		buffer.asIntBuffer().put(src);
		buffer.position(buffer.position() + remaining * 4);
		return this;
	}

	@Override
	public BigByteBuffer putLong(LongBuffer src) {
		int remaining = src.remaining();
		buffer.asLongBuffer().put(src);
		buffer.position(buffer.position() + remaining * 8);
		return this;
	}

	@Override
	public BigByteBuffer putFloat(FloatBuffer src) {
		int remaining = src.remaining();
		buffer.asFloatBuffer().put(src);
		buffer.position(buffer.position() + remaining * 4);
		return this;
	}

	@Override
	public BigByteBuffer putDouble(DoubleBuffer src) {
		int remaining = src.remaining();
		buffer.asDoubleBuffer().put(src);
		buffer.position(buffer.position() + remaining * 8);
		return this;
	}

	@Override
	public BigByteBuffer put(ByteBuffer src) {
		buffer.put(src);
		return this;
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof BigByteBuffer rhs) {
			if (remaining() != rhs.remaining())
				return false;
			else {
				// TODO: optimize this
				return buffer.equals(rhs.smallSlice(rhs.position(), rhs.limit()));
			}
		} else
			return false;
	}
}
