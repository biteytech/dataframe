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

package tech.bitey.dataframe;

import static java.util.Spliterator.DISTINCT;
import static java.util.Spliterator.SORTED;
import static tech.bitey.bufferstuff.BufferUtils.readFully;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

import tech.bitey.bufferstuff.BigByteBuffer;
import tech.bitey.bufferstuff.BufferUtils;

abstract class NonNullSingleBufferColumn<E extends Comparable<? super E>, I extends Column<E>, C extends NonNullSingleBufferColumn<E, I, C>>
		extends NonNullColumn<E, I, C> {

	final BigByteBuffer buffer;

	abstract C construct(BigByteBuffer buffer, int offset, int size, int characteristics, boolean view);

	NonNullSingleBufferColumn(BigByteBuffer buffer, int offset, int size, int characteristics, boolean view) {
		super(offset, size, characteristics, view);

		validateBuffer(buffer);
		this.buffer = buffer;
	}

	abstract int elementSize();

	BigByteBuffer allocate(int capacity) {
		return BufferUtils.allocateBig((long) capacity * elementSize());
	}

	@Override
	C withCharacteristics(int characteristics) {
		return construct(buffer, offset, size, characteristics, view);
	}

	abstract void sort();

	abstract int deduplicate();

	@Override
	C toSorted0() {
		C copy = copy();
		copy.sort();
		return copy.withCharacteristics(SORTED);
	}

	@Override
	C toDistinct0(boolean sort) {
		C copy = copy();
		if (sort)
			copy.sort();

		int size = copy.deduplicate();

		return construct(copy.buffer.copy(0, (long) size * elementSize()), 0, size, SORTED | DISTINCT, false);
	}

	@Override
	C subColumn0(int fromIndex, int toIndex) {
		return construct(buffer, fromIndex + offset, toIndex - fromIndex, characteristics, true);
	}

	@Override
	public C copy() {
		BigByteBuffer copy = buffer.copy((long) offset * elementSize(), (long) (offset + size) * elementSize());
		return construct(copy, 0, size, characteristics, false);
	}

	@Override
	public C slice() {
		return construct(slice0(), 0, size, characteristics, false);
	}

	@Override
	boolean equals0(C rhs, int lStart, int rStart, int length) {
		return buffer.slice((long) lStart * elementSize(), (long) (lStart + length) * elementSize())
				.equals(rhs.buffer.slice((long) rStart * elementSize(), (long) (rStart + length) * elementSize()));
	}

	@Override
	C appendNonNull(C tail) {

		final int size = size() + tail.size();

		BigByteBuffer buffer = allocate(size);

		buffer.put(this.slice0());
		buffer.put(tail.slice0());

		buffer.flip();

		return construct(buffer, 0, size, characteristics, false);
	}

	BigByteBuffer slice0() {
		return buffer.slice((long) offset * elementSize(), (long) (offset + size) * elementSize());
	}

	@Override
	void writeTo(WritableByteChannel channel) throws IOException {

		final ByteOrder order = buffer.order();

		writeByteOrder(channel, order);
		writeInt(channel, order, size);

		writeBuffer(channel, slice0());
	}

	@Override
	C readFrom(ReadableByteChannel channel, int version) throws IOException {

		ByteOrder order = readByteOrder(channel);
		int size = readInt(channel, order);

		final BigByteBuffer bbb;

		if (version <= 3) {

			ByteBuffer buffer = BufferUtils.allocate(size * elementSize(), order);
			readFully(channel, buffer);
			buffer.flip();
			bbb = BufferUtils.wrap(new ByteBuffer[] { buffer });
		} else {
			bbb = readBuffer(channel, order);
		}

		return construct(bbb, 0, size, characteristics, false);
	}
}
