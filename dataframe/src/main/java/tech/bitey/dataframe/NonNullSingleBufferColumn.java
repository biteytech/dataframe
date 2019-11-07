/*
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

import java.nio.ByteBuffer;

import tech.bitey.bufferstuff.BufferUtils;

abstract class NonNullSingleBufferColumn<E, I extends Column<E>, C extends NonNullSingleBufferColumn<E, I, C>>
		extends NonNullColumn<E, I, C> {

	final ByteBuffer buffer;

	abstract C construct(ByteBuffer buffer, int offset, int size, int characteristics, boolean view);

	NonNullSingleBufferColumn(ByteBuffer buffer, int offset, int size, int characteristics, boolean view) {
		super(offset, size, characteristics, view);

		validateBuffer(buffer);
		this.buffer = buffer;
	}

	abstract int elementSize();

	ByteBuffer allocate(int capacity) {
		return BufferUtils.allocate(capacity * elementSize());
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
	C toDistinct0(C sorted) {
		int size = sorted.deduplicate();
		return construct(BufferUtils.slice(sorted.buffer, 0, size * elementSize()), 0, size,
				sorted.characteristics | SORTED | DISTINCT, false);
	}

	@Override
	C subColumn0(int fromIndex, int toIndex) {
		return construct(buffer, fromIndex + offset, toIndex - fromIndex, characteristics, true);
	}

	@Override
	public C copy() {
		ByteBuffer copy = BufferUtils.copy(buffer, offset * elementSize(), (offset + size) * elementSize());
		return construct(copy, 0, size, characteristics, false);
	}

	@Override
	public C slice() {
		ByteBuffer copy = BufferUtils.slice(buffer, offset * elementSize(), (offset + size) * elementSize());
		return construct(copy, 0, size, characteristics, false);
	}

	@Override
	boolean equals0(C rhs, int lStart, int rStart, int length) {
		return BufferUtils.slice(buffer, lStart * elementSize(), (lStart + length) * elementSize())
				.equals(BufferUtils.slice(rhs.buffer, rStart * elementSize(), (rStart + length) * elementSize()));
	}

	@Override
	C appendNonNull(C tail) {

		final int size = size() + tail.size();

		ByteBuffer buffer = allocate(size);

		buffer.put(BufferUtils.slice(this.buffer, this.offset * elementSize(), (this.offset + this.size) * elementSize()));
		buffer.put(BufferUtils.slice(tail.buffer, tail.offset * elementSize(), (tail.offset + tail.size) * elementSize()));

		buffer.flip();

		return construct(buffer, 0, size, characteristics, false);
	}
}
