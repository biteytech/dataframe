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
import static java.util.Spliterator.NONNULL;
import static java.util.Spliterator.SORTED;
import static tech.bitey.bufferstuff.BufferUtils.EMPTY_BIG_BUFFER;
import static tech.bitey.bufferstuff.BufferUtils.readFully;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.function.IntBinaryOperator;
import java.util.function.IntUnaryOperator;

import tech.bitey.bufferstuff.BigByteBuffer;
import tech.bitey.bufferstuff.BufferBitSet;
import tech.bitey.bufferstuff.BufferSort;
import tech.bitey.bufferstuff.BufferUtils;
import tech.bitey.bufferstuff.SmallIntBuffer;
import tech.bitey.bufferstuff.SmallLongBuffer;

abstract class NonNullVarLenColumn<E extends Comparable<E>, I extends Column<E>, C extends NonNullVarLenColumn<E, I, C>>
		extends NonNullColumn<E, I, C> {

	final BigByteBuffer elements;

	final BigByteBuffer rawPointers;
	final SmallLongBuffer pointers; // pointers[0] is always 0 - it's just easier that way :P

	final VarLenPacker<E> packer;

	NonNullVarLenColumn(VarLenPacker<E> packer, BigByteBuffer elements, BigByteBuffer rawPointers, int offset, int size,
			int characteristics, boolean view) {
		super(offset, size, characteristics, view);

		validateBuffer(elements);
		validateBuffer(rawPointers);

		this.elements = elements;

		this.rawPointers = rawPointers;
		this.pointers = rawPointers.asLongBuffer();

		this.packer = packer;
	}

	// pointer at index
	long pat(int index) {
		return pointers.get(index);
	}

	// element byte at index
	int bat(long index) {
		return elements.get(index);
	}

	abstract C construct(BigByteBuffer elements, BigByteBuffer rawPointers, int offset, int size, int characteristics,
			boolean view);

	@Override
	C withCharacteristics(int characteristics) {
		return construct(elements, rawPointers, offset, size, characteristics, view);
	}

	long end(int index) {
		return index == pointers.limit() - 1 ? elements.limit() : pat(index + 1);
	}

	long length(int index) {
		return end(index) - pat(index);
	}

	ByteBuffer element(int index) {
		return elements.smallSlice(pat(index), end(index));
	}

	@Override
	E getNoOffset(int index) {
		return packer.unpack(element(index));
	}

	@Override
	C subColumn0(int fromIndex, int toIndex) {
		return construct(elements, rawPointers, fromIndex + offset, toIndex - fromIndex, characteristics, true);
	}

	int search(E value) {
		return AbstractColumnSearch.binarySearch(this, offset, offset + size, value);
	}

	@Override
	int search(E value, boolean first) {
		return AbstractColumnSearch.search(this, value, first);
	}

	@Override
	public int hashCode(int fromIndex, int toIndex) {
		// from Arrays::hashCode
		long result = 1;

		for (int i = fromIndex; i <= toIndex; i++)
			result = 31 * result + length(i);

		for (long i = pat(fromIndex); i < end(toIndex); i++)
			result = 31 * result + bat(i);

		return Long.hashCode(result);
	}

	@Override
	boolean equals0(C rhs, int lStart, int rStart, int length) {
		if (length == 0)
			return true;

		for (int i = 0; i < length; i++)
			if (length(lStart + i) != rhs.length(rStart + i))
				return false;

		return elements.slice(pat(lStart), end(lStart + length - 1))
				.equals(rhs.elements.slice(rhs.pat(rStart), rhs.end(rStart + length - 1)));
	}

	private void copyElement(int i, BigByteBuffer dest) {
		BigByteBuffer src = elements.slice(pat(i), end(i));
		dest.put(src);
	}

	@Override
	C applyFilter0(BufferBitSet keep, int cardinality) {

		BigByteBuffer rawPointers = BufferUtils.allocateBig((long) cardinality * 8);
		long byteLength = 0;
		for (int i = offset; i <= lastIndex(); i++) {
			if (keep.get(i - offset)) {
				rawPointers.putLong(byteLength);
				byteLength += length(i);
			}
		}
		rawPointers.flip();

		BigByteBuffer elements = BufferUtils.allocateBig(byteLength);
		for (int i = offset; i <= lastIndex(); i++)
			if (keep.get(i - offset))
				copyElement(i, elements);
		elements.flip();

		return construct(elements, rawPointers, 0, cardinality, characteristics, false);
	}

	@Override
	C select0(IntColumn indices) {

		BigByteBuffer rawPointers = BufferUtils.allocateBig((long) indices.size() * 8);
		long byteLength = 0;
		for (int i = 0; i < indices.size(); i++) {
			rawPointers.putLong(byteLength);
			byteLength += length(indices.getInt(i) + offset);
		}
		rawPointers.flip();

		BigByteBuffer elements = BufferUtils.allocateBig(byteLength);
		for (int i = 0; i < indices.size(); i++) {
			int index = indices.getInt(i) + offset;
			copyElement(index, elements);
		}
		elements.flip();

		return construct(elements, rawPointers, 0, indices.size(), NONNULL, false);
	}

	BigByteBuffer sliceElements() {
		return elements.slice(pat(offset), end(lastIndex()));
	}

	BigByteBuffer sliceRawPointers() {
		return rawPointers.slice((long) offset * 8, (long) (offset + size) * 8);
	}

	BigByteBuffer copyRawPointers() {
		return rawPointers.copy((long) offset * 8, (long) (offset + size) * 8);
	}

	@Override
	C appendNonNull(C tail) {

		final long thisByteLength = this.end(this.lastIndex()) - this.pat(this.offset);
		final long tailByteLength = tail.end(tail.lastIndex()) - tail.pat(tail.offset);

		BigByteBuffer elements = BufferUtils.allocateBig(thisByteLength + tailByteLength);
		{
			elements.put(this.sliceElements());
			elements.put(tail.sliceElements());
			elements.flip();
		}

		BigByteBuffer rawPointers = BufferUtils.allocateBig((long) (this.size() + tail.size()) * 8);
		{
			rawPointers.put(this.sliceRawPointers());
			rawPointers.put(tail.sliceRawPointers());
			rawPointers.flip();
		}

		final SmallLongBuffer pointers = rawPointers.asLongBuffer();
		final int size = pointers.limit();
		final long thisOffset = this.pat(this.offset);
		for (int i = 0; i < this.size(); i++)
			pointers.put(i, pointers.get(i) - thisOffset);
		final long tailOffset = tail.pat(tail.offset);
		for (int i = this.size(); i < size; i++)
			pointers.put(i, pointers.get(i) - tailOffset + thisByteLength);

		return construct(elements, rawPointers, 0, size, characteristics, false);
	}

	@Override
	int compareValuesAt(C rhs, int l, int r) {
		return getNoOffset(l + offset).compareTo(rhs.getNoOffset(r + rhs.offset));
	}

	@Override
	void intersectLeftSorted(C rhs, IntColumnBuilder indices, BufferBitSet keepRight) {

		for (int i = rhs.offset; i <= rhs.lastIndex(); i++) {

			int leftIndex = search(rhs.getNoOffset(i));
			if (leftIndex >= offset && leftIndex <= lastIndex()) {

				indices.add(leftIndex - offset);
				keepRight.set(i - rhs.offset);
			}
		}
	}

	@Override
	public C copy() {
		if (isEmpty())
			return construct(EMPTY_BIG_BUFFER, EMPTY_BIG_BUFFER, 0, 0, characteristics, false);

		BigByteBuffer rawPointers = copyRawPointers();
		zero(rawPointers, size);

		BigByteBuffer elements = this.elements.copy(pat(offset), end(lastIndex()));

		return construct(elements, rawPointers, 0, size, characteristics, false);
	}

	@Override
	public C slice() {
		if (isEmpty())
			return empty();

		BigByteBuffer rawPointers = copyRawPointers();
		zero(rawPointers, size);

		return construct(sliceElements(), rawPointers, 0, size, characteristics, false);
	}

	private static void zero(BigByteBuffer rawPointers, int size) {
		if (size > 0) {
			SmallLongBuffer pointers = rawPointers.asLongBuffer();
			long first = pointers.get(0);
			for (int i = 0; i < size; i++)
				pointers.put(i, pointers.get(i) - first);
		}
	}

	@Override
	boolean checkSorted() {
		return checkSorted0(i -> getNoOffset(i - 1).compareTo(getNoOffset(i)));
	}

	boolean checkSorted0(IntUnaryOperator comparator) {
		if (size < 2)
			return true;

		for (int i = offset + 1; i <= lastIndex(); i++)
			if (comparator.applyAsInt(i) > 0)
				return false;

		return true;
	}

	@Override
	boolean checkDistinct() {
		if (size < 2)
			return true;

		ByteBuffer prev = element(offset);

		for (int i = offset + 1; i <= lastIndex(); i++) {

			ByteBuffer curr = element(i);

			if (curr.equals(prev))
				return false;

			prev = curr;
		}

		return true;
	}

	@SuppressWarnings("unchecked")
	@Override
	C toSorted0() {
		return (C) toSorted00(this, (l, r) -> getNoOffset(l + offset).compareTo(getNoOffset(r + offset)));
	}

	@SuppressWarnings("unchecked")
	static <C extends NonNullColumn<?, ?, ?>> C toSorted00(C column, IntBinaryOperator comparator) {

		int size = column.size();

		BigByteBuffer bb = BufferUtils.allocateBig((long) size * 4);
		SmallIntBuffer b = bb.asIntBuffer();
		for (int i = 0; i < size; i++)
			b.put(i, i);

		BufferSort.heapSort(b, comparator, 0, size);

		NonNullIntColumn indices = new NonNullIntColumn(bb, 0, size, NONNULL_CHARACTERISTICS, false);

		C sorted = (C) column.select(indices);
		return (C) sorted.withCharacteristics(NONNULL_CHARACTERISTICS | SORTED);
	}

	@Override
	C toDistinct0(boolean sort) {

		@SuppressWarnings("unchecked")
		C col = (C) this;

		if (sort)
			col = toSorted0();

		return col.toDistinct00();
	}

	C toDistinct00() {

		BufferBitSet keep = new BufferBitSet();
		int cardinality = 0;
		for (int i = lastIndex(); i >= offset;) {

			keep.set(i - offset);
			cardinality++;

			ByteBuffer e1 = element(i);
			i--;
			for (; i >= offset; i--) {
				ByteBuffer e2 = element(i);
				if (!e1.equals(e2))
					break;
			}
		}

		@SuppressWarnings("unchecked")
		C filtered = (C) applyFilter(keep, cardinality);
		return filtered.withCharacteristics(NONNULL_CHARACTERISTICS | SORTED | DISTINCT);
	}

	@Override
	void writeTo(WritableByteChannel channel) throws IOException {

		final ByteOrder order = rawPointers.order();

		writeByteOrder(channel, order);
		writeInt(channel, order, size);

		if (size > 0) {
			writeBuffer(channel, sliceRawPointers());
			writeBuffer(channel, sliceElements());
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	C readFrom(ReadableByteChannel channel, int version) throws IOException {

		Pr.checkState(isEmpty(), "readFrom can only be called on empty column");

		ByteOrder order = readByteOrder(channel);
		int size = readInt(channel, order);

		if (size == 0)
			return (C) this;

		final BigByteBuffer rawPointers;
		final BigByteBuffer elements;

		if (version <= 3) {
			ByteBuffer rp = BufferUtils.allocate(size * 4, order);
			readFully(channel, rp);
			rp.flip();
			IntBuffer irp = rp.asIntBuffer();
			rawPointers = BufferUtils.allocateBig((long) size * 8);
			while (irp.hasRemaining())
				rawPointers.putLong(irp.get());
			rawPointers.flip();

			int length = readInt(channel, order);
			ByteBuffer el = BufferUtils.allocate(length, order);
			readFully(channel, el);
			el.flip();
			elements = BufferUtils.wrap(new ByteBuffer[] { el });
		} else {
			rawPointers = readBuffer(channel, order);
			elements = readBuffer(channel, order);
		}

		zero(rawPointers, size);

		return construct(elements, rawPointers, 0, size, characteristics, false);
	}
}
