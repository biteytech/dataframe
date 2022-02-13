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
import static tech.bitey.bufferstuff.BufferUtils.EMPTY_BUFFER;
import static tech.bitey.bufferstuff.BufferUtils.readFully;
import static tech.bitey.bufferstuff.BufferUtils.writeFully;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

import tech.bitey.bufferstuff.BufferBitSet;
import tech.bitey.bufferstuff.BufferSort;
import tech.bitey.bufferstuff.BufferUtils;

abstract class NonNullVarLenColumn<E extends Comparable<E>, I extends Column<E>, C extends NonNullVarLenColumn<E, I, C>>
		extends NonNullColumn<E, I, C> {

	final ByteBuffer elements;

	final ByteBuffer rawPointers;
	final IntBuffer pointers; // pointers[0] is always 0 - it's just easier that way :P

	final VarLenPacker<E> packer;

	NonNullVarLenColumn(VarLenPacker<E> packer, ByteBuffer elements, ByteBuffer rawPointers, int offset, int size,
			int characteristics, boolean view) {
		super(offset, size, characteristics, view);

		validateBuffer(elements);
		validateBuffer(rawPointers);

		this.elements = elements;

		this.rawPointers = rawPointers;
		this.pointers = rawPointers.asIntBuffer();

		this.packer = packer;
	}

	// pointer at index
	int pat(int index) {
		return pointers.get(index);
	}

	// element byte at index
	int bat(int index) {
		return elements.get(index);
	}

	abstract C construct(ByteBuffer elements, ByteBuffer rawPointers, int offset, int size, int characteristics,
			boolean view);

	@Override
	C withCharacteristics(int characteristics) {
		return construct(elements, rawPointers, offset, size, characteristics, view);
	}

	int end(int index) {
		return index == pointers.limit() - 1 ? elements.limit() : pat(index + 1);
	}

	int length(int index) {
		return end(index) - pat(index);
	}

	@Override
	E getNoOffset(int index) {

		ByteBuffer element = BufferUtils.slice(elements, pat(index), end(index));

		byte[] bytes = new byte[length(index)];
		element.get(bytes);

		return packer.unpack(bytes);
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
		int result = 1;

		for (int i = fromIndex; i <= toIndex; i++)
			result = 31 * result + length(i);

		for (int i = pat(fromIndex); i < end(toIndex); i++)
			result = 31 * result + bat(i);

		return result;
	}

	@Override
	boolean equals0(C rhs, int lStart, int rStart, int length) {
		if (length == 0)
			return true;

		for (int i = 0; i < length; i++)
			if (length(lStart + i) != rhs.length(rStart + i))
				return false;

		return BufferUtils.slice(elements, pat(lStart), end(lStart + length - 1))
				.equals(BufferUtils.slice(rhs.elements, rhs.pat(rStart), rhs.end(rStart + length - 1)));
	}

	private void copyElement(int i, ByteBuffer dest) {
		ByteBuffer src = BufferUtils.slice(elements, pat(i), end(i));
		dest.put(src);
	}

	@Override
	C applyFilter0(BufferBitSet keep, int cardinality) {

		ByteBuffer rawPointers = BufferUtils.allocate(cardinality * 4);
		int byteLength = 0;
		for (int i = offset; i <= lastIndex(); i++) {
			if (keep.get(i - offset)) {
				rawPointers.putInt(byteLength);
				byteLength += length(i);
			}
		}
		rawPointers.flip();

		ByteBuffer elements = BufferUtils.allocate(byteLength);
		for (int i = offset; i <= lastIndex(); i++)
			if (keep.get(i - offset))
				copyElement(i, elements);
		elements.flip();

		return construct(elements, rawPointers, 0, cardinality, characteristics, false);
	}

	@Override
	C select0(IntColumn indices) {

		ByteBuffer rawPointers = BufferUtils.allocate(indices.size() * 4);
		int byteLength = 0;
		for (int i = 0; i < indices.size(); i++) {
			rawPointers.putInt(byteLength);
			byteLength += length(indices.getInt(i) + offset);
		}
		rawPointers.flip();

		ByteBuffer elements = BufferUtils.allocate(byteLength);
		for (int i = 0; i < indices.size(); i++) {
			int index = indices.getInt(i) + offset;
			copyElement(index, elements);
		}
		elements.flip();

		return construct(elements, rawPointers, 0, indices.size(), NONNULL, false);
	}

	@Override
	C appendNonNull(C tail) {

		final int thisByteLength = this.end(this.lastIndex()) - this.pat(this.offset);
		final int tailByteLength = tail.end(tail.lastIndex()) - tail.pat(tail.offset);

		ByteBuffer elements = BufferUtils.allocate(thisByteLength + tailByteLength);
		{
			ByteBuffer thisElements = BufferUtils.slice(this.elements, this.pat(this.offset),
					this.end(this.lastIndex()));
			ByteBuffer tailElements = BufferUtils.slice(tail.elements, tail.pat(tail.offset),
					tail.end(tail.lastIndex()));

			elements.put(thisElements);
			elements.put(tailElements);
			elements.flip();
		}

		ByteBuffer rawPointers = BufferUtils.allocate((this.size() + tail.size()) * 4);
		{
			ByteBuffer thisPointers = BufferUtils.slice(this.rawPointers, this.offset * 4,
					(this.offset + this.size()) * 4);
			ByteBuffer tailPointers = BufferUtils.slice(tail.rawPointers, tail.offset * 4,
					(tail.offset + tail.size()) * 4);

			rawPointers.put(thisPointers);
			rawPointers.put(tailPointers);
			rawPointers.flip();
		}

		final IntBuffer pointers = rawPointers.asIntBuffer();
		final int size = pointers.limit();
		final int thisOffset = this.pat(this.offset);
		for (int i = 0; i < this.size(); i++)
			pointers.put(i, pointers.get(i) - thisOffset);
		final int tailOffset = tail.pat(tail.offset);
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
			return construct(EMPTY_BUFFER, EMPTY_BUFFER, 0, 0, characteristics, false);

		ByteBuffer rawPointers = BufferUtils.copy(this.rawPointers, offset * 4, (offset + size) * 4);
		zero(rawPointers, size);

		ByteBuffer elements = BufferUtils.copy(this.elements, pat(offset), end(lastIndex()));

		return construct(elements, rawPointers, 0, size, characteristics, false);
	}

	@Override
	public C slice() {
		if (isEmpty())
			return empty();

		ByteBuffer rawPointers = BufferUtils.copy(this.rawPointers, offset * 4, (offset + size) * 4);
		zero(rawPointers, size);

		ByteBuffer elements = BufferUtils.slice(this.elements, pat(offset), end(lastIndex()));

		return construct(elements, rawPointers, 0, size, characteristics, false);
	}

	private static void zero(ByteBuffer rawPointers, int size) {
		if (size > 0) {
			IntBuffer pointers = rawPointers.asIntBuffer();
			int first = pointers.get(0);
			for (int i = 0; i < size; i++)
				pointers.put(i, pointers.get(i) - first);
		}
	}

	@Override
	boolean checkSorted() {
		if (size < 2)
			return true;

		for (int i = offset + 1; i <= lastIndex(); i++) {
			if (getNoOffset(i - 1).compareTo(getNoOffset(i)) > 0)
				return false;
		}

		return true;
	}

	@Override
	boolean checkDistinct() {
		if (size < 2)
			return true;

		ByteBuffer prev = BufferUtils.slice(elements, pat(offset), end(offset));

		for (int i = offset + 1; i <= lastIndex(); i++) {
			if (length(i) != length(i - 1))
				continue;

			ByteBuffer curr = BufferUtils.slice(elements, pat(i), end(i));

			if (curr.equals(prev))
				return false;

			prev = curr;
		}

		return true;
	}

	@Override
	C toSorted0() {

		ByteBuffer bb = BufferUtils.allocate(size() * 4);
		IntBuffer b = bb.asIntBuffer();
		for (int i = 0; i < size(); i++)
			b.put(i, i);

		BufferSort.heapSort(b, (l, r) -> get(l).compareTo(get(r)), 0, size());

		NonNullIntColumn indices = new NonNullIntColumn(bb, 0, size(), NONNULL_CHARACTERISTICS, false);

		@SuppressWarnings("unchecked")
		C sorted = (C) select(indices);
		return sorted.withCharacteristics(NONNULL_CHARACTERISTICS | SORTED);
	}

	@SuppressWarnings("unchecked")
	@Override
	C toDistinct0(boolean sort) {

		C col = (C) this;

		if (sort)
			col = toSorted0();

		BufferBitSet keep = new BufferBitSet();
		int cardinality = 0;
		for (int i = col.lastIndex(); i >= col.offset;) {

			keep.set(i - col.offset);
			cardinality++;

			E value = col.getNoOffset(i);
			i--;
			for (; i >= col.offset && col.getNoOffset(i).equals(value); i--)
				;
		}

		C filtered = (C) col.applyFilter(keep, cardinality);
		return filtered.withCharacteristics(NONNULL_CHARACTERISTICS | SORTED | DISTINCT);
	}

	@Override
	void writeTo(WritableByteChannel channel) throws IOException {

		final ByteOrder order = rawPointers.order();

		writeByteOrder(channel, order);
		writeInt(channel, order, size);

		if (size > 0) {
			writeFully(channel, BufferUtils.slice(rawPointers, offset * 4, (offset + size) * 4));

			writeInt(channel, order, end(lastIndex()) - pat(offset));
			writeFully(channel, BufferUtils.slice(elements, pat(offset), end(lastIndex())));
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	C readFrom(ReadableByteChannel channel) throws IOException {

		Pr.checkState(isEmpty(), "readFrom can only be called on empty column");

		ByteOrder order = readByteOrder(channel);
		int size = readInt(channel, order);

		if (size == 0)
			return (C) this;

		ByteBuffer rawPointers = BufferUtils.allocate(size * 4, order);
		readFully(channel, rawPointers);
		rawPointers.flip();
		zero(rawPointers, size);

		int length = readInt(channel, order);
		ByteBuffer elements = BufferUtils.allocate(length, order);
		readFully(channel, elements);
		elements.flip();

		return construct(elements, rawPointers, 0, size, characteristics, false);
	}
}
