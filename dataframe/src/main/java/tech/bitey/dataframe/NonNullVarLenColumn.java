/*
 * Copyright 2019 biteytech@protonmail.com
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

import static java.util.Spliterator.NONNULL;
import static tech.bitey.bufferstuff.BufferUtils.EMPTY_BUFFER;
import static tech.bitey.dataframe.DfPreconditions.checkState;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

import tech.bitey.bufferstuff.BufferBitSet;
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
		return binarySearch(offset, offset + size, value);
	}

	@Override
	int search(E value, boolean first) {
		if (isSorted()) {
			int index = search(value);
			if (isDistinct() || index < 0)
				return index;
			else if (first)
				return binaryFindFirst(index, value);
			else
				return binaryFindLast(index, value);
		} else
			return indexOf(value, first) + offset;
	}

	private int binaryFindFirst(int fromIndex, E key) {

		while (offset != fromIndex && getNoOffset(fromIndex - 1).equals(key)) {

			int range = 1, rangeIndex;
			do {
				range <<= 1;
				rangeIndex = fromIndex - range;
			} while (offset <= rangeIndex && getNoOffset(rangeIndex).equals(key));

			fromIndex -= range >> 1;
		}

		return fromIndex;
	}

	private int binaryFindLast(int fromIndex, E key) {

		while (fromIndex != lastIndex() && getNoOffset(fromIndex + 1).equals(key)) {

			int range = 1, rangeIndex;
			do {
				range <<= 1;
				rangeIndex = fromIndex + range;
			} while (rangeIndex <= lastIndex() && getNoOffset(rangeIndex).equals(key));

			fromIndex += range >> 1;
		}

		return fromIndex;
	}

	/**
	 * Searches the specified value using the binary search algorithm.
	 * <p>
	 * Adapted from {@code Arrays::binarySearch0}
	 *
	 * @param key the value to be searched for
	 * @return index of the specified key, if it is contained in this column;
	 *         otherwise, <tt>(-(<i>insertion point</i>) - 1)</tt>. The <i>insertion
	 *         point</i> is defined as the point at which the key would be inserted
	 *         into the array: the index of the first element greater than the key,
	 *         or <tt>a.length</tt> if all elements in the array are less than the
	 *         specified key. Note that this guarantees that the return value will
	 *         be &gt;= 0 if and only if the key is found.
	 */
	private int binarySearch(int fromIndex, int toIndex, E key) {

		int low = fromIndex;
		int high = toIndex - 1;

		while (low <= high) {
			int mid = (low + high) >>> 1;

			E midVal = getNoOffset(mid);

			int cmp = midVal.compareTo(key);

			if (cmp < 0)
				low = mid + 1;
			else if (cmp > 0)
				high = mid - 1;
			else
				return mid; // key found
		}
		return -(low + 1); // key not found.
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

		for (int i = offset + 1; i <= lastIndex(); i++) {
			if (length(i) != length(i - 1))
				continue;

			ByteBuffer curr = BufferUtils.slice(elements, pat(i), end(i));
			ByteBuffer prev = BufferUtils.slice(elements, pat(i - 1), end(i - 1));

			if (curr.equals(prev))
				return false;
		}

		return true;
	}

	@Override
	void writeTo(WritableByteChannel channel) throws IOException {

		final ByteOrder order = rawPointers.order();

		writeByteOrder(channel, order);
		writeInt(channel, order, size);

		if (size > 0) {
			channel.write(BufferUtils.slice(rawPointers, offset * 4, (offset + size) * 4));

			writeInt(channel, order, end(lastIndex()) - pat(offset));
			channel.write(BufferUtils.slice(elements, pat(offset), end(lastIndex())));
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	C readFrom(ReadableByteChannel channel) throws IOException {

		checkState(isEmpty());

		ByteOrder order = readByteOrder(channel);
		int size = readInt(channel, order);

		if (size == 0)
			return (C) this;

		ByteBuffer rawPointers = BufferUtils.allocate(size * 4, order);
		channel.read(rawPointers);
		rawPointers.flip();
		zero(rawPointers, size);

		int length = readInt(channel, order);
		ByteBuffer elements = BufferUtils.allocate(length, order);
		channel.read(elements);
		elements.flip();

		return construct(elements, rawPointers, 0, size, characteristics, false);
	}

	@Override
	IntColumn sortIndices(C distinct) {
		IntColumnBuilder indices = new IntColumnBuilder(NONNULL);
		indices.ensureCapacity(size());

		for (int i = offset; i <= lastIndex(); i++) {
			int index = distinct.search(getNoOffset(i));
			indices.add(index);
		}

		return indices.build();
	}
}
