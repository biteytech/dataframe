/*
 * Copyright 2020 biteytech@protonmail.com
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

import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import tech.bitey.bufferstuff.BufferBitSet;

final class NonNullUuidColumn extends NonNullSingleBufferColumn<UUID, UuidColumn, NonNullUuidColumn>
		implements UuidColumn {

	static final Map<Integer, NonNullUuidColumn> EMPTY = new HashMap<>();
	static {
		EMPTY.computeIfAbsent(NONNULL_CHARACTERISTICS, c -> new NonNullUuidColumn(EMPTY_BUFFER, 0, 0, c, false));
		EMPTY.computeIfAbsent(NONNULL_CHARACTERISTICS | SORTED,
				c -> new NonNullUuidColumn(EMPTY_BUFFER, 0, 0, c, false));
		EMPTY.computeIfAbsent(NONNULL_CHARACTERISTICS | SORTED | DISTINCT,
				c -> new NonNullUuidColumn(EMPTY_BUFFER, 0, 0, c, false));
	}

	static NonNullUuidColumn empty(int characteristics) {
		return EMPTY.get(characteristics | NONNULL_CHARACTERISTICS);
	}

	private final LongBuffer elements;

	NonNullUuidColumn(ByteBuffer buffer, int offset, int size, int characteristics, boolean view) {
		super(buffer, offset, size, characteristics, view);

		this.elements = buffer.asLongBuffer();
	}

	@Override
	NonNullUuidColumn construct(ByteBuffer buffer, int offset, int size, int characteristics, boolean view) {
		return new NonNullUuidColumn(buffer, offset, size, characteristics, view);
	}

	private long msb(int index) {
		return elements.get(index << 1);
	}

	private long lsb(int index) {
		return elements.get((index << 1) + 1);
	}

	private void put(int index, long msb, long lsb) {
		elements.put(index << 1, msb);
		elements.put((index << 1) + 1, lsb);
	}

	@Override
	UUID getNoOffset(int index) {
		return new UUID(msb(index), lsb(index));
	}

	int search(UUID value) {
		return AbstractColumnSearch.binarySearch(this, offset, offset + size, value);
	}

	@Override
	int search(UUID value, boolean first) {
		return AbstractColumnSearch.search(this, value, first);
	}

	@Override
	void sort() {
		heapSort(offset, offset + size);
	}

	@Override
	int deduplicate() {
		return deduplicate(offset, offset + size);
	}

	@Override
	boolean checkSorted() {
		if (size < 2)
			return true;

		for (int i = offset + 1; i <= lastIndex(); i++) {
			if (compareValuesAt(i - 1, i) > 0)
				return false;
		}

		return true;
	}

	@Override
	boolean checkDistinct() {
		if (size < 2)
			return true;

		for (int i = offset + 1; i <= lastIndex(); i++) {
			if (compareValuesAt(i - 1, i) >= 0)
				return false;
		}

		return true;
	}

	@Override
	NonNullUuidColumn empty() {
		return EMPTY.get(characteristics);
	}

	@Override
	public ColumnType<UUID> getType() {
		return ColumnType.UUID;
	}

	@Override
	public int hashCode(int fromIndex, int toIndex) {
		// from Arrays::hashCode
		int result = 1;
		for (int i = fromIndex; i <= toIndex; i++) {

			long mostSigBits = elements.get(i / 2);
			long leastSigBits = elements.get(i / 2 + 1);
			long hilo = mostSigBits ^ leastSigBits;

			result = 31 * result + ((int) (hilo >> 32)) ^ (int) hilo;
		}
		return result;
	}

	private void put(ByteBuffer buffer, int index) {
		buffer.putLong(msb(index));
		buffer.putLong(lsb(index));
	}

	@Override
	NonNullUuidColumn applyFilter0(BufferBitSet keep, int cardinality) {

		ByteBuffer buffer = allocate(cardinality);
		for (int i = offset; i <= lastIndex(); i++)
			if (keep.get(i - offset))
				put(buffer, i);
		buffer.flip();

		return new NonNullUuidColumn(buffer, 0, cardinality, characteristics, false);
	}

	@Override
	NonNullUuidColumn select0(IntColumn indices) {

		ByteBuffer buffer = allocate(indices.size());
		for (int i = 0; i < indices.size(); i++)
			put(buffer, indices.getInt(i) + offset);
		buffer.flip();

		return construct(buffer, 0, indices.size(), NONNULL, false);
	}

	@Override
	int compareValuesAt(NonNullUuidColumn rhs, int l, int r) {

		return (this.msb(l) < rhs.msb(r) ? -1
				: (this.msb(l) > rhs.msb(r) ? 1
						: (this.lsb(l) < rhs.lsb(r) ? -1 : (this.lsb(l) > rhs.lsb(r) ? 1 : 0))));
	}

	private int compareValuesAt(int l, int r) {
		return compareValuesAt(this, l, r);
	}

	@Override
	void intersectLeftSorted(NonNullUuidColumn rhs, IntColumnBuilder indices, BufferBitSet keepRight) {

		for (int i = rhs.offset; i <= rhs.lastIndex(); i++) {

			int leftIndex = search(rhs.getNoOffset(i));
			if (leftIndex >= offset && leftIndex <= lastIndex()) {

				indices.add(leftIndex - offset);
				keepRight.set(i - rhs.offset);
			}
		}
	}

	@Override
	boolean checkType(Object o) {
		return o instanceof UUID;
	}

	@Override
	int elementSize() {
		return 16;
	}

	// =========================================================================

	private void heapSort(int fromIndex, int toIndex) {

		int n = toIndex - fromIndex;
		if (n <= 1)
			return;

		// Build max heap
		for (int i = fromIndex + n / 2 - 1; i >= fromIndex; i--)
			heapify(toIndex, i, fromIndex);

		// Heap sort
		for (int i = toIndex - 1; i >= fromIndex; i--) {
			swap(fromIndex, i);

			// Heapify root element
			heapify(i, fromIndex, fromIndex);
		}
	}

	// based on https://www.programiz.com/dsa/heap-sort
	private void heapify(int n, int i, int offset) {
		// Find largest among root, left child and right child
		int largest = i;
		int l = 2 * i + 1 - offset;
		int r = l + 1;

		if (l < n && compareValuesAt(l, largest) > 0)
			largest = l;

		if (r < n && compareValuesAt(r, largest) > 0)
			largest = r;

		// Swap and continue heapifying if root is not largest
		if (largest != i) {
			swap(i, largest);
			heapify(n, largest, offset);
		}
	}

	private void swap(int i, int j) {
		long im = msb(i);
		long il = lsb(i);
		long jm = msb(j);
		long jl = lsb(j);

		put(i, jm, jl);
		put(j, im, il);
	}

	// =========================================================================

	private int deduplicate(int fromIndex, int toIndex) {

		if (toIndex - fromIndex < 2)
			return toIndex;

		long prevM = msb(fromIndex);
		long prevL = lsb(fromIndex);
		int highest = fromIndex + 1;

		for (int i = fromIndex + 1; i < toIndex; i++) {
			long msb = msb(i);
			long lsb = lsb(i);

			if (prevM != msb || prevL != lsb) {
				if (highest < i)
					put(highest, msb, lsb);

				highest++;
				prevM = msb;
				prevL = lsb;
			}
		}

		return highest;
	}
}
