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

import static java.util.Spliterator.NONNULL;

import tech.bitey.bufferstuff.BigByteBuffer;
import tech.bitey.bufferstuff.BufferBitSet;

abstract class NonNullFixedLenColumn<E, I extends Column<E>, C extends NonNullFixedLenColumn<E, I, C>>
		extends NonNullSingleBufferColumn<E, I, C> {

	NonNullFixedLenColumn(BigByteBuffer buffer, int offset, int size, int characteristics, boolean view) {
		super(buffer, offset, size, characteristics, view);
	}

	@SuppressWarnings("unchecked")
	int compareValuesAt(int l, int r) {
		return compareValuesAt((C) this, l, r);
	}

	int search(E value) {
		return AbstractColumnSearch.binarySearch(this, offset, offset + size, value);
	}

	@Override
	int search(E value, boolean first) {
		return AbstractColumnSearch.search(this, value, first);
	}

	@Override
	void sort() {
		heapSort(offset, offset + size);
	}

	abstract void put(BigByteBuffer buffer, int index);

	@Override
	C applyFilter0(BufferBitSet keep, int cardinality) {

		BigByteBuffer buffer = allocate(cardinality);
		for (int i = offset; i <= lastIndex(); i++)
			if (keep.get(i - offset))
				put(buffer, i);
		buffer.flip();

		return construct(buffer, 0, cardinality, characteristics, false);
	}

	@Override
	C select0(IntColumn indices) {

		BigByteBuffer buffer = allocate(indices.size());
		for (int i = 0; i < indices.size(); i++)
			put(buffer, indices.getInt(i) + offset);
		buffer.flip();

		return construct(buffer, 0, indices.size(), NONNULL, false);
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

	abstract int deduplicate(int fromIndex, int toIndex);

	abstract void swap(int i, int j);

	void heapSort(int fromIndex, int toIndex) {

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
	void heapify(int n, int i, int offset) {
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
}
